import apache_beam as beam
import logging
import json
import os
import time
import base64
import random
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue
from apache_beam.metrics.metric import Metrics


class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--config_path", type=str, help="Path to config.json")


custom_options = CustomPipelineOptions()
CONFIG_PATH = custom_options.config_path

if not CONFIG_PATH or not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"Config file not found at: {CONFIG_PATH}")

with open(CONFIG_PATH) as config_file:
    config = json.load(config_file)

PROJECT_ID = config["project_id"]
REGION = config["region"]
SOURCE_HL7V2_STORE = config["source_hl7v2_store"]
DEST_HL7V2_STORE = config["dest_hl7v2_store"]
PUBSUB_SUBSCRIPTION = config["pubsub_subscription"]
STAGING_LOCATION = config["staging_location"]
TEMP_LOCATION = config["temp_location"]
SETUP_FILE = config["setup_file"]
WORKER_MACHINE_TYPE = config.get("worker_machine_type", "n1-highmem-16")
MIN_WORKERS = config.get("min_num_workers", 1)
MAX_WORKERS = config.get("max_num_workers", 100)
MAX_EXECUTOR_WORKERS = config.get("executor_max_workers", 4)
FETCH_TIMEOUT = config.get("fetch_timeout_sec", 500)
INGEST_TIMEOUT = config.get("ingest_timeout_sec", 500)
RETRY_CONFIG = config.get("retry_config", {})

RETRYABLE_STATUSES = [409, 429, 500, 502, 503, 504, 403]


class LogPubSubMessageID(beam.DoFn):
    def process(self, msg):
        try:
            decoded = msg.decode("utf-8", errors="replace").strip()
            logging.debug(f"Received Pub/Sub message: {decoded}")
        except Exception as e:
            logging.error(f"Error decoding Pub/Sub message: {e}")
        yield msg


class FetchHL7v2Message(beam.DoFn):
    def __init__(self, source_store):
        self.source_store = source_store
        self.authed_session = None
        self.max_retries = RETRY_CONFIG.get("max_retries", 5)
        self.backoff_base = RETRY_CONFIG.get("backoff_base", 2)
        self.max_wait_time = RETRY_CONFIG.get("max_wait_time", 30)
        self.fetched = Metrics.counter('HL7v2', 'Fetched')
        self.failed_fetch = Metrics.counter('HL7v2', 'FailedFetch')

    def setup(self):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-healthcare"])
        self.authed_session = AuthorizedSession(credentials)

    def fetch_message(self, message_path):
        full_url = f"https://healthcare.googleapis.com/v1/{message_path}"
        last_error = None
        status_code = None

        for attempt in range(self.max_retries):
            try:
                response = self.authed_session.get(full_url)
                status_code = response.status_code
                if status_code in [200, 201]:
                    self.fetched.inc()
                    return response.json()
                elif status_code in RETRYABLE_STATUSES:
                    last_error = f"Retryable HTTP {status_code}: {response.text[:200]}"
                    logging.warning(json.dumps({
                        "event": "retrying_request",
                        "attempt": attempt + 1,
                        "status": status_code,
                        "message_path": message_path,
                        "reason": response.text[:200]
                    }))
                    time.sleep(random.uniform(1, min(self.backoff_base ** attempt, self.max_wait_time)))
                else:
                    last_error = f"Non-retryable HTTP {status_code}: {response.text[:200]}"
                    logging.error(json.dumps({
                        "event": "fetch_failed_non_retryable",
                        "status": status_code,
                        "message_path": message_path,
                        "body": response.text[:300]
                    }))
                    break
            except Exception as e:
                last_error = f"Exception: {str(e)}"
                status_code = "exception"

        self.failed_fetch.inc()
        logging.error(json.dumps({
            "event": f"failed_after_{self.max_retries}_retries",
            "message_path": message_path,
            "error": last_error,
            "status": status_code
        }))
        return None

    def process(self, message_id_bytes):
        message_path = message_id_bytes.decode("utf-8", errors="replace").strip()
        try:
            with ThreadPoolExecutor(max_workers=MAX_EXECUTOR_WORKERS) as executor:
                logging.info(f"Submitting fetch task for {message_path}")
                future = executor.submit(self.fetch_message, message_path)
                result = future.result(timeout=FETCH_TIMEOUT)
                logging.info(f"Fetch completed or timed out for {message_path}")
                if result:
                    yield result
                else:
                    yield pvalue.TaggedOutput('failed_fetch', message_path)
        except FuturesTimeout:
            future.cancel()
            logging.error(json.dumps({
                "event": "fetch_timeout",
                "message_path": message_path,
                "status": "timeout"
            }))
            yield pvalue.TaggedOutput('failed_fetch', message_path)
        except Exception as e:
            logging.exception(json.dumps({
                "event": "fetch_unhandled_exception",
                "message_path": message_path,
                "status": "exception",
                "error": str(e)
            }))
            yield pvalue.TaggedOutput('failed_fetch', message_path)


class WriteToHL7v2Store(beam.DoFn):
    def __init__(self, dest_store):
        self.dest_store = dest_store
        self.authed_session = None
        self.max_retries = RETRY_CONFIG.get("max_retries", 5)
        self.backoff_base = RETRY_CONFIG.get("backoff_base", 2)
        self.max_wait_time = RETRY_CONFIG.get("max_wait_time", 30)
        self.ingested = Metrics.counter('HL7v2', 'Ingested')
        self.failed_ingest = Metrics.counter('HL7v2', 'FailedIngest')

    def setup(self):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-healthcare"])
        self.authed_session = AuthorizedSession(credentials)

    def ingest_message(self, data, source_id):
        ingest_url = f"https://healthcare.googleapis.com/v1/{self.dest_store}/messages:ingest"
        payload = {"message": {"data": data}}
        last_error = None
        status_code = None

        for attempt in range(self.max_retries):
            try:
                response = self.authed_session.post(ingest_url, json=payload)
                status_code = response.status_code
                if status_code in [200, 201]:
                    self.ingested.inc()
                    logging.info(f"Successfully ingested: {source_id}")
                    return response.json()
                elif status_code in RETRYABLE_STATUSES:
                    last_error = f"Retryable HTTP {status_code}: {response.text[:200]}"
                    logging.warning(json.dumps({
                        "event": "retrying_request",
                        "attempt": attempt + 1,
                        "status": status_code,
                        "source_id": source_id,
                        "reason": response.text[:200]
                    }))
                    time.sleep(random.uniform(1, min(self.backoff_base ** attempt, self.max_wait_time)))
                else:
                    last_error = f"Non-retryable HTTP {status_code}: {response.text[:200]}"
                    logging.error(json.dumps({
                        "event": "ingest_failed_non_retryable",
                        "status": status_code,
                        "source_id": source_id,
                        "body": response.text[:300]
                    }))
                    break
            except Exception as e:
                last_error = f"Exception: {str(e)}"
                status_code = "exception"

        hl7id, schematized_data = "unknown", {}
        try:
            decoded = base64.b64decode(data).decode("utf-8")
            schematized_data = json.loads(decoded)
            hl7id = schematized_data.get("hl7id", "unknown")
        except Exception:
            decoded = "decode_error"

        self.failed_ingest.inc()
        logging.error(json.dumps({
            "event": f"failed_after_{self.max_retries}_retries",
            "hl7id": source_id,
            "data": data,
            "schematized_data": schematized_data,
            "error": last_error,
            "status": status_code
        }))
        return None

    def process(self, hl7_message):
        data = hl7_message.get("data")
        source_id = hl7_message.get("name")
        if not data or not source_id:
            logging.warning(f"Missing data or source_id in message: {json.dumps(hl7_message)[:300]}")
            yield pvalue.TaggedOutput('failed_ingest', hl7_message)
            return
        try:
            try:
                base64.b64decode(data)
            except Exception as decode_error:
                logging.error(json.dumps({
                    "event": "base64_decode_error",
                    "source_id": source_id,
                    "error": str(decode_error),
                    "status": "decode_error"
                }))
                yield pvalue.TaggedOutput('failed_ingest', hl7_message)
                return

            with ThreadPoolExecutor(max_workers=MAX_EXECUTOR_WORKERS) as executor:
                logging.info(f"Submitting ingest task for {source_id}")
                future = executor.submit(self.ingest_message, data, source_id)
                result = future.result(timeout=INGEST_TIMEOUT)
                logging.info(f"Ingest completed or timed out for {source_id}")
                if result:
                    yield result
                else:
                    yield pvalue.TaggedOutput('failed_ingest', hl7_message)
        except FuturesTimeout:
            future.cancel()
            logging.error(json.dumps({
                "event": "ingest_timeout",
                "source_id": source_id,
                "status": "timeout"
            }))
            yield pvalue.TaggedOutput('failed_ingest', hl7_message)
        except Exception as e:
            logging.exception(json.dumps({
                "event": "ingest_unhandled_exception",
                "source_id": source_id,
                "status": "exception",
                "error": str(e)
            }))
            yield pvalue.TaggedOutput('failed_ingest', hl7_message)


def run():
    try:
        pipeline_options = PipelineOptions(
            project=PROJECT_ID,
            region=REGION,
            runner="DataflowRunner",
            streaming=True,
            staging_location=STAGING_LOCATION,
            temp_location=TEMP_LOCATION,
            setup_file=SETUP_FILE,
            worker_machine_type=WORKER_MACHINE_TYPE,
            experiments=["enable_preflight_validation=false"],
            save_main_session=True,
            min_num_workers=MIN_WORKERS,
            max_num_workers=MAX_WORKERS,
            autoscaling_algorithm="THROUGHPUT_BASED"
        )

        pipeline = beam.Pipeline(options=pipeline_options)

        fetched_output = (
            pipeline
            | "Read HL7v2 IDs from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
            | "Log Raw Pub/Sub Message" >> beam.ParDo(LogPubSubMessageID())
            | "Fetch Full HL7v2 Message" >> beam.ParDo(FetchHL7v2Message(SOURCE_HL7V2_STORE)).with_outputs('failed_fetch', main='fetched')
        )

        ingested_output = (
            fetched_output.fetched
            | "Write to Destination HL7v2 Store" >> beam.ParDo(WriteToHL7v2Store(DEST_HL7V2_STORE)).with_outputs('failed_ingest', main='ingested')
        )

        fetched_output.failed_fetch | "Log Fetch Failures" >> beam.Map(lambda x: logging.error(f"Failed to fetch: {x}"))
        ingested_output.failed_ingest | "Log Ingest Failures" >> beam.Map(lambda x: logging.error(f"Failed to ingest: {x}"))

        result = pipeline.run()
        logging.info(f"Streaming Dataflow job submitted. Job ID: {result.job_id()}")
        print(f"Dataflow job submitted. Job ID: {result.job_id()}")
        print(f"https://console.cloud.google.com/dataflow/jobsDetail/locations/{REGION}/jobs/{result.job_id()}?project={PROJECT_ID}")

    except Exception as e:
        logging.exception("Pipeline failed to start: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    run()
