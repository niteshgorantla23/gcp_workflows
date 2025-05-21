import apache_beam as beam
import json
import logging
import time
import random
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.metrics.metric import Metrics

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.abspath(os.path.join(CURRENT_DIR, "../../../config/config.json"))

with open(CONFIG_PATH) as config_file:
    config = json.load(config_file)

PROJECT_ID = config["project_id"]
REGION = config["region"]
FHIR_STORE_URL = config["fhir_store_url"]
PUBSUB_SUBSCRIPTION = config["pubsub_subscription"]
STAGING_LOCATION = config["staging_location"]
TEMP_LOCATION = config["temp_location"]
SETUP_FILE = config["setup_file"]
MIN_WORKERS = config.get("min_num_workers", 1)
MAX_WORKERS = config.get("max_num_workers", 5)
RETRY_CONFIG = config.get("retry_config", {})
FILTER_CONFIG = config.get("filter_config", {})

class WriteToFHIR(beam.DoFn):
    def __init__(self, fhir_url):
        self.fhir_url = fhir_url
        self.session = None
        self.max_retries = RETRY_CONFIG.get("max_retries", 5)
        self.backoff_base = RETRY_CONFIG.get("backoff_base", 2)
        self.max_wait_time = RETRY_CONFIG.get("max_wait_time", 30)
        self.success_counter = Metrics.counter(self.__class__, 'fhir_write_success')
        self.failure_counter = Metrics.counter(self.__class__, 'fhir_write_failure')

    def setup(self):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-healthcare"])
        self.session = AuthorizedSession(credentials)

    def process(self, fhir_resource):
        resource_type = fhir_resource.get("resourceType")
        resource_id = fhir_resource.get("id")

        if not resource_type or not resource_id:
            logging.error("Missing resourceType or ID. Cannot write to FHIR.")
            self.failure_counter.inc()
            return

        fhir_endpoint = f"{self.fhir_url}/{resource_type}/{resource_id}"
        success = False

        for attempt in range(self.max_retries):
            try:
                response = self.session.put(
                    fhir_endpoint,
                    headers={"Content-Type": "application/fhir+json"},
                    json=fhir_resource
                )

                if response.status_code in [200, 201]:
                    logging.info(json.dumps({
                        "msg": "Successfully wrote to FHIR",
                        "resource_type": resource_type,
                        "resource_id": resource_id,
                        "meta": fhir_resource.get("meta", {})
                    }))
                    success = True
                    self.success_counter.inc()
                    break

                elif response.status_code in [409, 429, 500, 502, 503, 504, 403]:
                    if response.status_code == 429:
                        error_body = response.text
                        if "lock" in error_body.lower():
                            logging.warning(f"429 Lock Contention detected on attempt {attempt+1} for {resource_type}/{resource_id}.")
                        elif "quota" in error_body.lower():
                            logging.error(f"429 Quota limit exceeded on attempt {attempt+1} for {resource_type}/{resource_id}.")
                        else:
                            logging.warning(f"429 Unknown cause for {resource_type}/{resource_id}. Response body: {error_body}")

                    base_wait = min(self.backoff_base ** attempt, self.max_wait_time)
                    jittered_wait = random.uniform(0, base_wait)
                    logging.warning(f"Transient error ({response.status_code}) on attempt {attempt+1} for {resource_type}/{resource_id}. Retrying in {jittered_wait:.2f}s.")
                    time.sleep(jittered_wait)
                    continue

                else:
                    logging.error(f"Non-retryable error ({response.status_code}) for {resource_type}/{resource_id}. Response: {response.text}")
                    self.failure_counter.inc()
                    break

            except Exception as e:
                base_wait = min(self.backoff_base ** attempt, self.max_wait_time)
                jittered_wait = random.uniform(0, base_wait)
                logging.warning(f"Exception on attempt {attempt+1} for {resource_type}/{resource_id}: {type(e).__name__}: {e}. Retrying in {jittered_wait:.2f}s.")
                if hasattr(e, 'response') and e.response is not None:
                    logging.error(f"Response content: {e.response.text}")
                time.sleep(jittered_wait)

        if success:
            yield fhir_resource
        else:
            logging.error(json.dumps({
                "msg": "Failed to write to FHIR after max retries",
                "resource_type": resource_type,
                "resource_id": resource_id,
                "fhir_endpoint": fhir_endpoint,
                "payload": fhir_resource
            }))
            self.failure_counter.inc()

class ParsePubSubMessage(beam.DoFn):
    def __init__(self):
        self.session = None
        self.fhir_api_prefix = "https://healthcare.googleapis.com/v1/"
        self.max_retries = RETRY_CONFIG.get("max_retries", 5)
        self.backoff_base = RETRY_CONFIG.get("backoff_base", 2)
        self.max_wait_time = RETRY_CONFIG.get("max_wait_time", 30)
        self.filter_config = FILTER_CONFIG
        self.accepted_counter = Metrics.counter(self.__class__, 'fhir_resource_accepted')
        self.rejected_counter = Metrics.counter(self.__class__, 'fhir_resource_rejected')
        self.failed_fetch_counter = Metrics.counter(self.__class__, 'fhir_fetch_failed')

    def setup(self):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-healthcare"])
        self.session = AuthorizedSession(credentials)

    def process(self, message):
        try:
            decoded_message = message.decode("utf-8").strip()

            if decoded_message.startswith("projects/"):
                resource_url = self.fhir_api_prefix + decoded_message
                success = False

                for attempt in range(self.max_retries):
                    try:
                        response = self.session.get(resource_url, headers={"Content-Type": "application/fhir+json"})

                        if response.status_code in [200, 201]:
                            full_resource = response.json()
                            if self._passes_filters(full_resource):
                                logging.info(json.dumps({
                                    "msg": "Accepted URL FHIR resource",
                                    "resource_type": full_resource.get("resourceType"),
                                    "resource_id": full_resource.get("id"),
                                    "meta": full_resource.get("meta", {})
                                }))
                                self.accepted_counter.inc()
                                yield full_resource
                            else:
                                self.rejected_counter.inc()
                            success = True
                            break

                        elif response.status_code in [429, 500, 502, 503]:
                            if response.status_code == 429:
                                error_body = response.text
                                if "lock" in error_body.lower():
                                    logging.warning(f"429 Lock Contention detected on fetch attempt {attempt+1} for {resource_url}.")
                                elif "quota" in error_body.lower():
                                    logging.error(f"429 Quota limit exceeded on fetch attempt {attempt+1} for {resource_url}.")
                                else:
                                    logging.warning(f"429 Unknown cause on fetch for {resource_url}. Response body: {error_body}")

                            base_wait = min(self.backoff_base ** attempt, self.max_wait_time)
                            jittered_wait = random.uniform(0, base_wait)
                            logging.warning(f"Transient GET error ({response.status_code}) on attempt {attempt+1} for {resource_url}. Retrying in {jittered_wait:.2f}s.")
                            time.sleep(jittered_wait)
                            continue

                        else:
                            logging.error(f"Non-retryable GET error: {response.status_code} for {resource_url}. Response: {response.text}")
                            self.failed_fetch_counter.inc()
                            break

                    except Exception as e:
                        base_wait = min(self.backoff_base ** attempt, self.max_wait_time)
                        jittered_wait = random.uniform(0, base_wait)
                        logging.warning(f"GET attempt {attempt+1} failed for {resource_url}: {type(e).__name__}: {e}. Retrying in {jittered_wait:.2f}s.")
                        if hasattr(e, 'response') and e.response is not None:
                            logging.error(f"Response content: {e.response.text}")
                        time.sleep(jittered_wait)

                if not success:
                    logging.error(json.dumps({
                        "msg": "Failed to fetch resource from FHIR after max retries",
                        "resource_url": resource_url,
                        "original_message": decoded_message
                    }))
                    self.failed_fetch_counter.inc()
                return

            try:
                json_message = json.loads(decoded_message)
                if self._passes_filters(json_message):
                    logging.info(json.dumps({
                        "msg": "Accepted JSON FHIR resource",
                        "resource_type": json_message.get("resourceType"),
                        "resource_id": json_message.get("id"),
                        "meta": json_message.get("meta", {})
                    }))
                    self.accepted_counter.inc()
                    yield json_message
                else:
                    self.rejected_counter.inc()
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON format: {e}")
                self.rejected_counter.inc()
            except Exception as e:
                logging.error(f"Unhandled error parsing message: {e}")
                self.rejected_counter.inc()

        except Exception as e:
            logging.error(f"Failed to parse incoming message: {e}")
            self.rejected_counter.inc()

    def _passes_filters(self, resource):
        resource_type = resource.get("resourceType", "")
        allowed_facilities = self.filter_config.get("allowed_facilities", [])

        if resource_type in {
            "Account", "Coverage", "CoverageEligibilityResponse", "DocumentReference", "Encounter",
            "Patient", "AllergyIntolerance", "Appointment", "Communication", "Condition", "Device",
            "DiagnosticReport", "Immunization", "Medication", "MedicationAdministration", "MedicationRequest",
            "Observation", "Procedure", "ServiceRequest", "Specimen"
        }:
            source = resource.get("meta", {}).get("source", "")
            return any(fac in source for fac in allowed_facilities)

        elif resource_type == "Location":
            rule = self.filter_config.get("location_filter", {})
            for identifier in resource.get("identifier", []):
                coding = identifier.get("type", {}).get("coding", [])
                if any(code.get("code") == rule.get("code") for code in coding) and identifier.get("value") == rule.get("value"):
                    return True
            return False

        elif resource_type == "Provenance":
            rule = self.filter_config.get("provenance_filter", {})
            for ext in resource.get("extension", []):
                if ext.get("url") == rule.get("control_id_url"):
                    for sub_ext in ext.get("extension", []):
                        if sub_ext.get("url") == rule.get("value_url") and any(fac in sub_ext.get("valueString", "") for fac in allowed_facilities):
                            return True
            return False

        elif resource_type == "Organization":
            rule = self.filter_config.get("organization_filter", {})
            for identifier in resource.get("identifier", []):
                t = identifier.get("type", {}).get("text", "")
                s = identifier.get("system", "")
                v = identifier.get("value", "")
                if t in rule:
                    if isinstance(rule[t], list) and v in rule[t]:
                        return True
                    if isinstance(rule[t], dict) and rule[t].get("system_contains") in s:
                        return True
            return False

        elif resource_type == "RelatedPerson":
            rule = self.filter_config.get("related_person_filter", {})
            for relation in resource.get("relationship", []):
                for coding in relation.get("coding", []):
                    if rule.get("system_contains", "") in coding.get("system", ""):
                        return True
            return False

        elif resource_type == "Practitioner":
            rule = self.filter_config.get("practitioner_filter", {})
            for identifier in resource.get("identifier", []):
                if rule.get("system_contains", "") in identifier.get("system", ""):
                    return True
            return False

        return False

def run():
    pipeline_options = PipelineOptions(
        project=PROJECT_ID,
        region=REGION,
        runner="DataflowRunner",
        streaming=True,
        staging_location=STAGING_LOCATION,
        temp_location=TEMP_LOCATION,
        setup_file=SETUP_FILE,
        experiments=["enable_preflight_validation=false"],
        save_main_session=True,
        max_num_workers=MAX_WORKERS,
        worker_machine_type="n1-highmem-16",
        autoscaling_algorithm="THROUGHPUT_BASED",
        min_num_workers=MIN_WORKERS
    )

    pipeline = beam.Pipeline(options=pipeline_options)

    (
        pipeline
        | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
        | "Parse Pub/Sub Message" >> beam.ParDo(ParsePubSubMessage())
        | "Write to FHIR Store" >> beam.ParDo(WriteToFHIR(FHIR_STORE_URL))
    )

    result = pipeline.run()
    job_id = getattr(result, "job_id", lambda: "<unknown>")()
    logging.info(f"Streaming Dataflow job submitted. Job ID: {job_id}")
    print(f"Dataflow job submitted. Job ID: {job_id}")
    print(f"https://console.cloud.google.com/dataflow/jobsDetail/locations/{REGION}/jobs/{job_id}?project={PROJECT_ID}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    run()
