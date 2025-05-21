HL7v2 to HL7v2 Dataflow Pipeline
This repository contains a streaming Apache Beam pipeline (Python) that runs on Google Cloud Dataflow. The pipeline reads HL7v2 message identifiers from a Pub/Sub subscription, fetches the full HL7v2 message from a source HL7v2 store, and ingests it into a destination HL7v2 store using the Google Cloud Healthcare API.

Overview
Input: Pub/Sub subscription with full HL7v2 message paths.
Process:
Fetch the message from the source HL7v2 store.
Log and decode message content.
Re-ingest it into the destination HL7v2 store.
Output: HL7v2 message ingested into a new store.
Architecture flow diagram
hl7v2_to_hl7v2

Configuration
Update the following variables in main.py to match your environment:

python PROJECT_ID = "" REGION = ""

SOURCE_HL7V2_STORE = "projects/your-project/locations/region/datasets/your-dataset/hl7V2Stores/source-store" DEST_HL7V2_STORE = "projects/your-project/locations/region/datasets/your-dataset/hl7V2Stores/destination-store"

PUBSUB_SUBSCRIPTION = "projects/your-project/subscriptions/your-subscription" Also ensure the correct path to your setup.py is specified in the pipeline options.
File Structure
hl7v2-to-hl7v2-dataflow/ │ ├── main.py # Main Dataflow pipeline ├── README.md # Project documentation └── hl7v2_to_hl7v2_dataflow/ └── src/ └── main/ └── hl7v2_to_hl7v2/ └── setup.py # Required for pipeline packaging Prerequisites Python 3.10+

Google Cloud SDK (gcloud)

Permissions for Pub/Sub, Healthcare API, Dataflow, and GCS

Dependencies installed:
bash Copy Edit pip install apache-beam[gcp] google-auth requests Running the Pipeline Deploy the pipeline with:

bash Copy Edit python main.py The pipeline will launch on Dataflow in streaming mode and listen to incoming Pub/Sub messages.

Required IAM Roles
Ensure the Dataflow service account has the following roles:

Pub/Sub Subscriber

Healthcare HL7v2 Store Viewer

Healthcare HL7v2 Store Editor

Dataflow Worker

Storage Object Admin (for staging and temp buckets)

Notes
HL7v2 messages are base64 encoded in the data field.

Missing or empty data fields are gracefully skipped.

Logs both raw and decoded content for visibility.

Uses AuthorizedSession for secure, scoped API calls.

retry logic
                  ┌─────────────────────────────────────┐
                  │      Pub/Sub Subscription           │
                  └─────────────────────────────────────┘
                               │
                               ▼
         ┌───────────────────────────────────────────────┐
         │  Read HL7v2 message IDs from Pub/Sub          │
         └───────────────────────────────────────────────┘
                               │
                               ▼
         ┌───────────────────────────────────────────────┐
         │ LogPubSubMessageID                            │
         │ (Decode message, log raw ID)                  │
         └───────────────────────────────────────────────┘
                               │
                               ▼
         ┌───────────────────────────────────────────────┐
         │ FetchHL7v2Message (ParDo)                     │
         │ → Starts ThreadPoolExecutor (2 threads)       │
         │ → Calls HL7v2 GET API                         │
         │ → Retries on 429/500/503                      │
         │ → Timeout in 60s                              │
         └───────────────────────────────────────────────┘
               │                             │
               │                             ▼
               │                  ┌──────────────────────────────┐
               │                  │ Non-retryable / Timeout /    │
               │                  │ Exception                    │
               │                  └──────────────────────────────┘
               │                             │
               ▼                             ▼
  ┌────────────────────────────┐   ┌─────────────────────────────┐
  │ Successfully fetched JSON  │   │ Tag to 'failed_fetch' output│
  └────────────────────────────┘   └─────────────────────────────┘
               │
               ▼
         ┌───────────────────────────────────────────────┐
         │ WriteToHL7v2Store (ParDo)                     │
         │ → Starts ThreadPoolExecutor (2 threads)       │
         │ → Calls HL7v2 POST :ingest API                │
         │ → Retries on 429/500/503                      │
         │ → Timeout in 60s                              │
         └───────────────────────────────────────────────┘
               │                             │
               │                             ▼
               │                  ┌──────────────────────────────┐
               │                  │ Non-retryable / Timeout /    │
               │                  │ Exception                    │
               │                  └──────────────────────────────┘
               │                             │
               ▼                             ▼
  ┌────────────────────────────┐   ┌─────────────────────────────┐
  │ Successfully ingested JSON │   │ Tag to 'failed_ingest'      │
  └────────────────────────────┘   └─────────────────────────────┘
Overview of whole process
Purpose of the Pipeline This streaming pipeline reads HL7v2 message IDs from a Pub/Sub subscription, fetches the full message content from a source HL7v2 FHIR store, and writes those messages to a destination HL7v2 FHIR store — with retry logic, timeouts, and logging throughout.

Key Components and Flow
Pipeline Configuration and Setup CustomPipelineOptions: Adds a custom argument (--config_path) for passing a config JSON file.
config.json: Contains keys like project_id, source_hl7v2_store, dest_hl7v2_store, pubsub_subscription, etc.

Reads the file and loads parameters into global variables.

DoFn: LogPubSubMessageID
Decodes and logs the raw Pub/Sub message for debugging.

Input: Pub/Sub message bytes.

Output: Same bytes, passed to the next step.

DoFn: FetchHL7v2Message
Purpose: Fetch the full HL7v2 message from Cloud Healthcare API using the message ID.

Setup: Uses google.auth and AuthorizedSession for authenticated requests.

Retry Logic:
Retries for transient HTTP statuses (429, 500, etc.).

Uses exponential backoff with randomness.

Times out the thread in 60 seconds using ThreadPoolExecutor.

Metrics: Tracks successful fetches and failures.

Output:
main: Successfully fetched message JSON.

failed_fetch: Failed message IDs.

DoFn: WriteToHL7v2Store
Purpose: Ingest the HL7v2 message into the destination FHIR store.

Payload: {"message": {"data": <hl7v2_data>}}

Uses similar retry and timeout logic as the fetch step.

Metrics: Tracks successful and failed ingestions.

Output:
main: Successful ingestion responses.

failed_ingest: Messages that failed to ingest.

Pipeline Construction (run function) python Copy Edit with beam.Pipeline(options=pipeline_options) as pipeline: Read from Pub/Sub:
Reads raw Pub/Sub messages that contain HL7v2 message paths/IDs.

Log Raw Pub/Sub:
Decodes and logs them for traceability.

Fetch Messages:
Calls FetchHL7v2Message to get the full message from the FHIR store.

Write Messages:
Sends successfully fetched messages to WriteToHL7v2Store.

Error Handling:
Logs failed fetches and ingestions with severity level error.

Monitoring and Metrics
Uses Metrics.counter() to track:

Messages fetched.

Failed fetches.

Messages ingested.

Failed ingestions.

These are visible in Cloud Monitoring (Stackdriver).

Deployment Parameters
runner="DataflowRunner": Runs on Google Cloud Dataflow.

streaming=True: It's a streaming pipeline (due to Pub/Sub input).

Uses autoscaling based on throughput.

Worker configuration (machine type, min/max workers) comes from config.

Summary
This is a robust, fault-tolerant, streaming Dataflow pipeline that moves HL7v2 messages from Pub/Sub → fetches from a source FHIR store → ingests into a destination FHIR store — with retries, logging, and metric tracking.
