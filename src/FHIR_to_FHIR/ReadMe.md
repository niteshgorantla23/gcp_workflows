#  Pub/Sub to FHIR Dataflow Pipeline

This Apache Beam pipeline reads FHIR resources from a **Google Cloud Pub/Sub** subscription, filters them based on facility-specific logic, and writes valid resources to a **FHIR store** using the Google Cloud **Healthcare API**.

---

## Architecture

- **Source**: Google Cloud Pub/Sub subscription  
- **Transformations**:  
  - Parse and validate incoming messages  
  - Filter based on `resourceType` and content  
- **Sink**: Google Cloud Healthcare FHIR store via REST API (`PUT`)

---

##  Configuration

All configuration values (project, region, bucket, FHIR store URL, subscription path) should be passed via environment variables or pipeline options, not hardcoded.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


## Run on Dataflow
Update your CLI command with your project-specific values:

bash
Copy
Edit
python pubsub_to_fhir_pipeline.py \
  --runner DataflowRunner \
  --project <YOUR_PROJECT_ID> \
  --region <YOUR_REGION> \
  --staging_location gs://<YOUR_BUCKET>/staging \
  --temp_location gs://<YOUR_BUCKET>/temp \
  --streaming \
  --setup_file ./path/to/setup.py \
  --experiments enable_preflight_validation=false

---
## Filtering Logic

Resources are filtered based on type and embedded metadata.

resourceType	Criteria
General: Resources	meta.source must include allowed facility code (e.g., COCULN)
Location:	Identifier type is COID and value matches specific facility ID
Provenance:	Extension must include MessageControlID containing allowed values
Organization:	Identifier type/value or system must match known facility criteria
RelatedPerson:	Relationship system must include known facility identifier
Practitioner:	Identifier system must match known organization code (e.g., HCAORL)

-------------------------------------------------------------------------------------------------------
## Project Structure
bash
Copy
Edit
.
├── pubsub_to_fhir_pipeline.py     # Main Beam pipeline
├── requirements.txt               # Python dependencies
├── setup.py                       # Beam setup file
└── README.md                      # This file
 Authentication
The pipeline uses Application Default Credentials (ADC) and authenticates using:

plaintext
Copy
Edit
https://www.googleapis.com/auth/cloud-healthcare

-------------------------------------------------------------------------------------------------------------
## permissions needed:

Cloud Pub/Sub Subscriber

Healthcare FHIR Store Editor

Dataflow Developer

---
## Notes
Accepts both raw FHIR JSON and FHIR resource URLs.

Logs accepted, filtered, and failed messages.

Designed for streaming mode using Pub/Sub.

## Next implementations adding the transient errors as retry logic in Dataflow job

429 – Rate limit

500 – Internal server error

502 – Bad gateway

503 – Service unavailable



