
import json
import datetime
from google.cloud import bigquery_datatransfer_v1
from google.auth import default
from google.protobuf import field_mask_pb2

def create_or_update_scheduled_query_from_json(scheduled_json_path):
    try:
        # Load the main configuration JSON file
        with open(scheduled_json_path, 'r') as scheduled_json_file:
            config = json.load(scheduled_json_file)

        json_file_path = config.get("json_file_path")
        if not json_file_path:
            raise ValueError("Error: 'json_file_path' not found in main.json.")

        # Load the test JSON configuration
        with open(json_file_path, 'r') as json_file:
            json_config = json.load(json_file)

        # Debugging: Print loaded JSON configuration
        print("🔹 Loaded JSON Config:", json.dumps(json_config, indent=4))

        # Extract required configurations
        required_fields = ["project_id", "dataset_id", "display_name"]

        # Only require "sql_file_path" for new scheduled queries (not updates)
        if not json_config.get("transfer_config_name"):
            required_fields.append("sql_file_path")

        missing_fields = [field for field in required_fields if not json_config.get(field)]

        if missing_fields:
            raise ValueError(f"Error: Missing required configuration fields in the JSON file: {missing_fields}")

        project_id = json_config["project_id"]
        dataset_id = json_config["dataset_id"]
        display_name = json_config["display_name"]
        sql_file_path = json_config.get("sql_file_path")  # Now optional for updates
        schedule = json_config.get("schedule")  # Schedule can be None
        destination_table_name_template = json_config.get("destination_table_name_template")
        pubsub_topic = json_config.get("pubsub_topic")
        manual_run = json_config.get("manual_run", False)
        write_disposition = json_config.get("write_disposition")
        partitioning_field = json_config.get("partitioning_field")
        service_account_name = json_config.get("service_account_name")
        existing_transfer_config_name = json_config.get("transfer_config_name")

        # Initialize BigQuery Data Transfer client
        credentials, _ = default()
        client = bigquery_datatransfer_v1.DataTransferServiceClient(credentials=credentials)
        parent = client.common_project_path(project_id)

        # Read SQL query from file only if provided (for new queries)
        query = None
        if sql_file_path:
            with open(sql_file_path, 'r') as sql_file:
                query = sql_file.read()
        elif existing_transfer_config_name:
            # Retrieve the existing transfer config to get the query
            existing_config = client.get_transfer_config(name=existing_transfer_config_name)
            query = existing_config.params.get("query", None)

        if not query:
            raise ValueError("Error: Missing SQL query. Provide 'sql_file_path' or ensure the existing query is available.")

        # Handle schedule options
        schedule_string = None
        disable_auto_scheduling = False
        start_time, end_time = None, None

        if isinstance(schedule, dict):
            start_time_str = schedule.get("start_time")
            end_time_str = schedule.get("end_time")

            if not start_time_str:
                raise ValueError("Error: 'start_time' must be specified in the schedule.")

            start_time = datetime.datetime.strptime(start_time_str, "%m/%d/%y, %I:%M %p")
            end_time = datetime.datetime.strptime(end_time_str, "%m/%d/%y, %I:%M %p") if end_time_str else None
            schedule_string = f"every day {start_time.strftime('%H:%M')}"
        elif schedule is None or schedule == "on-demand":
            schedule_string = ""  # Empty string removes schedule
            disable_auto_scheduling = True  # Mark as On-Demand
        else:
            schedule_string = schedule

        params = {"query": query}  # Ensure query is always included
        if destination_table_name_template:
            params["destination_table_name_template"] = destination_table_name_template
        if write_disposition:
            params["write_disposition"] = write_disposition
        if partitioning_field:
            params["partitioning_field"] = partitioning_field

        transfer_config = bigquery_datatransfer_v1.TransferConfig(
            name=existing_transfer_config_name if existing_transfer_config_name else "",
            destination_dataset_id=dataset_id,
            display_name=display_name,
            data_source_id="scheduled_query",
            params=params,
            schedule=schedule_string,
            schedule_options=bigquery_datatransfer_v1.ScheduleOptions(
                disable_auto_scheduling=disable_auto_scheduling,
                start_time=start_time if start_time else None,
                end_time=end_time if end_time else None,
            ),
        )

        if existing_transfer_config_name:
            update_mask_paths = [
                "destination_dataset_id", "display_name", "params", "schedule_options"
            ]

            if schedule_string is not None:
                transfer_config.schedule = schedule_string
                update_mask_paths.append("schedule")

            update_mask = field_mask_pb2.FieldMask(paths=update_mask_paths)
            updated_config = client.update_transfer_config(
                {
                    "transfer_config": transfer_config,
                    "update_mask": update_mask,
                }
            )
            print(f"Updated scheduled query: {updated_config.name}")

            # Update service account separately
            update_mask = field_mask_pb2.FieldMask(paths=["service_account_name"])
            transfer_config = bigquery_datatransfer_v1.TransferConfig(name=existing_transfer_config_name)
            updated_config = client.update_transfer_config(
                {
                    "transfer_config": transfer_config,
                    "update_mask": update_mask,
                    "service_account_name": service_account_name,
                }
            )
            print(f"Updated service account: {updated_config.name}")

            # Update Pub/Sub Topic (Corrected Placement)
            if pubsub_topic:
                print(f"Updating Pub/Sub topic to: {pubsub_topic}")
                update_mask = field_mask_pb2.FieldMask(paths=["notification_pubsub_topic"])
                transfer_config = bigquery_datatransfer_v1.TransferConfig(
                    name=existing_transfer_config_name, 
                    notification_pubsub_topic=pubsub_topic
                )
                updated_config = client.update_transfer_config(
                    {
                        "transfer_config": transfer_config,
                        "update_mask": update_mask
                    }
                )
                print(f"Updated Pub/Sub topic: {updated_config.notification_pubsub_topic}")

        else:
            # Create new transfer config
            created_config = client.create_transfer_config(
                bigquery_datatransfer_v1.CreateTransferConfigRequest(
                    parent=parent,
                    transfer_config=transfer_config,
                    service_account_name=service_account_name,
                )
            )
            print(f"Created scheduled query: {created_config.name}")


        # Run the query manually if manual_run is True
        if manual_run and existing_transfer_config_name:
            print(f"Triggering manual run for {existing_transfer_config_name}...")
            requested_run_time = datetime.datetime.utcnow()
            
            manual_run_request = bigquery_datatransfer_v1.StartManualTransferRunsRequest(
                parent=existing_transfer_config_name,  
                requested_run_time=requested_run_time
            )

            manual_run_response = client.start_manual_transfer_runs(request=manual_run_request)
            print(f"Manual run started: {manual_run_response}")


    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    scheduled_json_path = "./scheduled_query_jsons/main.json"
    create_or_update_scheduled_query_from_json(scheduled_json_path)
