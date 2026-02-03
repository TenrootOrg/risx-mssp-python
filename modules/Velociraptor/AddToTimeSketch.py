import grpc
import json
from pyvelociraptor import api_pb2
from pyvelociraptor import api_pb2_grpc
import yaml
import time
import argparse
import os
import sys
import subprocess
import modules.Velociraptor.VelociraptorScript
from datetime import datetime, timedelta
import additionals.funcs
import requests
import traceback
import os
import ssl
import warnings
from timesketch_api_client import client
from timesketch_import_client import importer as ts_importer
import urllib3


# Disable SSL warnings globally
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Modify SSL context globally
ssl._create_default_https_context = ssl._create_unverified_context
def connect_timesketch_api(config, logger):
    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Extract Timesketch credentials from the configuration
    try:
        logger.info("Connecting timesketch api")
        timesketch_config = config['ClientData']['API']['Timesketch']
        host_uri = f"https://{timesketch_config['IP']}"  # Ensure the URL is correctly formatted
        username = timesketch_config['Username']
        password = timesketch_config['Password']

        # Initialize the Timesketch API client with SSL verification disabled
        api = client.TimesketchApi(
            host_uri=host_uri,
            username=username,
            password=password,
            verify=False  # Disable SSL certificate verification
        )
        logger.info("TimeSketch api connected!")
        return api
    except Exception as e:
        logger.error(traceback.format_exc())
        return None

def is_plaso_running(logger):
    try:
        logger.info("In plaso_running function!")
        # Run the docker ps command
        result = subprocess.run(['sudo','docker', 'ps'], capture_output=True, text=True, check=True)
        logger.info("Checking if plaso container is already running...")
        # Check if log2timeline/plaso is in the output
        return 'log2timeline/plaso' in result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to run docker ps command: {e.stderr}")
        return False

def _format_vql_parameters(params):
    """Format parameters dictionary for VQL - matches Velociraptor 0.75 format"""
    import json
    parts = []
    for key, value in params.items():
        if isinstance(value, str):
            parts.append(f"`{key}`='{value}'")
        elif isinstance(value, bool):
            parts.append(f"`{key}`={'true' if value else 'false'}")
        elif isinstance(value, (int, float)):
            parts.append(f"`{key}`={value}")
        elif isinstance(value, list):
            # Format as JSON string so Velociraptor can parse it as array
            # The artifact's VQL uses parse_json_array() to handle this
            parts.append(f"`{key}`='{json.dumps(value)}'")
    return ", ".join(parts)


def run_kape_artifact(stub, client_id, kape_collection, timeout, cpu_limit,  logger):
    """Run KAPE/Triage artifact on client using Velociraptor 0.75 format

    Args:
        stub: Velociraptor gRPC stub
        client_id: Target client ID
        kape_collection: KAPE target(s) - string or list (e.g., "_SANS_Triage" or ["_J", "_BasicCollection"])
        timeout: Timeout in milliseconds
        cpu_limit: CPU limit percentage
        logger: Logger instance

    Returns:
        flow_id string or empty string on failure
    """
    flow_id = ""
    artifact_name = 'Windows.Triage.Targets'
    max_bytes = "9000000000000000"

    # Convert kape_collection to list format for Velociraptor 0.75
    if isinstance(kape_collection, list):
        targets_list = kape_collection
    elif isinstance(kape_collection, str):
        targets_list = [kape_collection]
    else:
        targets_list = ["_SANS_Triage"]  # Default

    # Normalize targets - ensure underscore prefix for collection targets
    normalized_targets = []
    for target in targets_list:
        if target.startswith('_'):
            normalized_targets.append(target)
        elif target in ['BasicCollection', 'KapeTriage', 'SANS_Triage', 'J', 'Live']:
            normalized_targets.append(f'_{target}')
        else:
            # Non-collection targets like "!Password", "AVG" don't need underscore
            normalized_targets.append(target)

    # Build parameters dict
    # Note: Parameter names must match the artifact definition exactly (case-sensitive!)
    params = {
        "Targets": normalized_targets,           # List of KAPE targets to collect
        "Devices": ["C:"],                       # json_array of devices to search (note: plural!)
        "VSS_MAX_AGE_DAYS": 0,                  # VSS analysis (0=disabled, >0=days back)
        "WORKERS": 5,                            # Number of concurrent workers (default: 5)
        "SlowGlobRegex": "^\\*\\*",             # Regex to eliminate slow globs (default from Triage artifact)
    }

    # Format parameters for VQL
    params_str = _format_vql_parameters(params)

    # Build VQL with spec=dict() format (Velociraptor 0.75)
    query = f"""
SELECT collect_client(
    client_id='{client_id}',
    artifacts='{artifact_name}',
    spec=dict(`{artifact_name}`=dict({params_str})),
    timeout={timeout},
    cpu_limit={cpu_limit},
    max_bytes={max_bytes}
) AS Flow FROM scope()
"""

    logger.info("Running KAPE artifact query (Velociraptor 0.75 format).")
    logger.info("KAPE Targets: " + str(normalized_targets))
    logger.info("Query:" + query)

    request = api_pb2.VQLCollectorArgs(
        max_wait=10,
        max_row=100,
        Query=[api_pb2.VQLRequest(
            Name=artifact_name,
            VQL=query,
        )]
    )

    for response in stub.Query(request):
        if response.Response:
            logger.info("Response:" + str(response))
            response_data = json.loads(str(response.Response))

            # Velociraptor 0.75 returns nested structure: response[0]["Flow"]["flow_id"]
            if response_data and len(response_data) > 0:
                flow_obj = response_data[0].get("Flow", {})
                if isinstance(flow_obj, dict):
                    flow_id = flow_obj.get("flow_id", "")

    if flow_id != "":
        logger.info("Flow id:" + str(flow_id))
        return flow_id
    logger.error("Failed to run KAPE artifact.")
    return ""


def get_timeline_status(api, sketch_id, timeline_id, logger):
    try:
        # Fetch the sketch
        sketch = api.get_sketch(sketch_id)
        if not sketch:
            logger.error(f"Sketch with ID {sketch_id} not found.")
            return None

        # Fetch the timeline directly using its ID
        timeline = sketch.get_timeline(timeline_id)
        if timeline:
            logger.info(f"Found timeline. Status: {timeline.status}")
            return timeline.status
        else:
            logger.warning(f"Timeline with ID {timeline_id} not found in sketch {sketch_id}.")
            return None

    except Exception as e:
        logger.error(f"Error fetching timeline status: {str(e)}")
        return None

def get_flow_state(stub, client_id, flow_id, timeout, logger):
    query = f"LET collection <= get_flow(client_id='{client_id}', flow_id='{flow_id}') SELECT * FROM collection"
    request = api_pb2.VQLCollectorArgs(
        Query=[api_pb2.VQLRequest(VQL=query)],
    )

    all_rows = []  # Initialize an empty list to accumulate all rows
    try:
        timeout_seconds = timeout  # Timeout after 10 seconds
        for response in stub.Query(request, timeout=timeout_seconds):
            if response.Response:
                rows = json.loads(response.Response)
                if rows:
                    all_rows.extend(rows)  # Append the rows from this response to the list
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            logger.error("The call timed out.")
        else:
            logger.error(f"An RPC error occurred: {e}")

    # Extract the state from the response
    if all_rows:
        state = all_rows[0].get('state', 'UNKNOWN')
        return state

    return 'UNKNOWN'

def run_artifact_on_client(channel, client_id, kape_collection, timeout, cpu_limit, logger):
    logger.info("Running artifact on client!")
    flow_id = ""
    # Here, you would add the logic to perform some action on the client
    # For example, querying data, sending commands, etc.

    stub = api_pb2_grpc.APIStub(channel)
    flow_id = run_kape_artifact(stub, client_id, kape_collection, timeout, cpu_limit, logger)
    if(flow_id == ""):
        logger.error("Failed to run kape artifact!")
        return
    start_time = time.time()
    #timeout = 9999999999  # Run loop for 30 seconds
    state = ""
    check_count = 0
    while True:
        check_count += 1
        state = get_flow_state(stub, client_id, flow_id, timeout, logger)
        elapsed = int(time.time() - start_time)

        if state == "FINISHED":
            logger.info(f"Artifact is done! (Check #{check_count}, Elapsed: {elapsed}s)")
            # Wait a bit more to ensure uploads are fully flushed to disk
            logger.info("Waiting 10 seconds to ensure all uploads are flushed...")
            time.sleep(10)
            return flow_id
        if state == "FAILED":
            logger.error("The flow has been failed!")
            return

        # Check if timeout reached
        if time.time() - start_time > timeout:
            logger.warning("Timeout reached. Exiting the loop.")
            break

        # Log progress and wait before next check
        logger.info(f"Check #{check_count}: Flow state: {state}, Elapsed: {elapsed}s")
        time.sleep(10)  # Wait 10 seconds before checking again
    return


def upload_plaso_direct(api, sketch, plaso_file_path, timeline_name, logger):
    """Upload .plaso file directly to Timesketch using the import API.

    Timesketch's Celery workers will handle the psort processing internally.

    Args:
        api: Connected TimesketchApi client
        sketch: Timesketch sketch object
        plaso_file_path: Path to the .plaso file
        timeline_name: Name for the timeline
        logger: Logger instance

    Returns:
        dict with 'timeline_id', 'celery_task_id', 'index_name'
    """
    logger.info(f"Starting direct .plaso upload to Timesketch")
    logger.info(f"Plaso file: {plaso_file_path}")
    logger.info(f"Timeline name: {timeline_name}")
    logger.info(f"Sketch ID: {sketch.id}")

    # Verify file exists and get size
    if not os.path.exists(plaso_file_path):
        raise FileNotFoundError(f".plaso file not found: {plaso_file_path}")

    plaso_size = os.path.getsize(plaso_file_path)
    logger.info(f"Plaso file size: {plaso_size / (1024*1024):.2f} MB")

    # Create importer streamer for direct .plaso upload
    with ts_importer.ImportStreamer() as streamer:
        streamer.set_sketch(sketch)
        streamer.set_timeline_name(timeline_name)
        streamer.set_data_label('plaso')
        streamer.set_upload_context(f'Uploaded via RISX automation at {datetime.now().isoformat()}')

        logger.info("Uploading .plaso file (this transfers the file to Timesketch)...")
        streamer.add_file(plaso_file_path)

        # Get the results before context manager closes
        timeline_id = getattr(streamer, '_timeline_id', None)
        celery_task_id = getattr(streamer, 'celery_task_id', None)
        index_name = getattr(streamer, '_index', None)

    logger.info(f"Upload complete!")
    logger.info(f"Timeline ID: {timeline_id}")
    logger.info(f"Celery Task ID: {celery_task_id}")
    logger.info(f"Index Name: {index_name}")

    return {
        'timeline_id': timeline_id,
        'celery_task_id': celery_task_id,
        'index_name': index_name
    }


def wait_for_timeline_ready(api, sketch_id, timeline_name, timeout_seconds=3600, poll_interval=30, logger=None):
    """Wait for timeline processing to complete with progress updates.

    Args:
        api: Connected TimesketchApi client
        sketch_id: Sketch ID containing the timeline
        timeline_name: Timeline name to monitor
        timeout_seconds: Maximum wait time (default 1 hour)
        poll_interval: Seconds between status checks
        logger: Logger instance

    Returns:
        tuple (success: bool, final_status: str, timeline_id: int or None)
    """
    start_time = time.time()
    sketch = api.get_sketch(sketch_id)

    logger.info(f"Waiting for timeline '{timeline_name}' to be ready...")
    logger.info(f"Timeout: {timeout_seconds}s, Poll interval: {poll_interval}s")

    while True:
        elapsed = int(time.time() - start_time)

        # Check timeout
        if elapsed > timeout_seconds:
            logger.error(f"Timeout waiting for timeline after {elapsed}s")
            return (False, "timeout", None)

        # Get all timelines and find ours by name
        try:
            timelines = sketch.list_timelines()
            timeline = None
            for tl in timelines:
                if tl.name == timeline_name:
                    timeline = tl
                    break

            if not timeline:
                logger.info(f"Timeline '{timeline_name}' not yet visible, waiting... (elapsed: {elapsed}s)")
                time.sleep(poll_interval)
                continue

            status = timeline.status
            logger.info(f"Timeline status: {status} (elapsed: {elapsed}s)")

            if status == "ready":
                logger.info(f"Timeline '{timeline_name}' is ready!")
                return (True, status, timeline.id)
            elif status == "fail":
                logger.error(f"Timeline '{timeline_name}' processing failed")
                return (False, status, timeline.id)
            elif status in ["processing", "pending"]:
                time.sleep(poll_interval)
            else:
                logger.warning(f"Unknown status: {status}, continuing to wait...")
                time.sleep(poll_interval)

        except Exception as e:
            logger.warning(f"Error checking timeline status: {e}, retrying...")
            time.sleep(poll_interval)

    return (False, "unknown", None)


def get_sketch_id(api, sketch_name, logger):
    # Get the list of sketches and search for the given sketch name
    logger.info("Getting list of sketches!")
    sketches = api.list_sketches()
    logger.info("List of sketches:" + str(sketches))
    for sketch in sketches:
        if sketch.name == sketch_name:
            return sketch.id
    return None

def get_timeline_id(api, sketch_id, timeline_name, logger):
    try:
        # Get the sketch
        logger.info(f"Fetching sketch with ID: {sketch_id}")
        sketch = api.get_sketch(sketch_id)
        # Test make it public
        #sketch.grant_permission(permission='read', user='public')
        logger.info(f"Sketch Name [Check if its not empty]: {sketch.name}")
        logger.info(f"Sketch Labels: {sketch.labels}")
        if not sketch:
            logger.error(f"Sketch with ID {sketch_id} not found.")
            return None

        # Get the list of timelines for the given sketch
        logger.info(f"Getting list of timelines for sketch ID: {sketch_id}")
        timelines = sketch.list_timelines()
        logger.info("Pre timelines loop!")
        for timeline in timelines:
            logger.info("Timeline loop:" + str(timeline.id))
            if timeline.name == timeline_name:
                logger.info(f"Found timeline '{timeline_name}' with ID: {timeline.id}")
                return timeline.id

        logger.warning(f"Timeline '{timeline_name}' not found in sketch ID: {sketch_id}")
        return None

    except AttributeError as e:
        logger.error(f"API method error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def make_sketches_public(api, logger):
    sketches = list(api.list_sketches())
            
    if sketches:   
        total_sketches = len(sketches)
        logger.info(f"Found {total_sketches} sketches")
        
        for sketch in sketches:
            logger.info(f"Making sketch '{sketch.name}' (ID: {sketch.id}) public")
            # First attempt - use set_acl if available (newer API)
            sketch.set_acl(user_list=[], group_list=[], make_public=True)
def start_timesketch(row, general_config, logger):
    # Redirect stderr to devnull to prevent Node.js maxBuffer overflow and false-failure detection
    # All meaningful output goes through the logger to log files
    import sys, os
    sys.stderr = open(os.devnull, "w")

    # Here, based on the parsed arguments, you can call different functions
    # For example:
    try:
        logger.info("WhoAmI:" + str(subprocess.run(['whoami'], stdout=subprocess.PIPE, text=True).stdout.strip()))
        
        # Clean up any orphaned (stopped/exited) plaso containers from previous crashed runs
        # This prevents accumulation of dead containers and ensures is_plaso_running only detects active jobs
        logger.info("Cleaning up any orphaned plaso containers from previous runs...")
        subprocess.run(
            'sudo docker ps -a -q --filter "ancestor=log2timeline/plaso" --filter "status=exited" | xargs -r sudo docker rm -f',
            shell=True, capture_output=True, text=True
        )
        subprocess.run(
            'sudo docker ps -a -q --filter "ancestor=log2timeline/plaso:latest" --filter "status=exited" | xargs -r sudo docker rm -f',
            shell=True, capture_output=True, text=True
        )
        
        if(is_plaso_running(logger)):
            logger.error("Timesketch plaso is already running. Let it finish and run again later")
            row["Status"] = "Failed"
            row["Error"] = "Timesketch plaso is already running. Let it finish and run again later"
            return row

        # Clean plaso folder at the beginning of each run
        user_name = subprocess.run(['whoami'], stdout=subprocess.PIPE, text=True).stdout.strip()
        if user_name == "node":
            plaso_cleanup_dir = "/plaso"
        else:
            plaso_cleanup_dir = "/home/tenroot/setup_platform/workdir/risx-mssp/backend/plaso"
        # DEBUG: Comment out plaso folder cleanup to allow debugging
        # logger.info(f"Cleaning plaso folder before starting: {plaso_cleanup_dir}")
        # additionals.funcs.run_subprocess(f"rm -rf {plaso_cleanup_dir}/*", "", logger)
    
        logger.info("=" * 60)
        logger.info("TIMESKETCH PIPELINE STARTED")
        logger.info("=" * 60)
        logger.info("Configuration:")
        logger.info(f"  - ArtifactTimeOutInMinutes: {row.get('ArtifactTimeOutInMinutes', 'N/A')}")
        logger.info(f"  - SketchName: {row['Arguments'].get('SketchName', 'N/A')}")
        logger.info(f"  - KapeCollection: {row['Arguments'].get('KapeCollection', 'N/A')}")
        logger.info(f"  - CPUThrottling: {row['Arguments'].get('CPUThrottling', 'N/A')}%")
        logger.info(f"  - MemoryThrottling: {row['Arguments'].get('MemoryThrottling', 'N/A')}%")
        logger.info(f"  - Parsers: {row['Arguments'].get('Parsers', '*')}")
        logger.info(f"  - Population: {len(row.get('Population', []))} client(s)")
        logger.info("-" * 60)
        config_path = os.path.join("modules", "Velociraptor", "dependencies", "api.config.yaml")
        velociraptor_config = ""
        with open(config_path, 'r') as f:
            velociraptor_config = yaml.safe_load(f)

        creds = grpc.ssl_channel_credentials(
        root_certificates=velociraptor_config["ca_certificate"].encode("utf8"),
        private_key=velociraptor_config["client_private_key"].encode("utf8"),
        certificate_chain=velociraptor_config["client_cert"].encode("utf8"))
        options = (('grpc.ssl_target_name_override', "VelociraptorServer",),)

        # Establish a secure channel
        with grpc.secure_channel(velociraptor_config["api_connection_string"], creds, options) as channel:
            host_client_id_dict = modules.Velociraptor.VelociraptorScript.get_clients(logger, False)
            logger.info("Current host_client_id_dict:" + str(host_client_id_dict))
            logger.info("Running artifact")
            logger.info("TimeSketchPopulation:" + str(row["Population"]))
            for client in row["Population"]:
                extract_dir = None  # Initialize for cleanup in exception handler
                try:
                    client_name = client["asset_string"]
                    if(client_name in host_client_id_dict):
                        logger.info("Client name:" + client_name)
                        client_id = host_client_id_dict[client_name]
                        logger.info("-" * 40)
                        logger.info(f"[STEP 1/6] KAPE ARTIFACT COLLECTION")
                        logger.info("-" * 40)
                        logger.info(f"Client: {client_name} (ID: {client_id})")
                        logger.info(f"KAPE Collection: {row['Arguments']['KapeCollection']}")
                        logger.info(f"Timeout: {row['ArtifactTimeOutInMinutes']} seconds")
                        logger.info(f"CPU Limit: 50%")
                        cpu_limit = 50
                        flow_id = run_artifact_on_client(channel=channel, client_id=client_id, kape_collection=row["Arguments"]["KapeCollection"], timeout = int(row["ArtifactTimeOutInMinutes"]), cpu_limit = cpu_limit, logger=logger)
                        logger.info(f"[STEP 1/6] COMPLETE - Flow ID: {flow_id}")
                        # Get the username
                        user_name = subprocess.run(['whoami'], stdout=subprocess.PIPE, text=True).stdout.strip()
                        # user_name="tenroot"

                        

                        # Get the hostname
                        host_name = subprocess.run(['uname', '-n'], stdout=subprocess.PIPE, text=True).stdout.strip()
                        
                        # collect_path = os.path.join("home", user_name, "setup_platform", "workdir", "velociraptor", "velociraptor", "clients", client_id, "collections", flow_id, "uploads")
                        # logger.info(f"Collect path: {collect_path}")
                        cpus = additionals.funcs.closest_cpu_percentage(int(row['Arguments']['CPUThrottling']))
                        ram = additionals.funcs.closest_memory_percentage(int(row['Arguments']['MemoryThrottling'])) + "g"
                        logger.info("Number of CPUs:" + cpus)
                        logger.info("Number of Memory:" + ram)

                        logger.info("-" * 40)
                        logger.info(f"[STEP 2/6] EXPORT COLLECTION AS ZIP")
                        logger.info("-" * 40)
                        logger.info(f"Exporting collection {flow_id} as ZIP for plaso processing...")
                        export_query = f"""
SELECT create_flow_download(
    client_id='{client_id}',
    flow_id='{flow_id}',
    wait=true,
    expand_sparse=true
) AS Export FROM scope()
"""
                        stub = api_pb2_grpc.APIStub(channel)
                        export_request = api_pb2.VQLCollectorArgs(
                            max_wait=600,
                            Query=[api_pb2.VQLRequest(VQL=export_query)]
                        )

                        export_path = None
                        for response in stub.Query(export_request):
                            if response.Response:
                                logger.info(f"Export response: {response.Response}")
                                export_data = json.loads(response.Response)
                                if export_data and len(export_data) > 0:
                                    export_info = export_data[0].get('Export', '')
                                    # Export can be a string path or a dict with 'path' key
                                    if isinstance(export_info, str):
                                        export_path = export_info
                                    elif isinstance(export_info, dict):
                                        export_path = export_info.get('path', '')

                        if not export_path:
                            logger.error("Failed to export collection as ZIP")
                            raise Exception("Collection export failed")

                        # Remove fs: prefix if present
                        if export_path.startswith('fs:'):
                            export_path = export_path[3:]

                        logger.info(f"[STEP 2/6] COMPLETE - Exported to: {export_path}")

                        # Extract ZIP to temp directory for plaso
                        # Note: Container's /tmp maps to /home/tenroot/setup_platform/workdir/tmp on host
                        # Docker commands need host paths for volume mounts
                        extract_dir = f"/tmp/plaso_extract_{client_name}_{flow_id}"
                        host_extract_dir = f"/home/tenroot/setup_platform/workdir/tmp/plaso_extract_{client_name}_{flow_id}"
                        zip_path = f"/velociraptor{export_path}"

                        logger.info("-" * 40)
                        logger.info(f"[STEP 3/6] COPY ZIP FOR PLASO")
                        logger.info("-" * 40)
                        logger.info(f"Copying ZIP from Velociraptor container...")
                        additionals.funcs.run_subprocess(f"sudo rm -rf {extract_dir}", "", logger)
                        additionals.funcs.run_subprocess(f"sudo mkdir -p {extract_dir}", "", logger)

                        # Copy ZIP from velociraptor container (plaso can process zip directly)
                        zip_file_path = f"{extract_dir}/collection.zip"
                        host_zip_path = f"{host_extract_dir}/collection.zip"
                        additionals.funcs.run_subprocess(
                            f"sudo docker cp velociraptor:{zip_path} {zip_file_path}",
                            "", logger
                        )

                        logger.info(f"[STEP 3/6] COMPLETE - ZIP file ready: {host_zip_path}")

                        # Generate timestamp for log files
                        log_datetime = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")

                        # Docker volume mounts always use HOST paths (even when running from inside a container)
                        # But file checks use the path as seen from THIS process
                        plaso_host_dir = "/home/tenroot/setup_platform/workdir/risx-mssp/backend/plaso"
                        if user_name == "node":
                            plaso_dir = "/plaso"  # Container's view of the mounted volume
                        else:
                            plaso_dir = plaso_host_dir  # Running on host directly

                        # Get parsers from config
                        parsers = row["Arguments"].get("Parsers", "*")

                        # Get number of workers from Arguments (default: 2)
                        num_workers = row["Arguments"].get("NumberOfWorkers", 2)

                        # Build log2timeline command - always exclude winevtx (causes Timesketch psort crash)
                        # Combine user parsers with !winevtx exclusion
                        # NOTE: winevtx exclusion disabled - bug fix applied to plaso image
                        # if parsers and parsers != "*":
                        #     parser_arg = f"{parsers},!winevtx"
                        # else:
                        #     parser_arg = "!winevtx"
                        parser_arg = parsers if parsers else "*"
                        command1 = f"sudo docker run --rm -v {host_extract_dir}:{host_extract_dir}:ro -v {plaso_host_dir}/:/data --cpus='{cpus}' --memory='{ram}' --user root log2timeline/plaso:latest log2timeline --parsers '{parser_arg}' --workers {num_workers} --status_view window --status_view_interval 60 --logfile /data/log2timeline_{client_name}_{log_datetime}.log"
                        # Add storage file and source
                        # Note: Full plaso file is uploaded to Timesketch - use Timesketch queries for date filtering
                        command1 += f" --storage-file /data/{client_name}Artifacts.plaso {host_zip_path}"

                        logger.info("-" * 40)
                        logger.info(f"[STEP 4/6] LOG2TIMELINE (PLASO)")
                        logger.info("-" * 40)
                        logger.info(f"Parsers: {parsers}")
                        logger.info(f"CPU: {cpus}, Memory: {ram}, Workers: {num_workers}")
                        logger.info(f"Log file: log2timeline_{client_name}_{log_datetime}.log")

                        # Connect to Timesketch API early to get/create sketch
                        api = connect_timesketch_api(general_config, logger)
                        logger.info("Preparing environment for plaso...")

                        # Delete old plaso file to avoid "Output file already exists" error
                        # Note: sudo rm runs on HOST, so use host path
                        additionals.funcs.run_subprocess(f"sudo rm -f {plaso_host_dir}/{client_name}Artifacts.plaso", "", logger)

                        logger.info("Running log2timeline (this may take a while)...")
                        logger.info(f"Full command: {command1}")
                        plaso_start_time = time.time()
                        additionals.funcs.run_subprocess(command1,"Processing completed", logger)
                        plaso_duration = int(time.time() - plaso_start_time)
                        logger.info(f"[STEP 4/6] COMPLETE - log2timeline finished in {plaso_duration}s")

                        logger.info("-" * 40)
                        logger.info(f"[STEP 5/6] PINFO (PLASO FILE INFO)")
                        logger.info("-" * 40)
                        logs_dir = "/home/tenroot/setup_platform/workdir/risx-mssp/backend/logs"
                        pinfo_command = f"sudo docker run --rm -v {plaso_host_dir}/:/data -v {logs_dir}/:/logs log2timeline/plaso:latest pinfo -w /logs/pinfo_{client_name}_{log_datetime}.log /data/{client_name}Artifacts.plaso"
                        logger.info(f"Running pinfo to extract plaso file statistics...")
                        logger.info(f"Full command: {pinfo_command}")
                        additionals.funcs.run_subprocess(pinfo_command, "", logger)
                        logger.info(f"[STEP 5/6] COMPLETE - pinfo saved to: pinfo_{client_name}_{log_datetime}.log")

                        # Reconnect Timesketch API to get fresh CSRF token (old one may have expired during plaso)
                        logger.info("Reconnecting to Timesketch API (refreshing CSRF token)...")
                        api = connect_timesketch_api(general_config, logger)
                        if not api:
                            raise Exception("Failed to reconnect to Timesketch API after plaso processing")

                        logger.info("-" * 40)
                        logger.info(f"[STEP 6/6] DIRECT PLASO UPLOAD TO TIMESKETCH")
                        logger.info("-" * 40)

                        # Check if input .plaso file exists
                        plaso_file_path = f"{plaso_dir}/{client_name}Artifacts.plaso"
                        if not os.path.exists(plaso_file_path):
                            logger.error(f"Input .plaso file NOT FOUND: {plaso_file_path}")
                            raise Exception(f".plaso file not found: {plaso_file_path}")

                        plaso_size = os.path.getsize(plaso_file_path)
                        logger.info(f"Plaso file: {plaso_file_path}")
                        logger.info(f"Plaso size: {plaso_size / (1024*1024):.2f} MB")

                        # Get or create sketch
                        sketch_name = row["Arguments"]["SketchName"]
                        sketch_id = get_sketch_id(api, sketch_name, logger)

                        # Generate timeline name
                        formatted_datetime = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
                        timeline_name = f"{client_name}_{formatted_datetime}"

                        # Store metadata
                        row["LastIntervalDate"] = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
                        row["ExpireDate"] = formatted_datetime

                        logger.info(f"Sketch Name: {sketch_name}")
                        logger.info(f"Timeline Name: {timeline_name}")

                        # Get or create sketch object
                        if sketch_id is not None:
                            logger.info(f"Using existing sketch with ID: {sketch_id}")
                            sketch = api.get_sketch(sketch_id)
                            row["UniqueID"] = {"SketchID": sketch_id, "TimelineID": timeline_name}
                        else:
                            logger.info(f"Creating new sketch: {sketch_name}")
                            sketch = api.create_sketch(sketch_name)
                            row["UniqueID"] = {"SketchID": sketch.id, "TimelineID": timeline_name}
                            logger.info(f"Created new sketch with ID: {sketch.id}")

                        # Upload .plaso file directly to Timesketch
                        logger.info("Uploading .plaso file directly to Timesketch...")
                        logger.info("(Timesketch workers will handle psort processing internally)")
                        upload_start_time = time.time()

                        upload_result = upload_plaso_direct(
                            api=api,
                            sketch=sketch,
                            plaso_file_path=plaso_file_path,
                            timeline_name=timeline_name,
                            logger=logger
                        )

                        upload_duration = int(time.time() - upload_start_time)
                        logger.info(f"Upload completed in {upload_duration}s")

                        # Wait for Timesketch workers to process the .plaso file
                        logger.info("Waiting for Timesketch to process the .plaso file...")
                        timeout_minutes = int(row.get('ArtifactTimeOutInMinutes', 60))
                        timeout_seconds = timeout_minutes * 60

                        success, final_status, timeline_id = wait_for_timeline_ready(
                            api=api,
                            sketch_id=row["UniqueID"]["SketchID"],
                            timeline_name=timeline_name,
                            timeout_seconds=timeout_seconds,
                            poll_interval=30,
                            logger=logger
                        )

                        if not success:
                            raise Exception(f"Timeline processing failed with status: {final_status}")

                        # Update timeline ID with actual ID from Timesketch
                        if timeline_id:
                            row["UniqueID"]["TimelineID"] = timeline_id

                        logger.info(f"[STEP 6/6] COMPLETE - Timeline ready!")
                        logger.info(f"Sketch ID: {row['UniqueID']['SketchID']}")
                        logger.info(f"Timeline ID: {row['UniqueID']['TimelineID']}")

                        logger.info("-" * 40)
                        logger.info("CLEANUP")
                        logger.info("-" * 40)
                        logger.info(f"Cleaning up extracted files: {extract_dir}")
                        additionals.funcs.run_subprocess(f"sudo rm -rf {extract_dir}", "", logger)

                        api.session.close()
                except Exception as e:
                    logger.error("Mid run timesketch error:" + str(e))
                    logger.error(traceback.format_exc())
                    # Cleanup extracted files if they exist
                    if extract_dir:
                        logger.info(f"Cleaning up extracted files after error: {extract_dir}")
                        #additionals.funcs.run_subprocess(f"sudo rm -rf {extract_dir}", "", logger)
                    #make_sketches_public(api, logger)
                    api.session.close()

            row["Status"] = "Complete"
            logger.info("=" * 60)
            logger.info("TIMESKETCH PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            # DEBUG: Comment out plaso folder cleanup to allow debugging
            logger.info(f"Cleaning plaso folder: {plaso_cleanup_dir}")
            additionals.funcs.run_subprocess(f"rm -rf {plaso_cleanup_dir}/*", "", logger)
            return row
    except Exception as e:
        logger.error("=" * 60)
        logger.error("TIMESKETCH PIPELINE FAILED")
        logger.error("=" * 60)
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
        row["Status"] = "Failed"
        row["Error"] = "Unknown error:" + str(e)
        # DEBUG: Comment out plaso folder cleanup to allow debugging
        logger.info("Cleaning plaso folder...")
        user_name = subprocess.run(['whoami'], stdout=subprocess.PIPE, text=True).stdout.strip()
        if user_name == "node":
            plaso_cleanup_dir = "/plaso"
        else:
            plaso_cleanup_dir = "/home/tenroot/setup_platform/workdir/risx-mssp/backend/plaso"
        additionals.funcs.run_subprocess(f"rm -rf {plaso_cleanup_dir}/*", "", logger)
        return row
