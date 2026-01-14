import logging
import os
import json
import sys
import traceback
import time
import base64
from pyvelociraptor import api_pb2
from pyvelociraptor import api_pb2_grpc
import grpc
import yaml
import zipfile
import subprocess
import time
import random
import string

# Set the script directory and parent directory
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "../../"))
os.chdir(parent_dir)
print(parent_dir)
# Add parent directory to Python path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import additionals.mysql_functions
import additionals.funcs
import modules.Velociraptor.VelociraptorScript
import ssl
import urllib3
import asyncio

# Disable SSL warnings globally
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Modify SSL context globally to allow unverified HTTPS connections
ssl._create_default_https_context = ssl._create_unverified_context


# Wrapper for run_generic_vql
def run_generic_vql(query, logger):
    return modules.Velociraptor.VelociraptorScript.run_generic_vql(query, logger)


async def async_run_generic_vql(query, logger):
    return await asyncio.to_thread(run_generic_vql, query, logger)


def transform_kape_parameters(parameters):
    """Transform old KAPE boolean format to Velociraptor 0.75 Triage.Targets format

    Velociraptor 0.75 changed KAPE parameters:
    1. Individual boolean flags → Targets list
    2. Parameter name changes: Device → Devices, VSSAnalysis → VSS_MAX_AGE_DAYS

    OLD: {_KapeTriage: true, _J: true, Device: 'C:', VSSAnalysis: 'Y'}
    NEW: {Targets: ['_KapeTriage', '_J'], Devices: ['C:'], VSS_MAX_AGE_DAYS: 0}

    Args:
        parameters: Dictionary of KAPE parameters in old or new format

    Returns:
        Dictionary with Targets list format and updated parameter names
    """
    # If already has Targets parameter and new parameter names, return as-is
    if 'Targets' in parameters and 'Devices' in parameters:
        return parameters

    targets = []
    other_params = {}

    for key, value in parameters.items():
        # Check if this is a KAPE target (starts with underscore or is a known target name)
        if key.startswith('_') or key in ['BasicCollection', 'KapeTriage', 'SANS_Triage', 'J', 'Live']:
            if value:  # If true/enabled
                # Ensure underscore prefix for collection targets
                target_name = key if key.startswith('_') else f'_{key}'
                targets.append(target_name)
        # Transform old parameter names to new ones
        elif key == 'Device':
            # Device (string) → Devices (array)
            other_params['Devices'] = [value] if isinstance(value, str) else value
        elif key == 'VSSAnalysis':
            # VSSAnalysis ('Y'/'N') → VSS_MAX_AGE_DAYS (int, 0=disabled)
            other_params['VSS_MAX_AGE_DAYS'] = 0
        else:
            # Keep other parameters as-is
            other_params[key] = value

    # Add targets list if any were found
    if targets:
        other_params['Targets'] = targets
    elif 'Targets' not in other_params:
        # Default to _SANS_Triage if no targets specified
        other_params['Targets'] = ['_SANS_Triage']

    return other_params


def connect_my_sql(env_dict, logger):
    connection = additionals.mysql_functions.setup_mysql_connection(env_dict, logger)
    tmpId = "Empty"
    config_data = json.loads(
        additionals.mysql_functions.execute_query(
            connection,
            f"SELECT config FROM on_premise_velociraptor where config_id = '{sys.argv[1] or tmpId}'",
            logger,
        )[0][0]
    )
    config = json.loads(
        additionals.mysql_functions.execute_query(
            connection,
            f"SELECT JSON_EXTRACT(config,'$.General.AgentLinks') FROM configjson",
            logger,
        )[0][0]
    )
    return [config_data, config]


def create_zip(files_to_zip, zip_file_path, logger):
    """
    Creates a zip file containing the specified files or directories, placing all files in the root of the zip.
    If a directory is specified, all files within it (including in subdirectories) will be added.

    Args:
        files_to_zip (list): List of file paths or directories to include in the zip file.
        zip_file_path (str): The destination path for the zip file.
        logger (logging.Logger): Logger instance to log progress and errors.

    Returns:
        None
    """
    try:
        with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for item in files_to_zip:
                if os.path.isdir(item):
                    logger.info(f"Adding directory to zip: {item}")
                    for foldername, subfolders, filenames in os.walk(item):
                        for filename in filenames:
                            # Create complete path to file in folder
                            file_path = os.path.join(foldername, filename)
                            # Archive name keeps the folder structure relative to the folder being zipped
                            archive_name = os.path.relpath(
                                file_path, os.path.dirname(item)
                            )
                            zipf.write(file_path, archive_name)
                            logger.info(f"Added file to zip: {archive_name}")
                elif os.path.isfile(item):
                    logger.info(f"Adding file to zip: {item}")
                    archive_name = os.path.basename(item)
                    zipf.write(item, archive_name)
                    logger.info(f"Added file to zip: {archive_name}")
                else:
                    logger.info(f"Item does not exist, skipping: {item}")
    except Exception as e:
        logger.error(f"Error while creating zip file: {str(e)}")


def download_required_tools(logger, artifacts_list):
    """
    Download all tools required by the specified artifacts and set serve_locally=TRUE.
    This ensures the offline collector will include all necessary tools.

    Args:
        logger: Logger instance
        artifacts_list: List of artifact names that will be collected
    """
    logger.info(f"Checking tools required for artifacts: {artifacts_list}")

    try:
        # Get all tools required by the selected artifacts (with name, url, and version)
        # Query each artifact individually to get its tools
        required_tools = {}
        for artifact_name in artifacts_list:
            tools_query = f"""
            SELECT * FROM foreach(
                row={{SELECT tools FROM artifact_definitions(deps=TRUE, names=["{artifact_name}"]) WHERE tools}},
                query={{SELECT * FROM foreach(row=tools, query={{
                    SELECT name, url, version FROM scope() WHERE name
                }})}}
            )
            """
            try:
                tools_result = run_generic_vql(tools_query, logger)
                for tool in tools_result:
                    name = tool.get('name', '')
                    url = tool.get('url', '')
                    version = tool.get('version', '')
                    if name:
                        required_tools[name] = {
                            'url': url if url and not url.startswith('todo') else None,
                            'version': version if version else 'latest'
                        }
            except Exception as e:
                logger.debug(f"No tools found for artifact {artifact_name}: {e}")
                continue

        logger.info(f"Found {len(required_tools)} required tools: {list(required_tools.keys())}")

        # Check which tools need to be downloaded
        for tool_name, tool_data in required_tools.items():
            artifact_url = tool_data['url']
            version = tool_data['version']

            # Check inventory for current download status and URL
            check_query = f"""
            SELECT name, url, serve_locally, serve_url, filestore_path
            FROM inventory()
            WHERE name = '{tool_name}'
            """
            tool_info = run_generic_vql(check_query, logger)

            inventory_url = None
            if tool_info:
                tool = tool_info[0]
                serve_locally = tool.get('serve_locally', False)
                serve_url = tool.get('serve_url', '')
                filestore_path = tool.get('filestore_path', '')
                inventory_url = tool.get('url', '')

                # Skip if already served locally with valid filestore
                if serve_locally and '/public/' in str(serve_url) and filestore_path:
                    logger.info(f"Tool '{tool_name}' already available locally, skipping")
                    continue

            # Use URL from artifact definition first, fall back to inventory
            url = artifact_url or inventory_url
            if not url or url.startswith('todo'):
                logger.warning(f"Tool '{tool_name}' has no valid URL in artifact or inventory, skipping")
                continue

            # Extract filename from URL for proper metadata
            filename = url.split('/')[-1] if '/' in url else tool_name

            # Download the tool
            logger.info(f"Downloading tool: {tool_name} from {url[:60]}...")
            download_query = f'''
            LET download <= SELECT Content FROM http_client(
                url="{url}",
                tempfile_extension=".tmp"
            )
            SELECT inventory_add(
                tool="{tool_name}",
                url="{url}",
                filename="{filename}",
                version="{version}",
                file=download[0].Content,
                serve_locally=TRUE
            ) AS result
            FROM scope()
            '''

            try:
                result = run_generic_vql(download_query, logger)
                if result and result[0].get('result'):
                    logger.info(f"Successfully downloaded tool: {tool_name}")
                else:
                    logger.warning(f"Failed to download tool: {tool_name}")
            except Exception as e:
                logger.error(f"Error downloading tool '{tool_name}': {str(e)}")
                continue

            # Small delay between downloads
            time.sleep(1)

        logger.info("Tool download check complete")

    except Exception as e:
        logger.error(f"Error in download_required_tools: {str(e)}")
        logger.error(traceback.format_exc())


def run_server_artifact(logger, config_data, config_agent):
    logger.info(
        "Running server artifact query. "
        + str(config_data)
        + " Agent LiNKS: "
        + str(config_agent)
    )
    try:
        artifacts_dict = {"Server.Utils.CreateCollector": {"opt_format": "csv"}}
        artifactsListArr = ["Generic.Client.Info"]
        artifactsParmObj = {}
        for obj in config_data["Artifacts"]:
            if obj.get("parameters"):  # Non-empty dicts evaluate to True
                # Transform KAPE/Triage parameters for Velociraptor 0.75 compatibility
                if obj["name"] in ["Windows.KapeFiles.Targets", "Windows.Triage.Targets"]:
                    logger.info(f"Transforming {obj['name']} parameters for Velociraptor 0.75")
                    logger.info(f"Old parameters: {obj['parameters']}")
                    transformed_params = transform_kape_parameters(obj["parameters"])
                    logger.info(f"New parameters: {transformed_params}")
                    artifactsParmObj[obj["name"]] = transformed_params
                else:
                    artifactsParmObj[obj["name"]] = obj["parameters"]
            artifactsListArr.append(obj["name"])
        artifacts_dict["Server.Utils.CreateCollector"]["OS"] = "Generic"
        artifacts_dict["Server.Utils.CreateCollector"]["opt_collector_filename"] = (
            config_data["Configuration"]["CollectorFileName"]
        )
        artifacts_dict["Server.Utils.CreateCollector"]["opt_filename_template"] = (
            config_data["Configuration"]["OutputsFileName"]
            + "-r___r-%FQDN%-%TIMESTAMP%"
        )
        artifacts_dict["Server.Utils.CreateCollector"]["artifacts"] = artifactsListArr
        artifacts_dict["Server.Utils.CreateCollector"]["parameters"] = artifactsParmObj
        artifacts_dict["Server.Utils.CreateCollector"]["opt_cpu_limit"] = config_data[
            "Resources"
        ]["CpuLimit"]
        artifacts_dict["Server.Utils.CreateCollector"]["opt_progress_timeout"] = (
            config_data["Resources"]["MaxIdleTimeInSeconds"]
        )
        artifacts_dict["Server.Utils.CreateCollector"]["opt_timeout"] = config_data[
            "Resources"
        ]["MaxExecutionTimeInSeconds"]

        # Download all required tools before creating the collector
        # This ensures tools are available locally and will be bundled in the collector
        logger.info("Downloading required tools for offline collector...")
        download_required_tools(logger, artifactsListArr)

        FlowId = modules.Velociraptor.VelociraptorScript.run_server_artifact(
            "Server.Utils.CreateCollector", logger, artifacts_dict
        )
        # time.sleep(23)
        logger.info(f"Time Before {FlowId}: " + time.ctime())
        Collector_query = f"""
     LET _ <= SELECT * FROM watch_monitoring(artifact='System.Flow.Completion')
                            WHERE FlowId = "{FlowId}"
                            LIMIT 1
"""
        logger.info(Collector_query)
        Collector_results = run_generic_vql(Collector_query, logger)
        logger.info("Time after: " + time.ctime())
        time.sleep(5)

        logger.info("This is the Collector_results: " + str(Collector_results))
        random_string = "".join(random.choices(string.ascii_letters, k=11))
        os.makedirs(f"Collector/{random_string}", exist_ok=True)
        OsCollector = ""
        OsCollectorPath = ""
        BatchFile = ""
        shell_script_content = ""
        logger.info(f"Log FlowId : {FlowId}")
        collectorPath = f'clients/server/collections/{FlowId}/uploads/scope/{config_data["Configuration"]["CollectorFileName"]}'
        channel = modules.Velociraptor.VelociraptorScript.setup_connection(logger)
        stub = api_pb2_grpc.APIStub(channel)
        offset = 0
        NewVeloCollector = f'Collector/{random_string}/{config_data["Configuration"]["CollectorFileName"]}'  # Open the output file in binary write mode
        with open(NewVeloCollector, "wb") as output_file:
            while True:
                # Prepare the request
                request = api_pb2.VFSFileBuffer(
                    components=collectorPath.split("/"),
                    length=1024,  # Adjust buffer size as needed
                    offset=offset,
                )

                # Send the request and receive the response
                res = stub.VFSGetBuffer(request)
                if len(res.data) == 0:
                    break

                # Write data to the file
                output_file.write(res.data)
                offset += len(res.data)

        # TestPathVelo = "Collector/"
        SplitScript = ""
        # TestPathVelo = os.path.abspath(os.path.expanduser(TestPathVelo))

        match sys.argv[2]:

            case "Windows":
                OsCollector = "velociraptor_client.exe"
                # SplitScript = "modules/Collector/PowerShellSplit.ps1"
                SplitScript = "modules/Collector/7-ZipPortable"
                OsCollectorPath = "velociraptor_client.exe"

                BatchFile = f'Collector/{random_string}/{config_data["Configuration"]["CollectorFileName"]}.bat'
                # shell_script_content = f"""
                # @echo off

                # :: Define the folder where the files are generated
                # set "folderPath=%cd%"

                # :: Run Velociraptor command to generate the files
                # {OsCollector} -- --embedded_config {config_data["Configuration"]["CollectorFileName"]}
                # :: Find the most recent file matching the pattern
                # for /f "delims=" %%F in ('dir /b /od "%folderPath%\{config_data["Configuration"]["CollectorFileName"]}-r___r-*"') do set "latestFile=%%F"

                # :: Validate that a file was found
                # if not defined latestFile (
                #     echo No file matching the pattern "{config_data["Configuration"]["CollectorFileName"]}-r___r-*" was found.
                #     exit /b 1
                # )

                # :: Log the file being processed
                # echo Most recent file: %latestFile%

                # :: Run the PowerShell script on the most recent file
                # powershell -NoProfile -Command "&  ".\PowerShellSplit.ps1 -filePath '%folderPath%\%latestFile%' -outputFolder '%folderPath%' -chunkSizeMB 250" "

                # :: Exit the batch script
                # exit /b

                # """
                shell_script_content = f"""
                @echo off

                :: Define the folder where the files are generated
             	set "folderPath=%~dp0"
	        	echo folderPath is set to: %folderPath%

	        	:: Change to the working directory
	        	cd /d "%folderPath%"

                :: Run Velociraptor command to generate the files
                {OsCollector} -- --embedded_config {config_data["Configuration"]["CollectorFileName"]}        
                
                 :: Find the most recent file matching the pattern
                 for /f "delims=" %%F in ('dir /b /od "%folderPath%\{config_data["Configuration"]["OutputsFileName"]}-r___r-*.zip"') do set "latestFile=%%F"

                 :: Validate that a file was found
                 if not defined latestFile (
                     echo No file matching the pattern "{config_data["Configuration"]["OutputsFileName"]}-r___r-*" was found.
                     exit /b 1
                 )


                :: Extract filename without extension for the archive name
                for %%I in ("%latestFile%") do set "latestFileWithoutExtension=%%~nI"
                

                :: Run the PowerShell script on the most recent file
                "7-ZipPortable/App/7-Zip/7z.exe" a -t7z -v{config_data["Configuration"]["ZipSplitSizeInMb"]}m "Results\%latestFileWithoutExtension%.7z" %latestFile% > txt.txt

                :: Remove the Original Result file after creating the archive Split for Clarity proposes
                del "%folderPath%\%latestFile%"

                """

            case "Mac":
                OsCollector = "velociraptor_client"
                SplitScript = "modules/Collector/split_and_hash.sh"
                OsCollectorPath = "velociraptor_client"

                BatchFile = f'Collector/{random_string}/{config_data["Configuration"]["CollectorFileName"]}.sh'
                shell_script_content = f"""
                folderPath=$(pwd)
                {OsCollector} -- --embedded_config {config_data["Configuration"]["CollectorFileName"]}
                :: Find the most recent file matching the pattern
                latestFile=$(ls -t "$folderPath"/{config_data["Configuration"]["OutputsFileName"]}-r___r-*.zip 2>/dev/null | head -n 1)

                # Validate that a file was found
                if [ -z "$latestFile" ]; then
                    echo "No file matching the pattern '{config_data["Configuration"]["OutputsFileName"]}-r___r-*' was found."
                    exit 1
                fi

                :: Log the file being processed
                echo "Most recent file: $latestFile"

                :: Run another shell script on the most recent file
                ./split_and_hash.sh "$latestFile" {config_data["Configuration"]["ZipSplitSizeInMb"]}M
                """
            case "Linux":
                OsCollector = "velociraptor_client"
                SplitScript = "modules/Collector/split_and_hash.sh"
                OsCollectorPath = "velociraptor_client"

                BatchFile = f'Collector/{random_string}/{config_data["Configuration"]["CollectorFileName"]}.sh'
                shell_script_content = f"""
                folderPath=$(pwd)
                {OsCollector} -- --embedded_config {config_data["Configuration"]["CollectorFileName"]}
                :: Find the most recent file matching the pattern
                latestFile=$(ls -t "$folderPath"/{config_data["Configuration"]["OutputsFileName"]}-r___r-*.zip 2>/dev/null | head -n 1)

                :: Validate that a file was found
                if [ -z "$latestFile" ]; then
                    echo "No file matching the pattern '{config_data["Configuration"]["OutputsFileName"]}-r___r-*' was found."
                    exit 1
                fi

                :: Log the file being processed
                echo "Most recent file: $latestFile"

                :: Run another shell script on the most recent file
                ./split_and_hash.sh "$latestFile" {config_data["Configuration"]["ZipSplitSizeInMb"]}M
                """

        logger.info("step 1 complete")
        OsCollectorPath = os.path.join(
            os.path.dirname(config_agent[sys.argv[2]]), OsCollectorPath
        )
        OsCollectorPath = os.path.abspath(os.path.expanduser(OsCollectorPath))
        logger.info(f"OsCollectorPath : {OsCollectorPath}")

        # Write the content to the shell script file
        with open(BatchFile, "w") as file:
            file.write(shell_script_content)
        time.sleep(1)
        logger.info("22222222222222222222")

        time.sleep(1)

        logger.info("66666666666666")

        # Make the shell script executable
        os.chmod(BatchFile, 0o755)

        files_to_zip = [BatchFile, OsCollectorPath, NewVeloCollector, SplitScript]
        zip_file_path = f'Collector/{random_string}/{config_data["Configuration"]["CollectorFileName"]}.zip'
        create_zip(files_to_zip, zip_file_path, logger)
        logger.info("cut " + zip_file_path)

    except Exception as e:
        logger.error(str(e))


if __name__ == "__main__":
    logger = additionals.funcs.setup_logger("Collector.log")

    # Load environment configuration
    env_dict = additionals.funcs.read_env_file(logger)
    config_data = connect_my_sql(env_dict, logger)

    # Run the server artifact
    run_server_artifact(logger, config_data[0], config_data[1])
