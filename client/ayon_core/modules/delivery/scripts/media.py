
"""Module for handling generation of delivery media of SG playlists and versions"""
import os
import collections
import copy
import re
import click
import csv

import ayon_api

from ayon_core.lib import Logger, StringTemplate, get_datetime_data, path_tools
from ayon_core.pipeline import delivery, template_data
from ayon_core.modules.deadline import constants as dl_constants
from ayon_core.modules.deadline.lib import submit

from ayon_shotgrid.lib import credentials


# Default paths where template nuke script and corresponding python file
# that operate the template script live
NUKE_DELIVERY_PY_DEFAULT = "/pipe/nuke/templates/delivery_template.py"
NUKE_DELIVERY_SCRIPT_DEFAULT = "/pipe/nuke/templates/delivery_template.nk"

# Path where Nuke template script would live for a project
PROJ_NUKE_DELIVERY_SCRIPT = "/proj/{proj_code}/resources/delivery/delivery_template.nk"

# Root path where delivery media gets generated
DELIVERY_STAGING_DIR = "/proj/{project[code]}/io/delivery/ready_to_deliver/{yyyy}{mm}{dd}"

# Template string of how delivery filenames will be called
FILENAME_TEMPLATE_DEFAULT = "{SEQ}_{shotnum}_{task[short]}_v{version:0>4}_{vendor}<_{suffix}>"

# Template path where outputs will be generated to
DELIVERY_TEMPLATE_DEFAULT = "{package_name}/{output}/<{is_sequence}<{filename}/>>{filename}<.{frame:0>4}>.{ext}"

# Constant string to identify the values that we want to bypass and not override
USE_SOURCE_VALUE = "-- Use source --"

# Shotgrid names of fields that are relevant for delivery media
SG_FIELD_OP_INSTANCE_ID = "sg_op_instance_id"
SG_FIELD_MEDIA_GENERATED = "sg_op_delivery_media_generated"
SG_FIELD_MEDIA_PATH = "sg_op_delivery_media_path"
SG_SUBMISSION_NOTES = "sg_submission_notes"
SG_SUBMIT_FOR = "sg_submit_for"
SG_VERSION_IMPORTANT_FIELDS = [
    "project",
    "code",
    "entity",
    "description",
    "user",
    SG_FIELD_OP_INSTANCE_ID,
    SG_FIELD_MEDIA_GENERATED,
    SG_FIELD_MEDIA_PATH,
    SG_SUBMIT_FOR,
    SG_SUBMISSION_NOTES
]

# Regular expression pattern to match word[word]
NESTED_TOKENS_RE = re.compile(r"(\w+)\[(\w+)\]")

# All file extensions that will (most likely) be a single file
SINGLE_FILE_EXTENSIONS = ["mov", "mp4", "png", "jpg", "jpeg", "mxf"]

# Columns for CSV data file
CSV_DATA_COLUMNS = ["Filename", "Submitted For", "Notes"]


logger = Logger.get_logger(__name__)


def get_output_anatomy_data(anatomy_data, delivery_data, output_name, output_extension):
    """Returns a dictionary of anatomy data for a given output name and extension.

    Args:
        anatomy_data (dict): A dictionary of base generic anatomy data to use
            as a base for the tokens.
        delivery_data (dict): A dictionary of delivery data that contains
            data specific to the output and possible overrides of the tokens.
        output_name (str): The name of the output.
        output_extension (str): The extension of the output.

    Returns:
        dict: A dictionary of anatomy data for the output.
    """
    output_anatomy_data = copy.deepcopy(anatomy_data)

    # Specific tokens for output

    # Add output name
    output_anatomy_data["output"] = output_name

    # Add output extension
    output_anatomy_data["ext"] = output_extension

    # If output extension is one of the single file extensions we remove the
    # "frame" token
    if output_extension in SINGLE_FILE_EXTENSIONS:
        try:
            output_anatomy_data.pop("frame")
        except KeyError:
            pass
    # Otherwise we add "is_sequence" as an empty token so we can use it on
    # nested optional tokens to add extra items
    # i.e., "<{is_sequence}<{filename}/>>" will only add that extra folder
    # if the output_anatomy_data contains "is_sequence"
    else:
        output_anatomy_data["is_sequence"] = ""

    # Add delivery type
    output_anatomy_data["delivery_type"] = output_name.rsplit("_", 1)[-1]

    # Create a dictionary of all the tokens we will override
    # anatomy data with
    for key, value in delivery_data.items():

        # Create a new dictionary on every iteration as we progressively
        # update the delivery data with each new override
        output_override = {}

        if key.endswith("_override") and value and value != USE_SOURCE_VALUE:
            # Remove the _override suffix
            key = key.replace("_override", "")

            # Fill up values of tokens that might be referencing other tokens
            value = StringTemplate.format_template(value, output_anatomy_data)
            logger.debug("Updated value '%s'", value)

            # Check if key is a nested key (i.e., "task[code]_override")
            # so if it's nested we create an inner dictionary with the
            # value
            nested_tokens_match = NESTED_TOKENS_RE.match(key)
            if nested_tokens_match:
                outer_key, inner_key = nested_tokens_match.groups()
                output_override[outer_key] = {inner_key: value}
            # Otherwise we simply assign the value to the key
            else:
                output_override[key] = value

        # Add custom tokens
        elif key == "custom_tokens":
            # Create a dictionary with all the overrides specific to the output name
            for custom_key, custom_value in value.items():
                if custom_key.startswith(output_name) and custom_value:
                    custom_key = custom_key.replace(f"{output_name}:", "")
                # Ignore keys that aren't specific to the output
                elif ":" in custom_key:
                    logger.debug(
                        "Skipping custom token with key '%s' as it's not specific to output '%s'.",
                        custom_key,
                        output_name
                    )
                    continue
                logger.debug(
                    "Adding custom token '%s':'%s' for output '%s'.",
                    custom_key,
                    custom_value,
                    output_name
                )
                custom_value = StringTemplate.format_template(
                    custom_value, output_anatomy_data
                )
                output_override[custom_key] = custom_value

        output_anatomy_data.update(output_override)

    return output_anatomy_data


def generate_delivery_media_playlist_id(
    playlist_id,
    delivery_data,
):
    """Given a SG playlist id, generate all the delivery media for all the versions
    associated to it.

    Args:
        playlist_id (int): Shotgrid playlist id to republish.
        delivery_data (dict[str]): Dictionary of relevant data necessary
            for delivery.

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the republish was successful.
    """
    report_items = collections.defaultdict(list)

    sg = credentials.get_shotgrid_session()

    sg_playlist = sg.find_one(
        "Playlist",
        [
            ["id", "is", int(playlist_id)],
        ],
        ["project"],
    )

    # Get the project name associated with the selected entities
    project_name = sg_playlist["project"]["name"]

    project_doc = ayon_api.get_project(project_name, fields=["name"])
    if not project_doc:
        return report_items[f"Didn't find project '{project_name}' in avalon."], False

    # Get all the SG versions associated to the playlist
    sg_versions = sg.find(
        "Version",
        [["playlists", "in", sg_playlist]],
        SG_VERSION_IMPORTANT_FIELDS,
    )

    success = True
    for sg_version in sg_versions:
        new_report_items, new_success = generate_delivery_media_version(
            sg_version,
            project_name,
            delivery_data,
            report_items,
        )
        if new_report_items:
            report_items.update(new_report_items)

        if not new_success:
            success = False

    click.echo(report_items)
    return report_items, success


def generate_delivery_media_version_id(
    version_id,
    delivery_data,
):
    """Given a SG version id, generate its corresponding delivery so it
        triggers the OP publish pipeline again.

    Args:
        version_id (int): Shotgrid version id to republish.
        delivery_data (dict[str]): Dictionary of relevant data necessary
            for delivery.

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the republish was successful.
    """
    report_items = collections.defaultdict(list)

    sg = credentials.get_shotgrid_session()

    sg_version = sg.find_one(
        "Version",
        [
            ["id", "is", int(version_id)],
        ],
        SG_VERSION_IMPORTANT_FIELDS,
    )
    return generate_delivery_media_version(
        sg_version,
        sg_version["project"]["name"],
        delivery_data,
        report_items
    )


def generate_delivery_media_version(
    sg_version,
    project_name,
    delivery_data,
    report_items,
    update_sg_data=True,
):
    """
    Generate the corresponding delivery version given SG version by creating a new
        product with review and/or final outputs.

    Args:
        sg_version (dict): The Shotgrid version to republish.
        project_name (str): The name of the Shotgrid project.
        delivery_data (dict[str]): Dictionary of relevant data necessary
            for delivery.

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the republish was successful.
    """
    logger.debug("Delivery data: %s", delivery_data)

    # Grab the OP's id corresponding to the SG version
    op_version_id = sg_version["sg_op_instance_id"]
    if not op_version_id or op_version_id == "-":
        msg = "Missing 'sg_op_instance_id' field on SG Versions"
        sub_msg = f"{project_name} - {sg_version['code']} - id: {sg_version['id']}<br>"
        logger.error("%s: %s", msg, sub_msg)
        report_items[msg].append(sub_msg)
        return report_items, False

    # Get OP version corresponding to the SG version
    version_doc = ayon_api.get_version_by_id(project_name, op_version_id)
    if not version_doc:
        msg = "No OP version found for SG versions"
        sub_msg = f"{project_name} - {sg_version['code']} - id: {sg_version['id']}<br>"
        logger.error("%s: %s", msg, sub_msg)
        report_items[msg].append(sub_msg)
        return report_items, False

    # Find the "exr" OP representation we want to deliver
    exr_repre_doc = ayon_api.get_representation_by_name(
        project_name,
        "exr",
        version_id=op_version_id,
    )
    if not exr_repre_doc:
        msg = "No 'exr' representation found on SG versions"
        sub_msg = f"{project_name} - {sg_version['code']} - id: {sg_version['id']}<br>"
        logger.error("%s: %s", msg, sub_msg)
        report_items[msg].append(sub_msg)
        return report_items, False

    # If force delivery media isn't enabled, validate whether the SG version was already
    # submitted for delivery or not
    if not delivery_data.get("force_delivery_media"):
        if sg_version.get(SG_FIELD_MEDIA_GENERATED):
            report_items["Delivery media already exists for versions"].append(
                f"{project_name} - {sg_version['code']} - id: {sg_version['id']} - {sg_version[SG_FIELD_MEDIA_PATH]}"
            )
            return report_items, False

    # Grab frame range from version being delivered
    frame_start_handle = int(
        version_doc["data"]["frameStart"] - version_doc["data"].get("handleStart", 0)
    )
    frame_end_handle = int(
        version_doc["data"]["frameEnd"] + version_doc["data"].get("handleEnd", 0)
    )
    logger.debug("Frame start handle: %s", frame_start_handle)
    logger.debug("Frame end handle: %s", frame_end_handle)
    out_frame_start = frame_start_handle
    out_frame_end = frame_end_handle

    # Try find the thumbnail representation of the OP version
    # and add it to the delivery template in case it comes
    # useful in the future
    thumbnail_repre_doc = ayon_api.get_representation_by_name(
        project_name,
        "thumbnail",
        version_id=op_version_id,
    )

    # Calculate the input path where the "exr" representation
    # lives
    input_path = exr_repre_doc["data"]["path"]
    # Replace frame number with #'s for expected_files function
    path, filename = os.path.split(input_path)
    new_filename = path_tools.replace_frame_number_with_token(filename, "#", padding=True)
    input_hashes_path = os.path.join(path, new_filename)

    # Create a dictionary of anatomy data so we can fill up
    # all the tokenized paths
    anatomy_data = copy.deepcopy(exr_repre_doc["context"])
    datetime_data = get_datetime_data()
    anatomy_data.update(datetime_data)
    asset_data = template_data.get_template_data_with_names(
        project_name, exr_repre_doc["context"]["asset"]
    )
    anatomy_data.update(asset_data)

    # Add {submission_notes} from SG version
    anatomy_data["submission_notes"] = sg_version[SG_SUBMISSION_NOTES]

    # Add {submit_for} from SG version
    anatomy_data["submit_for"] = sg_version[SG_SUBMIT_FOR]

    logger.debug("Original anatomy data: %s", anatomy_data)

    # Create path where delivery package will be created
    package_name = ""
    package_path = StringTemplate.format_template(
        DELIVERY_STAGING_DIR, anatomy_data
    )

    # Create environment variables required to run Nuke script
    task_env = {
        "_AX_DELIVERY_NUKESCRIPT": delivery_data["nuke_template_script"],
        "_AX_DELIVERY_READPATH": input_hashes_path,
        "_AX_DELIVERY_FRAMES": "{0}_{1}".format(
            int(out_frame_start), int(out_frame_end)
        ),
        "_AX_DELIVERY_SUBMISSION_NOTES": delivery_data.get("submission_notes_override") or
            anatomy_data.get("submission_notes"),
        "_AX_DELIVERY_SUBMIT_FOR": delivery_data.get("submit_for_override") or
            anatomy_data.get("submit_for"),
        "_AX_DELIVERY_ARTIST": sg_version.get("user", {}).get("name") or
            anatomy_data.get("user"),
        "_AX_DEBUG_PATH": os.path.join(package_path, "nuke_scripts"),
        "AYON_PROJECT_NAME": project_name,
        "AYON_FOLDER_PATH": anatomy_data["folderPath"],
        "AYON_TASK_NAME": anatomy_data["task"]["name"],
        "AYON_APP_NAME": "nuke/15.0",
        "AYON_RENDER_JOB":  "1",
        "AYON_BUNDLE_NAME": os.getenv("AYON_BUNDLE_NAME")
    }

    if thumbnail_repre_doc:
        task_env["_AX_DELIVERY_THUMBNAIL_PATH"] = thumbnail_repre_doc["data"]["path"]

    # Create a list of all the outputs that will be generated
    # to store them in a CSV file
    csv_data = []

    success = True

    # For each output selected, submit a job to the farm
    for index, output_name_ext in enumerate(delivery_data["output_names_ext"]):

        # Inject output specific anatomy data and resolve tokens
        output_name, output_ext = output_name_ext
        output_anatomy_data = get_output_anatomy_data(
            anatomy_data, delivery_data, output_name, output_ext
        )
        package_name = output_anatomy_data["package_name"]
        logger.debug(
            "Anatomy data with output '%s' overrides: %s",
            output_name, output_anatomy_data
        )

        # Calculate destination path where output will be generated
        output_path_template = os.path.join(
            package_path, delivery_data["template_path"]
        )
        repre_report_items, dest_path = delivery.check_destination_path(
            output_name,
            None,
            output_anatomy_data,
            datetime_data,
            template_name=None,
            template_str=output_path_template,
            return_dest_path=True,
        )
        if repre_report_items:
            success = False
            report_items.update(repre_report_items)
            continue

        # Ensure output directory exists
        parent_dir = os.path.dirname(dest_path)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        # Add some validation to make sure we don't overwrite existing files
        elif not delivery_data.get("force_override_files") and os.path.isfile(dest_path):
            logger.warning("Destination path '%s' already exists.", dest_path)
            report_items["Destination path already exists"].append(
                dest_path
            )
            return report_items, False

        out_filename = output_anatomy_data["filename"]

        # Create a separate variable as CSV filename is the full name with
        # extension included
        csv_outfilename = f"{out_filename}.{output_ext}"

        # If {frame} token exists, replace frame with padded #'s
        if output_anatomy_data.get("frame"):
            dest_path = re.sub(
                r"\d+(?=\.\w+$)", lambda m: "#" * len(m.group()) if m.group() else "#",
                dest_path
            )
            # Add frame range to output filename used for CSV data too
            slate_frame_start = out_frame_start - 1
            csv_outfilename = f"{out_filename}.[{slate_frame_start}-{out_frame_end}].{output_ext}"

        # Add environment variables specific to this output
        output_task_env = task_env.copy()
        output_task_env["_AX_DELIVERY_OUTPUT_NAME"] = output_name
        output_task_env["_AX_DELIVERY_FILENAME"] = out_filename
        output_task_env["_AX_DELIVERY_WRITEPATH"] = dest_path

        # Trigger generation of thumbnail only on the first output generation
        if index == 0:
            output_task_env["_AX_DELIVERY_GENERATE_THUMBNAIL"] = "1"
            output_task_env["_AX_DELIVERY_THUMBNAIL_PATH"] = os.path.join(
                package_path, package_name, "_thumb"
            )

        # Append output information to CSV data
        csv_data.append(
            [
                csv_outfilename,
                sg_version.get(SG_SUBMIT_FOR, ""),
                sg_version.get(SG_SUBMISSION_NOTES, "")
            ]
        )

        logger.info("Submitting Nuke delivery job for '%s'...", output_name)

        # Create dictionary of data specific to Nuke plugin for payload submit
        plugin_data = {
            "ScriptJob": True,
            "SceneFile": NUKE_DELIVERY_PY_DEFAULT,
            "ScriptFilename": NUKE_DELIVERY_PY_DEFAULT,
            # the Version entry is kind of irrelevant as our Deadline workers only
            # contain a single DCC version at the time of writing this
            "Version": "15.0",
            "UseGpu": False,
            "OutputFilePath": dest_path,
        }

        # Submit job to Deadline
        task_name = "Delivery - {} - {} - {} ({})".format(
            output_name,
            output_anatomy_data["filename"],
            project_name,
            asset_data["project"]["code"]
        )
        response = submit.payload_submit(
            plugin="AxNuke",
            plugin_data=plugin_data,
            frame_range=(out_frame_start, out_frame_end),
            batch_name=f"Delivery media - {package_path}",
            task_name=task_name,
            group=dl_constants.NUKE_CPU_GROUP.format("15", "0"),
            extra_env=output_task_env,
        )
        report_items["Submitted delivery media job to Deadline"].append(
            f"{dest_path} - {task_name} - {response['_id']}"
        )

    # Update SG version with the path where it got delivered and
    # whether media got generated
    if update_sg_data:
        data_to_update = {
            SG_FIELD_MEDIA_GENERATED: True,
            SG_FIELD_MEDIA_PATH: os.path.join(package_path, package_name),
        }
        sg = credentials.get_shotgrid_session()
        sg.update("Version", sg_version["id"], data_to_update)
        logger.debug(
            "Updating version '%s' with '%s'", sg_version["code"], data_to_update
        )

    # Write CSV data to file in package
    csv_path = os.path.join(
        package_path, package_name, "{}.csv".format(package_name)
    )

    # Make sure output directory folder exists
    output_dir = os.path.dirname(csv_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Check if the file exists and has content (i.e., is not empty)
    csv_file_exists = os.path.isfile(csv_path) and os.path.getsize(csv_path) > 0

    with open(csv_path, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)

        # If file didn't exist or was empty, write the header first
        if not csv_file_exists:
            logger.debug("CSV file created at '%s'", csv_path)
            writer.writerow(CSV_DATA_COLUMNS)

        for row in csv_data:
            writer.writerow(row)

    logger.debug("Added CSV data at '%s'", csv_path)

    return report_items, success
