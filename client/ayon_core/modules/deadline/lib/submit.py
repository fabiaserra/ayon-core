import os
import requests
import re
import getpass
import json

from ayon_core.lib import Logger
from ayon_core.lib import is_running_from_build

from ayon_core.modules.deadline import constants


logger = Logger.get_logger(__name__)

# Default Deadline job
DEFAULT_PRIORITY = 50
DEFAULT_CHUNK_SIZE = 9999
DEAFAULT_CONCURRENT_TASKS = 1


def payload_submit(
    plugin,
    plugin_data,
    batch_name,
    task_name,
    group="",
    comment="",
    priority=DEFAULT_PRIORITY,
    chunk_size=DEFAULT_CHUNK_SIZE,
    concurrent_tasks=DEAFAULT_CONCURRENT_TASKS,
    frame_range=None,
    department="",
    extra_env=None,
    job_dependencies=None,
):
    if not job_dependencies:
        job_dependencies = []

    frames = "0" if not frame_range else f"{frame_range[0]}-{frame_range[1]}"

    payload = {
        "JobInfo": {
            # Top-level group name
            "BatchName": batch_name,
            # Job name, as seen in Monitor
            "Name": task_name,
            # Arbitrary username, for visualisation in Monitor
            "UserName": getpass.getuser(),
            "Priority": priority,
            "ChunkSize": chunk_size,
            "ConcurrentTasks": concurrent_tasks,
            "Department": department,
            "Pool": "",
            "SecondaryPool": "",
            "Group": group,
            "Plugin": plugin,
            "Frames": frames,
            "Comment": comment or "",
            # Optional, enable double-click to preview rendered
            # frames from Deadline Monitor
            # "OutputFilename0": preview_fname(render_path).replace("\\", "/"),
        },
        "PluginInfo": plugin_data,
        # Mandatory for Deadline, may be empty
        "AuxFiles": [],
    }

    # Add 'nuke' limit to control license count
    if "Nuke" in plugin:
        payload["JobInfo"]["LimitGroups"] = "nuke"

    # Set job dependencies if they exist
    for index, job in enumerate(job_dependencies):
        payload["JobInfo"][f"JobDependency{index}"] = job["_id"]

    # Include critical environment variables with submission
    keys = [
        "AYON_FOLDER_PATH",
        "AYON_TASK_NAME",
        "AYON_PROJECT_NAME",
        "AYON_APP_NAME",
        "OCIO",
        "USER",
        "AYON_SG_USERNAME",
    ]

    environment = dict(
        {key: os.environ[key] for key in keys if key in os.environ},
    )
    if extra_env:
        environment.update(extra_env)

    payload["JobInfo"].update(
        {
            "EnvironmentKeyValue%d"
            % index: "{key}={value}".format(
                key=key, value=environment[key]
            )
            for index, key in enumerate(environment)
        }
    )

    plugin = payload["JobInfo"]["Plugin"]
    logger.debug("using render plugin : {}".format(plugin))

    logger.debug("Submitting..")
    logger.debug(json.dumps(payload, indent=4, sort_keys=True))

    url = "{}/api/jobs".format(constants.DEADLINE_URL)
    response = requests.post(url, json=payload, timeout=10)

    if not response.ok:
        raise Exception(response.text)

    return response.json()


def preview_fname(path):
    """Return output file path with #### for padding.

    Deadline requires the path to be formatted with # in place of numbers.
    For example `/path/to/render.####.png`

    Args:
        path (str): path to rendered images

    Returns:
        str

    """
    logger.debug("_ path: `{}`".format(path))
    if "%" in path:
        hashes_path = re.sub(
            r"%(\d*)d", lambda m: "#" * int(m.group(1)) if m.group(1) else "#", path
        )
        return hashes_path

    if "#" in path:
        logger.debug("_ path: `{}`".format(path))

    return path
