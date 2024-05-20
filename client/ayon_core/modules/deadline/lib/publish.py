import os
import getpass
import json

import ayon_api

from ayon_core.lib import Logger, path_tools, run_subprocess
from ayon_core.pipeline import Anatomy
from ayon_core.pipeline.template_data import get_template_data
from ayon_core.modules.deadline import constants as dl_constants
from ayon_core.modules.deadline.lib import submit
from ayon_core.modules.delivery.scripts import utils, review

from ayon_shotgrid.lib import credentials
from ayon_shotgrid.scripts import populate_tasks


logger = Logger.get_logger(__name__)


REVIEW_FAMILIES = {
    "render",
    "reference",
    "plate",
    "review"
}

PUBLISH_TO_SG_FAMILIES = {
    "render",
    "review",
    "reference",
}

IGNORE_LUT_FAMILIES = {
    "reference",
    "review",
}

TASKS_TO_IGNORE_REVIEW = {}


def check_version_exists(project_name, folder_entity, product_name, version):
    """Check whether version document exists in database."""

    product_entity = ayon_api.get_product_by_name(
        project_name, product_name, folder_entity["id"]
    )
    if not product_entity:
        return False

    existing_version_doc = ayon_api.get_version_by_name(
        project_name, version, product_entity["id"]
    )

    # Check if version already exists
    if existing_version_doc:
        return True

    return False


def check_task_exists(project_name, folder_entity, task_name, force_creation=False):
    """Check whether version document exists in database."""
    if force_creation:
        logger.debug("Creating task '%s' in asset '%s'", task_name, folder_entity["name"])
        sg = credentials.get_shotgrid_session()
        sg_project = sg.find_one("Project", [["name", "is", project_name]], ["code"])
        sg_entity_type = folder_entity["folderType"]
        sg_entity = sg.find_one(sg_entity_type, [["id", "is", int(folder_entity["attrib"]["shotgridId"])]], ["code"])
        if not sg_entity:
            return False
        populate_tasks.add_tasks_to_sg_entities(
            sg_project,
            [sg_entity],
            sg_entity_type,
            tasks={task_name: task_name}
        )
    else:
        existing_tasks = ayon_api.get_tasks_by_folder_path(project_name, folder_entity["path"])
        existing_task_names = [task["name"] for task in existing_tasks]
        if task_name not in existing_task_names:
            return False

    return True


def validate_version(
    project_name,
    folder_path,
    task_name,
    product_type,
    product_name,
    expected_representations,
    publish_data,
    overwrite_version=False,
    force_task_creation=False,
    product_group=None,
):
    # String representation of product being published
    item_str = f"Folder Path: {folder_path} - Task: {task_name} - Product Type: {product_type} - Product Name: {product_name}"

    version_data = {}

    # Validate that all required fields exist
    if not all(
        [
            project_name,
            folder_path,
            task_name,
            product_type,
            product_name,
            expected_representations
        ]
    ):
        msg = (
            f"{item_str} -> Can't publish version without all arguments."
        )
        logger.error(msg)
        return msg, False, version_data

    folder_entity = ayon_api.get_folder_by_path(project_name, folder_path)
    if not folder_entity:
        msg = (
            f"{item_str} -> Couldn't find folder in project with path {folder_path}, make sure it exists."
        )
        logger.error(msg)
        return msg, False, version_data

    version_data["folder_entity"] = folder_entity
    context_data = folder_entity["data"]

    # Validate that the version doesn't exist if we choose to not overwrite
    if not overwrite_version and publish_data.get("version"):
        if check_version_exists(
            project_name, folder_entity, product_name, publish_data.get("version")
        ):
            msg = (
                f"{item_str} -> Version already exists."
            )
            logger.error(msg)
            return msg, False, version_data

    # Validate that the task exists
    if not check_task_exists(project_name, folder_entity, task_name, force_task_creation):
        msg = (
            f"{item_str} -> Task '{task_name}' doesn't exist."
        )
        logger.error(msg)
        return msg, False, version_data

    # TODO: write some logic that finds the main path from the list of
    # representations
    source_path = list(expected_representations.values())[0]
    version_data["source_path"] = source_path

    instance_data = {
        "project": project_name,
        "productType": product_type,
        "productName": product_name,
        "family": product_type,
        "families": publish_data.get("families", []),
        "folderPath": folder_path,
        "task": task_name,
        "fps": publish_data.get("fps", context_data.get("fps")),
        "comment": publish_data.get("comment", ""),
        "source": source_path,
        "overrideExistingFrame": False,
        "useSequenceForReview": True,
        "colorspace": publish_data.get("src_colorspace", "scene_linear"),
        "version": publish_data.get("version"),
        "outputDir": os.path.dirname(source_path),
        "convertToScanline": publish_data.get("convertToScanline", False),
        "stagingDir_persistent": True,
    }

    if product_group:
        instance_data["productGroup"] = product_group

    version_data["instance_data"] = instance_data

    add_review = product_type in REVIEW_FAMILIES
    # Quick dirty solution to avoid generating reviews for certain
    # tasks
    if task_name in TASKS_TO_IGNORE_REVIEW:
        add_review = False
    version_data["add_review"] = add_review

    representations = utils.get_representations(
        instance_data,
        expected_representations,
        add_review=add_review,
        publish_to_sg=product_type in PUBLISH_TO_SG_FAMILIES,
    )
    if not representations:
        msg = f"{item_str} -> No representations could be found on expected dictionary: {expected_representations}"
        logger.error(msg)
        return msg, False, None, None

    version_data["representations"] = representations

    return f"{item_str} -> Valid", True, version_data


def publish_version(
    project_name,
    folder_path,
    task_name,
    product_type,
    product_name,
    expected_representations,
    publish_data,
    overwrite_version=False,
    force_task_creation=False,
    product_group=None,
    local_publish=False
):
    # String representation of product being published
    item_str = f"Folder Path: {folder_path} - Task: {task_name} - Product Type: {product_type} - Product Name: {product_name}"

    # Validate version while creating representations that we would need to publish
    msg, success, version_data = validate_version(
        project_name,
        folder_path,
        task_name,
        product_type,
        product_name,
        expected_representations,
        publish_data,
        overwrite_version,
        force_task_creation,
        product_group
    )
    if not success:
        return msg, False

    # Unpack data created on validate version function
    instance_data = version_data["instance_data"]
    representations = version_data["representations"]
    folder_entity = version_data["folder_entity"]
    add_review = version_data["add_review"]
    source_path = version_data["source_path"]

    # Get project code to grab the project code and add it to the task name
    project_entity = ayon_api.get_project(project_name)
    project_code = project_entity["code"]

    deadline_task_name = "Publish {} - {}{} - {} - {} - {} ({})".format(
        product_type,
        product_name,
        " v{0:03d}".format(int(instance_data.get("version"))) if instance_data.get("version") else "",
        task_name,
        folder_path,
        project_name,
        project_code,
    )

    # Fill instance data with anatomyData
    task_entity = ayon_api.get_task_by_name(
        project_name, folder_entity["id"], task_name
    )
    anatomy_data = get_template_data(
        project_entity, folder_entity, task_entity
    )
    instance_data["anatomyData"] = anatomy_data

    # If we are generating a review, create a Deadline Nuke task for
    # the representation that is an image extension
    job_submissions = []
    if add_review:
        response = generate_review_from_instance(
            project_name,
            project_code,
            folder_path,
            task_name,
            product_type,
            product_name,
            publish_data,
            representations,
            instance_data,
            deadline_task_name,
        )
        if response:
            job_submissions.append(response)

    instance_data["frameStart"] = int(representations[0]["frameStart"])
    instance_data["frameEnd"] = int(representations[0]["frameEnd"])
    instance_data["frameStartHandle"] = int(representations[0]["frameStart"])
    instance_data["frameEndHandle"] = int(representations[0]["frameEnd"])

    # add representation
    instance_data["representations"] = representations
    instances = [instance_data]

    # Create farm job to run OP publish
    metadata_path = utils.create_metadata_path(instance_data)
    logger.info("Metadata path: %s", metadata_path)

    publish_args = [
        "--headless",
        "publish",
        '"{}"'.format(metadata_path),
        "--targets", "deadline",
        "--targets", "farm",
    ]

    # Create dictionary of data specific to OP plugin for payload submit
    plugin_data = {
        "Arguments": " ".join(publish_args),
        "Version": os.getenv("OPENPYPE_VERSION"),
        "SingleFrameOnly": "True",
    }

    username = getpass.getuser()

    # Submit job to Deadline
    extra_env = {
        "AYON_PROJECT_NAME": project_name,
        "AYON_FOLDER_PATH": folder_path,
        "AYON_TASK_NAME": task_name,
        "AYON_WORKDIR": os.path.dirname(source_path),
        "AYON_SG_USERNAME": username,
        "AYON_PUBLISH_JOB": "1",
        "AYON_RENDER_JOB": "0",
        "AYON_REMOTE_JOB": "0",
        "AYON_BUNDLE_NAME": os.getenv("AYON_BUNDLE_NAME")
    }

    # publish job file
    publish_job = {
        "folderPath": instance_data["folderPath"],
        "frameStart": instance_data["frameStartHandle"],
        "frameEnd": instance_data["frameEndHandle"],
        "fps": instance_data["fps"],
        "source": instance_data["source"],
        "user": getpass.getuser(),
        "version": None,  # this is workfile version
        "comment": instance_data["comment"],
        "job": {},
        "instances": instances,
    }

    if local_publish:
        logger.info("Writing json file: {}".format(metadata_path))
        with open(metadata_path, "w") as f:
            json.dump(publish_job, f, indent=4, sort_keys=True)

        publish_cmd = ["/pipe/ayon/release/current/ayon"] + publish_args
        try:
            run_subprocess(publish_cmd, env=extra_env)
        except RuntimeError as error:
            msg = f"{item_str} -> Failed to publish locally: {error}"
            return msg, False
        
        msg = f"{item_str} -> Local publish"
    else:
        logger.debug("Submitting payload...")
        response = submit.payload_submit(
            plugin="Ayon",
            plugin_data=plugin_data,
            batch_name=publish_data.get("jobBatchName") or deadline_task_name,
            task_name=deadline_task_name,
            group=dl_constants.AYON_GROUP,
            extra_env=extra_env,
            job_dependencies=job_submissions
        )
        deadline_job_id = response.get("id")
        publish_job["deadline_publish_job_id"] = deadline_job_id
        msg = f"{item_str} -> Deadline Job {deadline_job_id}"

        logger.info("Writing json file: {}".format(metadata_path))
        with open(metadata_path, "w") as f:
            json.dump(publish_job, f, indent=4, sort_keys=True)

    return msg, True


def generate_review_from_instance(
    project_name,
    project_code,
    folder_path,
    task_name,
    product_type,
    product_name,
    publish_data,
    representations,
    instance_data,
    deadline_task_name,
):
    anatomy = Anatomy(project_name)

    review_repre = None
    for repre in representations:
        # Skip generating review if one of the repres is already
        # a supported review extension
        if repre["ext"] in review.VIDEO_EXTENSIONS:
            review_repre = None
            break
        elif repre["ext"] in review.GENERATE_REVIEW_EXTENSIONS:
            review_repre = repre

    if not review_repre:
        return None
    
    staging_dir = anatomy.fill_root(
        review_repre["stagingDir"]
    )

    # Set output colorspace default to 'shot_lut' unless it's a review/reference family
    out_colorspace = "shot_lut"
    if product_type in IGNORE_LUT_FAMILIES:
        out_colorspace = ""

    # Create dictionary with some useful data required to submit
    # Nuke review job to the farm
    review_data = {
        "comment": publish_data.get("comment", ""),
        "batch_name": publish_data.get("jobBatchName") or deadline_task_name,
        "src_colorspace": publish_data.get("src_colorspace", "scene_linear"),
        # We default the output colorspace to out_colorspace if it's not
        # explicitly set on the publish_data dictionary
        "out_colorspace": publish_data.get("out_colorspace", out_colorspace),
        "product_name": product_name,
        "contact_sheet": True if product_name.endswith("_util") else False,
    }

    # Create read path to pass to Nuke task
    basename = review_repre["files"][0] if isinstance(review_repre["files"], list) else review_repre["files"]
    read_path = os.path.join(staging_dir, basename)
    read_path = path_tools.replace_frame_number_with_token(read_path, "#", padding=True)
    logger.debug("Review read path: %s", read_path)

    # Create review output path
    file_name = f"{review_repre['name']}_h264.mov"
    output_path = os.path.join(
        staging_dir,
        file_name
    )
    logger.debug("Review output path: %s", output_path)

    response = review.generate_review(
        project_name,
        project_code,
        folder_path,
        task_name,
        read_path,
        output_path,
        review_repre["frameStart"],
        review_repre["frameEnd"],
        review_data
    )

    # Add future generated review to representations that will be published
    if response:
        # Add review as a new representation to publish
        representations.append(
            {
                "name": "h264",
                "ext": "mov",
                "files": file_name,
                "frameStart": repre["frameStart"],
                "frameEnd": repre["frameEnd"],
                "stagingDir": staging_dir,
                "fps": instance_data.get("fps"),
                "tags": ["shotgridreview"],
            }
        )

    return response