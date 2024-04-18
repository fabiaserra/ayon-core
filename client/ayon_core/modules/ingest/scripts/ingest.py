import os
import re
import clique
from collections import defaultdict

import ayon_api

from ayon_core.lib import Logger
from ayon_core.lib.transcoding import IMAGE_EXTENSIONS
from ayon_core.modules.deadline.lib import publish
from ayon_core.modules.ingest.lib import textures


# Types of tasks that we support for outsource
ROTO_TASK = "roto"
PAINT_TASK = "paint"
TRACK_2D_TASK = "2dtrack"
TRACK_3D_TASK = "3dtrack"
COMP_TASK = "comp"
EDIT_TASK = "edit"
GENERIC_TASK = "generic"
OUTSOURCE_TASKS = [
    ROTO_TASK,
    PAINT_TASK,
    COMP_TASK,
    TRACK_2D_TASK,
    TRACK_3D_TASK,
    EDIT_TASK,
    GENERIC_TASK,
]

# Dictionary that maps the extension name to the representation name
# we want to use for it
EXT_TO_REP_NAME = {
    ".nk": "nuke",
    ".ma": "maya",
    ".mb": "maya",
    ".hip": "houdini",
    ".sfx": "silhouette",
    ".mocha": "mocha",
    ".psd": "photoshop"
}

# Dictionary that maps the file extensions to the product_type name we want to
# ingest it as
# This is only used as a fallback in case we can't extract the product_type
# information from other contexts like the file name (i.e, cameras would
# for example have .abc as extension too)
FAMILY_EXTS_MAP = {
    "render": {".exr", ".dpx", ".tif", ".tiff", ".jpg", ".jpeg"},
    "plate": {".exr"},
    "pointcache": {".abc"},
    "camera": {".abc", ".fbx"},
    "reference": {".mov", ".mp4", ".mxf", ".avi", ".wmv"},
    "workfile": {".nk", ".ma", ".mb", ".hip", ".sfx", ".mocha", ".psd"},
    "distortion": {".nk", ".exr"},
    "color_grade": {".ccc", ".cc"},
    "textures": {".png", ".rat", ".tx", ".exr", ".jpg", ".jpeg"},
    "image": {".hdri", ".hdr"}
}

# Compatible file extensions for camera products
CAMERA_EXTS = {".abc", ".fbx"}

# Dictionary that maps names that we find in a filename to different
# data that we want to override in the publish data
FUZZY_NAME_OVERRIDES = {
    ("_cam", "camera"): {
        "product_type": "camera",
    },
    ("_mm", "_trk", "track", "matchmove", "tracking"): {
        "task_name": TRACK_3D_TASK
    },
    ("distortion", "distortion_node"): {
        "product_type": "distortion"
    },
    ("_geo_", ): {
        "product_type": "pointcache",
    },
}

# List of fields that are required in the products in order to publish them
MUST_HAVE_FIELDS = {
    "folder_path",
    "task_name",
    "product_type",
    "product_name",
    "rep_name",
}

# Regular expression that matches file names following precisely our naming convention
# format that we expect from the vendor
# Examples:
# uni_pg_0455_plt_01_roto_output-01_v001
# uni_pg_0455_plt_01_roto_output-02_v001
# uni_pg_0380_denoise_dn_plt_01_v004_paint_v001
TASKS_RE = "|".join(OUTSOURCE_TASKS)
STRICT_FILENAME_RE_STR = (
    r"^(?P<shot_code>({{shot_codes}}))_"
    r"(?P<product_name>[a-zA-Z0-9_]+)_"
    r"(?P<task>({}))?_?"
    r"(?P<variant>[a-zA-Z0-9_\-]*_)?"
    r"(?P<delivery_version>v\d+)"
    r"(?:\.(?P<frame>%0?\d*d|\d+))?"
    r"(?P<extension>\.[a-zA-Z]+)$"
).format(TASKS_RE)

# Less greedy regular expression that matches the generic file name format that we
# expect from the vendor
GENERIC_FILENAME_RE = re.compile(
    r"^(?P<shot_code>[a-zA-Z0-9]+_[a-zA-Z0-9]+_\d+)_"
    r"(?P<product_name>[a-zA-Z0-9_]+)_"
    fr"(?P<task>({TASKS_RE}))?_?"
    r"(?P<variant>[a-zA-Z0-9_\-]*_)?"
    r"(?P<delivery_version>v\d+)"
    r"(?:\.(?P<frame>%0?\d*d|\d+))?"
    r"(?P<extension>\.[a-zA-Z]+)$",
    re.IGNORECASE
)

# Regular expression that we use as a final resource if we haven't found any
# of the publish data yet
# Example: MP01_V0001_CC.%04d.exr
FALLBACK_FILENAME_RE = re.compile(
    r"(?P<product_name>\w+)_"
    r"(?P<delivery_version>v\d+)"
    r"_?(?P<suffix>[a-zA-Z0-9_\-]*)"
    r"(?:\.(?P<frame>%0?\d*d|\d+))?"
    r"(?P<extension>\.[a-zA-Z]+)$",
    re.IGNORECASE
)

# Words to remove from product_name if they exist
product_nameS_TO_IGNORE = {
    "abc",
    "fbx",
}
product_nameS_TO_IGNORE_RE = re.compile(
    f"_?({'|'.join(re.escape(word) for word in product_nameS_TO_IGNORE)})_?",
    re.IGNORECASE
)

# Fallback task name if we can't find any
TASK_NAME_FALLBACK = "edit"

# Regular expression to find show code given a media path that starts with the
# project root folder
SHOW_MATCH_RE = re.compile(r"/proj/(?P<show>\w+)")

# Regular expression to match package name to extract vendor code
VENDOR_PACKAGE_RE = r"From_(\w+)"


logger = Logger.get_logger(__name__)


def validate_products(
    project_name, products_data, overwrite_version=False, force_task_creation=False
):
    """Given a list of ProductRepresentation objects, validate if there's any potential errors

    Args:
        project_name (str): Name of the project to validate to
        products_data (list): List of ProductRepresentation objects

    Returns:
        tuple: Tuple containing:
            report_items (dict): Dictionary with the messages to show in the
                report.
            success (bool): Whether the validate was successful or not

    """
    report_items = defaultdict(list)
    success = True

    if not project_name:
        return report_items["Project not selected"].append(
            "Select project before validating!"
        ), False

    # Go through list of products data from ingest dialog table and combine the
    # representations dictionary for the products that target the same product_name
    products = {}
    for product_item in products_data:
        item_str = f"{product_item.folder_path} - {product_item.task} - {product_item.product_type} - {product_item.product_name}"
        logger.debug(item_str)

        key = (
            product_item.folder_path,
            product_item.task,
            product_item.product_name
        )
        if not all(key) and product_item.product_type:
            logger.debug(
                "Skipping product as it doesn't have all required fields to publish"
            )
            continue
        elif key not in products:
            products[key] = {
                "expected_representations": {
                    product_item.rep_name: product_item.path,
                },
                "version": product_item.version,
                "product_type": product_item.product_type,
            }
        else:
            if product_item.rep_name in products[key]["expected_representations"]:
                report_items["Duplicated representations in products"].append(
                    item_str + f" : {product_item.rep_name}"
                )
                continue
            if product_item.product_type != products[key]["product_type"]:
                report_items["Duplicated product/Product names in different families, they must be unique"].append(
                    item_str + f" : {product_item.rep_name}"
                )
                continue
            products[key]["expected_representations"][product_item.rep_name] = product_item.path

    for product_fields, product_data in products.items():
        folder_path, task, product_name = product_fields
        msg, success = publish.validate_version(
            project_name,
            folder_path,
            task,
            product_data["product_type"],
            product_name,
            product_data["expected_representations"],
            {"version": product_data.get("version")},
            overwrite_version,
            force_task_creation,
        )
        if success:
            report_items["Products are valid to submit"].append(msg)
        else:
            report_items["Failed validation for products"].append(msg)

    return report_items, success


def publish_products(
    project_name, products_data, overwrite_version=False, force_task_creation=False
):
    """Given a list of ProductRepresentation objects, publish them to OP and SG

    Args:
        project_name (str): Name of the project to publish to
        products_data (list): List of ProductRepresentation objects

    Returns:
        tuple: Tuple containing:
            report_items (dict): Dictionary with the messages to show in the
                report.
            success (bool): Whether the publish was successful or not

    """
    report_items = defaultdict(list)
    success = True

    if not project_name:
        return report_items["Project not selected"].append(
            "Select project before publishing!"
        ), False

    # Go through list of products data from ingest dialog table and combine the
    # representations dictionary for the products that target the same product_name
    products = {}
    for product_item in products_data:
        item_str = f"{product_item.folder_path} - {product_item.task} - {product_item.product_type} - {product_item.product_name}"
        logger.debug(item_str)

        key = (
            product_item.folder_path,
            product_item.task,
            product_item.product_type,
            product_item.product_name
        )
        if not all(key):
            logger.debug(
                "Skipping product as it doesn't have all required fields to publish"
            )
            continue
        elif key not in products:
            products[key] = {
                "expected_representations": {
                    product_item.rep_name: product_item.path,
                },
                "version": product_item.version,
            }
        else:
            if product_item.rep_name in products[key]["expected_representations"]:
                logger.debug("Duplicated representation")
                report_items["Duplicated representation in product"].append(
                    item_str
                )
                continue

            products[key]["expected_representations"][product_item.rep_name] = product_item.path

    logger.debug("Flattened products: %s", products)

    for product_fields, product_data in products.items():
        folder_path, task_name, product_type, product_name = product_fields
        msg, success = publish.publish_version(
            project_name,
            folder_path,
            task_name,
            product_type,
            product_name,
            product_data["expected_representations"],
            {"version": product_data.get("version")},
            overwrite_version,
            force_task_creation,
        )
        if success:
            report_items["Successfully submitted products to publish"].append(msg)
        else:
            report_items["Failed submission for products"].append(msg)

    return report_items, success


def get_products_from_filepath(package_path, project_name, project_code):
    """Given a path to a folder, find all the products that we can ingest from it"""
    def _split_camel_case(name):
        """Split camel case name into words separated by underscores"""
        result = ""
        for i, c in enumerate(name):
            # If letter is capital, it's not after a "_", and the previous character is
            # not uppercase we add a lowercase
            if i > 0 and c.isupper() and name[i-1] != "_" and not name[i-1].isupper():
                result += "_"
            result += c.lower()
        return result

    folder_names = [
        folder_entity["name"] for folder_entity in ayon_api.get_folders(project_name)
    ]

    folders_re = "|".join(folder_names)
    strict_regex_str = STRICT_FILENAME_RE_STR.format(shot_codes=folders_re)
    strict_regex = re.compile(
        strict_regex_str, re.IGNORECASE
    )
    logger.debug("Strict regular expression: %s", strict_regex_str)

    # Recursively find all paths on folder and check if it's a product we can ingest
    products = {}
    for root, _, files in os.walk(package_path):
        # Create a list of all the collections of files and single files that
        # we find that could potentially be an ingestable product
        collections, remainders = clique.assemble(files, patterns=[clique.PATTERNS["frames"]])
        filepaths_frame_range = [
            (
                os.path.join(root, collection.format("{head}{padding}{tail}")),
                min(collection.indexes),
                max(collection.indexes)
            )
            for collection in collections
        ]
        filepaths_frame_range.extend(
            (
                os.path.join(root, remainder),
                None,
                None
            )
            for remainder in remainders
        )

        for filepath_frame_range in filepaths_frame_range:

            filepath, frame_start, frame_end = filepath_frame_range

            # Skip metadata files that we generate to ingest to the farm
            if filepath.endswith(".json") or filepath.endswith(".txt") or \
                filepath.endswith("exr_h264.mov") or "/nuke_review_script/" in filepath:
                continue

            publish_data = get_product_from_filepath(
                project_name,
                project_code,
                filepath,
                strict_regex,
                folder_names,
            )
            if not publish_data:
                continue

            # Add frame range to publish data
            if frame_start:
                publish_data["frame_start"] = frame_start

            if frame_end:
                publish_data["frame_end"] = frame_end

            # Validate that we have all the required fields to publish
            if not all([publish_data.get(field_name) for field_name in MUST_HAVE_FIELDS]):
                logger.warning("Missing fields in publish data: %s", publish_data)

            product_name = publish_data["product_name"]
            if product_name:
                # Make sure Product name is always lower case and split by underscores
                product_name = _split_camel_case(product_name)
                product_name = ayon_api.slugify_string(product_name)
                # Add `_vnd` to the Product name to show it comes from a vendor
                if "/io/incoming" in filepath:
                    # Patch to suffix product_type name to workaround OP limitation of
                    # product_names needing to be unique
                    product_type = publish_data["product_type"]
                    if product_type == "workfile" and product_type not in product_name:
                        product_name = f"{product_name}_{product_type}"
                    product_name = f"{product_name}_vnd"
                publish_data["product_name"] = product_name

            products[filepath] = publish_data

    return products


def get_product_from_filepath(
    project_name, project_code, filepath, strict_regex, folder_names
):
    """Given a filepath, try to extract the publish data from it"""
    filename = os.path.basename(filepath)
    extension = os.path.splitext(filename)[-1]

    folder_entity = None
    folder_path = None
    task_name = None
    product_type = None
    product_name = None
    variant_name = None
    delivery_version = None

    re_match = strict_regex.match(filename)
    if not re_match:
        logger.debug("Strict regular expression didn't match filename '%s'.", filename)
        re_match = GENERIC_FILENAME_RE.match(filename)

    if re_match:
        logger.debug("Found matching regular expression for '%s'.", filename)

        shot_code = re_match.group("shot_code")
        product_name = re_match.group("product_name")
        task_name = re_match.group("task")
        variant_name = re_match.group("variant")
        delivery_version = re_match.group("delivery_version")
        extension = re_match.group("extension") or extension

        logger.debug("Shot code: '%s'", shot_code)
        logger.debug("Product name: '%s'", product_name)
        logger.debug("Task name: '%s'", task_name)
        logger.debug("Variant name: '%s'", variant_name)
        logger.debug("Delivery version: '%s'", delivery_version)
        logger.debug("Extension: '%s'", extension)

        logger.debug("Looking for folder name '%s'", shot_code)

        folder_entity = get_folder_by_name_case_not_sensitive(project_name, shot_code)

    # If folder doc hasn't been found yet, try find it by just looking if the
    # string exists anywhere in the filepath
    if not folder_entity:
        logger.debug(
            "Asset name not found yet, doing string comparison in filepath '%s'",
            filepath
        )
        folder_entity, found_name = parse_containing(
            project_name, project_code, filepath, folder_names
        )
        if folder_entity:
            logger.debug(
                "Found folder name '%s' in filepath '%s'.", folder_entity["name"], filepath
            )
            if not product_name:
                simple_filename = filename.lower().replace(
                    found_name, ""
                )
                logger.debug(
                    "Product name not found yet, trying last resort with '%s' after removing prefix '%s'",
                    simple_filename, found_name
                )
                # Remove the first character after removing the folder name
                # if it's a separator character
                if simple_filename.startswith(("-", "_", ".")):
                    simple_filename = simple_filename[1:]
                fallback_re = FALLBACK_FILENAME_RE.match(simple_filename)
                if fallback_re:
                    product_name = fallback_re.group("product_name")
                    delivery_version = fallback_re.group("delivery_version")
                    extension = fallback_re.group("extension")
                else:
                    logger.debug("Fallback filename regex didn't match")
                    product_name = simple_filename.split(".")[0]

            if not task_name:
                task_name = TASK_NAME_FALLBACK

    if folder_entity:
        folder_path = folder_entity["path"]
    else:
        logger.warning("Couldn't find folder in file '%s'", filepath)

    # Make sure extension is in lower case
    extension = extension.lower()

    # Find product_type name based on file extension
    for product_type, extensions in FAMILY_EXTS_MAP.items():
        if extension in extensions:
            product_type = product_type
            break

    if extension in CAMERA_EXTS:
        if "camera" in filepath.lower():
            logger.debug(
                "Found 'camera' string in filepath %s, assuming it's a 'camera' product",
                filepath,
            )
            product_type = "camera"

    if not product_type:
        logger.warning(
            "Couldn't find a product_type for the file extension '%s'", extension
        )

    # Create representation name from extension
    rep_name = EXT_TO_REP_NAME.get(extension)
    if not rep_name:
        rep_name = extension.rsplit(".", 1)[-1]

    # Override task name if we find any of the names of the supported tasks in the
    # filepath
    if task_name not in OUTSOURCE_TASKS:
        if task_name:
            logger.debug(
                "Overriding Product name '%s' with task name '%s'",
                product_name, task_name
            )
            product_name = task_name
        for possible_task_name in OUTSOURCE_TASKS:
            if possible_task_name in filepath.split("/") or f"_{possible_task_name}_" in filepath:
                logger.debug(
                    "Found '%s' in filepath '%s', assuming it's a '%s' task",
                    possible_task_name,
                    filepath,
                    possible_task_name,
                )
                task_name = possible_task_name
                break

    # Make sure delivery version is an integer
    if delivery_version:
        delivery_version = int(delivery_version.strip("v"))

    publish_data = {
        "project_name": project_name,
        "folder_path": folder_path,
        "task_name": task_name,
        "product_type": product_type,
        "product_name": product_name,
        "variant_name": variant_name,
        "version": delivery_version,
        "rep_name": rep_name
    }

    # Go through the fuzzy name overrides and apply them if we find
    # a match
    for fuzzy_names, overrides in FUZZY_NAME_OVERRIDES.items():
        for fuzzy_name in fuzzy_names:
            if fuzzy_name in filepath.lower():
                logger.debug(
                    "Found fuzzy name '%s' in filename '%s', applying overrides %s",
                    fuzzy_name,
                    filename,
                    overrides,
                )
                publish_data.update(overrides)

    # If task name is still not one of the supported ones, mark it as None so we
    # can clearly see what happened and not error out during the publish
    if publish_data["task_name"] not in OUTSOURCE_TASKS:
        logger.error(
            "Task name found '%s' in filepath '%s' is not one of the supported ones "
            "by this tool: %s.\nIf you think the task should be parsed by the tool "
            "please report it to @pipe",
            publish_data["task_name"],
            filepath,
            OUTSOURCE_TASKS
        )
        publish_data["task_name"] = EDIT_TASK

    # Remove tokens that can be ignored from Product name
    if publish_data["product_name"]:
        publish_data["product_name"], count = product_nameS_TO_IGNORE_RE.subn(
            "", publish_data["product_name"]
        )
        if count:
            logger.debug("Removed some common tokens from product_name we can ignore")

    # Add variant name to Product name if we have one
    if publish_data["variant_name"] and publish_data["product_name"]:
        # Remove the last underscore from captured variant name
        variant_name = publish_data["variant_name"].rsplit("_", 1)[0]
        publish_data["product_name"] = f"{publish_data['product_name']}_{variant_name}"

    # If no Product name found yet just use the filename
    if not publish_data["product_name"]:
        publish_data["product_name"] = filename.split(".")[0]

    # If extension is an image, guess input colorspace
    if extension in IMAGE_EXTENSIONS:
        in_colorspace = textures.guess_colorspace(filepath)
        if in_colorspace:
            publish_data["in_colorspace"] = in_colorspace

    logger.debug("Publish data for filepath %s: %s", filepath, publish_data)

    return publish_data


def get_folder_by_name_case_not_sensitive(project_name, folder_name):
    """Get folder by name ignoring case"""
    # NOTE: This is not supported yet in Ayon
    # folder_name = re.compile(folder_name, re.IGNORECASE)

    folders = list(
        ayon_api.get_folders(project_name, folder_names=[folder_name])
    )
    if folders:
        if len(folders) > 1:
            logger.warning(
                "Too many records found for {}, using first.".format(folder_name)
            )

        return folders.pop()


def parse_containing(project_name, project_code, filepath, folder_names):
    """Parse filepath to find folder name"""
    logger.debug("Looking for any folder names '%s' in filepath '%s'", folder_names, filepath)
    for folder_name in folder_names:
        if folder_name.lower() in filepath.lower():
            return ayon_api.get_folder_by_name(
                project_name, folder_name
            ), folder_name.lower()

    # If we haven't find it yet check folder name without project code
    for folder_name in folder_names:
        if folder_name.lower().startswith(project_code.lower()):
            # Remove the project code + "_" from the folder name
            short_folder_name = folder_name[len(project_code)+1:]
            if short_folder_name in filepath.lower():
                return ayon_api.get_folder_by_name(
                    project_name, folder_name
                ), short_folder_name
        else:
            logger.warning(
                "Assets aren't starting with project code %s", project_code
            )
            return None, None

    return None, None
