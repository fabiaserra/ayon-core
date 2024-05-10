import attr
import os
import copy
import re
import warnings
import datetime
from copy import deepcopy

import ayon_api
import pyblish.api
import clique

from ayon_core.pipeline import (
    get_current_project_name,
    get_representation_path,
    Anatomy,
)
from ayon_core.lib import Logger
from ayon_core.pipeline.publish import KnownPublishError
from ayon_core.pipeline.farm.patterning import match_aov_pattern


@attr.s
class TimeData(object):
    """Structure used to handle time related data."""
    start = attr.ib(type=int)
    end = attr.ib(type=int)
    fps = attr.ib()
    step = attr.ib(default=1, type=int)
    handle_start = attr.ib(default=0, type=int)
    handle_end = attr.ib(default=0, type=int)


def remap_source(path, anatomy):
    """Try to remap path to rootless path.

    Args:
        path (str): Path to be remapped to rootless.
        anatomy (Anatomy): Anatomy object to handle remapping
            itself.

    Returns:
        str: Remapped path.

    Throws:
        ValueError: if the root cannot be found.

    """
    success, rootless_path = (
        anatomy.find_root_template_from_path(path)
    )
    if success:
        source = rootless_path
    else:
        raise ValueError(
            "Root from template path cannot be found: {}".format(path))
    return source


def extend_frames(folder_path, product_name, start, end):
    """Get latest version of asset nad update frame range.

    Based on minimum and maximum values.

    Arguments:
        folder_path (str): Folder path.
        product_name (str): Product name.
        start (int): Start frame.
        end (int): End frame.

    Returns:
        (int, int): update frame start/end

    """
    # Frame comparison
    prev_start = None
    prev_end = None

    project_name = get_current_project_name()
    folder_entity = ayon_api.get_folder_by_path(
        project_name, folder_path, fields={"id"}
    )
    version_entity = ayon_api.get_last_version_by_product_name(
        project_name,
        product_name,
        folder_entity["id"]
    )

    # Set prev start / end frames for comparison
    if not prev_start and not prev_end:
        prev_start = version_entity["attrib"]["frameStart"]
        prev_end = version_entity["attrib"]["frameEnd"]

    updated_start = min(start, prev_start)
    updated_end = max(end, prev_end)

    return updated_start, updated_end


def get_time_data_from_instance_or_context(instance):
    """Get time data from instance (or context).

    If time data is not found on instance, data from context will be used.

    Args:
        instance (pyblish.api.Instance): Source instance.

    Returns:
        TimeData: dataclass holding time information.

    """
    context = instance.context
    return TimeData(
        start=instance.data.get("frameStart", context.data.get("frameStart")),
        end=instance.data.get("frameEnd", context.data.get("frameEnd")),
        fps=instance.data.get("fps", context.data.get("fps")),
        step=instance.data.get("byFrameStep", instance.data.get("step", 1)),
        handle_start=instance.data.get(
            "handleStart", context.data.get("handleStart")
        ),
        handle_end=instance.data.get(
            "handleEnd", context.data.get("handleEnd")
        )
    )


def get_transferable_representations(instance, log=None):
    """Transfer representations from original instance.

    This will get all representations on the original instance that
    are flagged with `publish_on_farm` and return them to be included
    on skeleton instance if needed.

    Args:
        instance (pyblish.api.Instance): Original instance to be processed.

    Return:
        list of dicts: List of transferable representations.

    """
    if log is None:
        log = Logger.get_logger(__name__)

    anatomy = instance.context.data["anatomy"]  # type: Anatomy
    to_transfer = []

    for representation in instance.data.get("representations", []):
        if "publish_on_farm" not in representation.get("tags", []):
            continue

        trans_rep = representation.copy()

        # remove publish_on_farm from representations tags
        trans_rep["tags"].remove("publish_on_farm")

        staging_dir = trans_rep.get("stagingDir")

        if staging_dir:
            try:
                trans_rep["stagingDir"] = remap_source(staging_dir, anatomy)
            except ValueError:
                log.warning(
                    ("Could not find root path for remapping \"{}\". "
                     "This may cause issues on farm.").format(staging_dir))

        to_transfer.append(trans_rep)
    return to_transfer


def create_skeleton_instance(
    instance, families_transfer=None, instance_transfer=None, log=None
):
    # type: (pyblish.api.Instance, list, dict) -> dict
    """Create skeleton instance from original instance data.

    This will create dictionary containing skeleton
    - common - data used for publishing rendered instances.
    This skeleton instance is then extended with additional data
    and serialized to be processed by farm job.

    Args:
        instance (pyblish.api.Instance): Original instance to
            be used as a source of data.
        families_transfer (list): List of family names to transfer
            from the original instance to the skeleton.
        instance_transfer (dict): Dict with keys as families and
            values as a list of property names to transfer to the
            new skeleton.

    Returns:
        dict: Dictionary with skeleton instance data.

    """
    if not log:
        log = Logger.get_logger(__name__)

    context = instance.context
    data = instance.data.copy()
    anatomy = instance.context.data["anatomy"]  # type: Anatomy

    # get time related data from instance (or context)
    time_data = get_time_data_from_instance_or_context(instance)

    if data.get("extendFrames", False):
        time_data.start, time_data.end = extend_frames(
            data["folderPath"],
            data["productName"],
            time_data.start,
            time_data.end,
        )

    source = data.get("source") or context.data.get("currentFile")
    success, rootless_path = (
        anatomy.find_root_template_from_path(source)
    )
    if success:
        source = rootless_path
    else:
        log.warning(
            "Could not find root path for remapping \"{}\". "
            "This may cause issues.".format(source)
        )

    # family = ("render"
    #           if "prerender" not in instance.data["families"]
    #           else "prerender")
    # families = [family]
    families = []

    # pass review to families if marked as review
    if data.get("review"):
        families.append("review")

    instance_skeleton_data = {
        "productType": data["productType"],
        "productName": data["productName"],
        "task": data["task"],
        "families": families,
        "folderPath": data["folderPath"],
        "frameStart": time_data.start,
        "frameEnd": time_data.end,
        "handleStart": time_data.handle_start,
        "handleEnd": time_data.handle_end,
        "frameStartHandle": time_data.start - time_data.handle_start,
        "frameEndHandle": time_data.end + time_data.handle_end,
        "comment": data.get("comment"),
        "fps": time_data.fps,
        "source": source,
        "extendFrames": data.get("extendFrames"),
        "overrideExistingFrame": data.get("overrideExistingFrame"),
        "pixelAspect": data.get("pixelAspect", 1),
        "resolutionWidth": data.get("resolutionWidth", 1920),
        "resolutionHeight": data.get("resolutionHeight", 1080),
        "multipartExr": data.get("multipartExr", False),
        "jobBatchName": data.get("jobBatchName", ""),
        "useSequenceForReview": data.get("useSequenceForReview", True),
        # map inputVersions `ObjectId` -> `str` so json supports it
        "inputVersions": list(map(str, data.get("inputVersions", []))),
        "colorspace": data.get("colorspace")
    }

    # skip locking version if we are creating v01
    instance_version = data.get("version")  # take this if exists
    if instance_version != 1:
        instance_skeleton_data["version"] = instance_version

    # transfer specific families from original instance to new render
    for item in families_transfer:
        if item in instance.data.get("families", []):
            instance_skeleton_data["families"] += [item]

    # transfer specific properties from original instance based on
    # mapping dictionary `instance_transfer`
    for key, values in instance_transfer.items():
        if key in instance.data.get("families", []):
            for v in values:
                instance_skeleton_data[v] = instance.data.get(v)

    representations = get_transferable_representations(instance)
    instance_skeleton_data["representations"] = representations

    persistent = instance.data.get("stagingDir_persistent") is True
    instance_skeleton_data["stagingDir_persistent"] = persistent

    return instance_skeleton_data


def _add_review_families(families):
    """Adds review flag to families.

    Handles situation when new instances are created which should have review
    in families. In that case they should have 'ftrack' too.

    TODO: This is ugly and needs to be refactored. Ftrack family should be
          added in different way (based on if the module is enabled?)

    """
    # if we have one representation with preview tag
    # flag whole instance for review and for ftrack
    if "ftrack" not in families and os.environ.get("FTRACK_SERVER"):
        families.append("ftrack")
    if "review" not in families:
        families.append("review")
    return families


def prepare_representations(skeleton_data, exp_files, anatomy, aov_filter,
                            skip_integration_repre_list,
                            do_not_add_review,
                            context,
                            color_managed_plugin, log=None):
    """Create representations for file sequences.

    This will return representations of expected files if they are not
    in hierarchy of aovs. There should be only one sequence of files for
    most cases, but if not - we create representation from each of them.

    Arguments:
        skeleton_data (dict): instance data for which we are
                         setting representations
        exp_files (list): list of expected files
        anatomy (Anatomy):
        aov_filter (dict): add review for specific aov names
        skip_integration_repre_list (list): exclude specific extensions,
        do_not_add_review (bool): explicitly skip review
        color_managed_plugin (publish.ColormanagedPyblishPluginMixin)
    Returns:
        list of representations

    """
    representations = []
    host_name = os.environ.get("AYON_HOST_NAME", "")
    collections, remainders = clique.assemble(exp_files)

    if not log:
        log = Logger.get_logger(__name__)

    # create representation for every collected sequence
    for collection in collections:
        ext = collection.tail.lstrip(".")
        preview = False
        # TODO 'useSequenceForReview' is temporary solution which does
        #   not work for 100% of cases. We must be able to tell what
        #   expected files contains more explicitly and from what
        #   should be review made.
        # - "review" tag is never added when is set to 'False'
        if skeleton_data["useSequenceForReview"]:
            # toggle preview on if multipart is on
            if skeleton_data.get("multipartExr", False):
                log.debug(
                    "Adding preview tag because its multipartExr"
                )
                preview = True
            else:
                render_file_name = list(collection)[0]
                # if filtered aov name is found in filename, toggle it for
                # preview video rendering
                preview = match_aov_pattern(
                    host_name, aov_filter, render_file_name
                )

        staging = os.path.dirname(list(collection)[0])
        success, rootless_staging_dir = (
            anatomy.find_root_template_from_path(staging)
        )
        if success:
            staging = rootless_staging_dir
        else:
            log.warning((
                "Could not find root path for remapping \"{}\"."
                " This may cause issues on farm."
            ).format(staging))

        frame_start = int(skeleton_data.get("frameStartHandle"))
        if skeleton_data.get("slate"):
            frame_start -= 1

        ### Starts Alkemy-X Override ###
        # Add override to support representations with the same extension

        # Make sure we don't have duplicate representation names
        repre_name = ext

        # We need to hard-code the case of the collection ending with _fr
        # as we can't be certain that clique.assemble always returns the
        # collections ordered so the `exr` that's from working resolution
        # always comes first
        if "_fr" in collection.head:
            repre_name = "{}_fr".format(ext)
            preview = False
        elif "h264" in collection.head:
            repre_name = "h264"

        # explicitly disable review by user
        preview = preview and not do_not_add_review
        rep = {
            "name": repre_name,
        ### Ends Alkemy-X Override ###
            "ext": ext,
            "files": [os.path.basename(f) for f in list(collection)],
            "frameStart": frame_start,
            "frameEnd": int(skeleton_data.get("frameEndHandle")),
            # If expectedFile are absolute, we need only filenames
            "stagingDir": staging,
            "fps": skeleton_data.get("fps"),
            "tags": ["review", "shotgridreview"] if preview else [],
        }

        # poor man exclusion
        if ext in skip_integration_repre_list:
            rep["tags"].append("delete")

        if skeleton_data.get("multipartExr", False):
            rep["tags"].append("multipartExr")

        # support conversion from tiled to scanline
        if skeleton_data.get("convertToScanline"):
            log.info("Adding scanline conversion.")
            rep["tags"].append("toScanline")

        representations.append(rep)

        if preview:
            skeleton_data["families"] = _add_review_families(
                skeleton_data["families"])

    # add remainders as representations
    for remainder in remainders:
        ext = remainder.split(".")[-1]

        staging = os.path.dirname(remainder)
        success, rootless_staging_dir = (
            anatomy.find_root_template_from_path(staging)
        )
        if success:
            staging = rootless_staging_dir
        else:
            log.warning((
                "Could not find root path for remapping \"{}\"."
                " This may cause issues on farm."
            ).format(staging))

        rep = {
            "name": ext,
            "ext": ext,
            "files": os.path.basename(remainder),
            "stagingDir": staging,
        }

        preview = match_aov_pattern(
            host_name, aov_filter, remainder
        )
        preview = preview and not do_not_add_review

        ### Starts Alkemy-X Override ###
        # Only add 'review' and 'shotgridreview' tags for video files
        # and single exr frames
        if ext not in {"mp4", "mov", "mxf", "exr"}:
            preview = False
        ### Ends Alkemy-X Override ###

        if preview:
            rep.update({
                "fps": skeleton_data.get("fps"),
                "tags": ["review", "shotgridreview"]
            })
            skeleton_data["families"] = \
                _add_review_families(skeleton_data["families"])

        already_there = False
        for repre in skeleton_data.get("representations", []):
            # might be added explicitly before by publish_on_farm
            already_there = repre.get("files") == rep["files"]
            if already_there:
                log.debug("repre {} already_there".format(repre))
                break

        if not already_there:
            representations.append(rep)

    for rep in representations:
        # inject colorspace data
        color_managed_plugin.set_representation_colorspace(
            rep, context,
            colorspace=skeleton_data["colorspace"]
        )

    return representations


def create_instances_for_aov(instance, skeleton, aov_filter,
                             skip_integration_repre_list,
                             do_not_add_review, log=None):
    """Create instances from AOVs.

    This will create new pyblish.api.Instances by going over expected
    files defined on original instance.

    Args:
        instance (pyblish.api.Instance): Original instance.
        skeleton (dict): Skeleton instance data.
        skip_integration_repre_list (list): skip

    Returns:
        list of pyblish.api.Instance: Instances created from
            expected files.

    """
    if not log:
        log = Logger.get_logger(__name__)

    # we cannot attach AOVs to other products as we consider every
    # AOV products of its own.
    additional_color_data = {
        "renderProducts": instance.data["renderProducts"],
        "colorspaceConfig": instance.data["colorspaceConfig"],
        "display": instance.data["colorspaceDisplay"],
        "view": instance.data["colorspaceView"]
    }

    # Get templated path from absolute config path.
    anatomy = instance.context.data["anatomy"]
    colorspace_template = instance.data["colorspaceConfig"]
    try:
        additional_color_data["colorspaceTemplate"] = remap_source(
            colorspace_template, anatomy)
    except ValueError as e:
        log.warning(e)
        additional_color_data["colorspaceTemplate"] = colorspace_template

    # if there are product to attach to and more than one AOV,
    # we cannot proceed.
    if (
        len(instance.data.get("attachTo", [])) > 0
        and len(instance.data.get("expectedFiles")[0].keys()) != 1
    ):
        raise KnownPublishError(
            "attaching multiple AOVs or renderable cameras to "
            "product is not supported yet.")

    # create instances for every AOV we found in expected files.
    # NOTE: this is done for every AOV and every render camera (if
    #       there are multiple renderable cameras in scene)
    return _create_instances_for_aov(
        instance,
        skeleton,
        aov_filter,
        additional_color_data,
        skip_integration_repre_list,
        do_not_add_review
    )


def _create_instances_for_aov(
    instance, skeleton, aov_filter, additional_data,
    skip_integration_repre_list, do_not_add_review, log=None
):
    """Create instance for each AOV found.

    This will create new instance for every AOV it can detect in expected
    files list.

    Args:
        instance (pyblish.api.Instance): Original instance.
        skeleton (dict): Skeleton data for instance (those needed) later
            by collector.
        additional_data (dict): ..
        skip_integration_repre_list (list): list of extensions that shouldn't
            be published
        do_not_addbe _review (bool): explicitly disable review


    Returns:
        list of instances

    Throws:
        ValueError:

    """
    # TODO: this needs to be taking the task from context or instance
    task = os.environ["AYON_TASK_NAME"]

    anatomy = instance.context.data["anatomy"]
    s_product_name = skeleton["productName"]
    cameras = instance.data.get("cameras", [])
    exp_files = instance.data["expectedFiles"]

    if not log:
        log = Logger.get_logger(__name__)

    instances = []
    # go through AOVs in expected files
    for aov, files in exp_files[0].items():
        cols, rem = clique.assemble(files)
        # we shouldn't have any reminders. And if we do, it should
        # be just one item for single frame renders.
        if not cols and rem:
            if len(rem) != 1:
                raise ValueError("Found multiple non related files "
                                 "to render, don't know what to do "
                                 "with them.")
            col = rem[0]
            ext = os.path.splitext(col)[1].lstrip(".")
        else:
            # but we really expect only one collection.
            # Nothing else make sense.
            if len(cols) != 1:
                raise ValueError("Only one image sequence type is expected.")  # noqa: E501
            ext = cols[0].tail.lstrip(".")
            col = list(cols[0])

        # create subset name `familyTaskSubset_AOV`
        # TODO refactor/remove me
        # product_type = skeleton["productType"]
        # if not s_product_name.startswith(product_type):
        #     group_name = '{}{}{}{}{}'.format(
        #         product_type,
        #         task[0].upper(), task[1:],
        #         s_product_name[0].upper(), s_product_name[1:])
        group_name = s_product_name

        # if there are multiple cameras, we need to add camera name
        expected_filepath = col[0] if isinstance(col, (list, tuple)) else col
        cams = [cam for cam in cameras if cam in expected_filepath]
        if cams:
            for cam in cams:
                if not aov:
                    product_name = '{}_{}'.format(group_name, cam)
                elif not aov.startswith(cam):
                    product_name = '{}_{}_{}'.format(group_name, cam, aov)
                else:
                    product_name = "{}_{}".format(group_name, aov)
        else:
            if aov:
                product_name = '{}_{}'.format(group_name, aov)
            else:
                product_name = '{}'.format(group_name)

        if isinstance(col, (list, tuple)):
            staging = os.path.dirname(col[0])
        else:
            staging = os.path.dirname(col)

        try:
            staging = remap_source(staging, anatomy)
        except ValueError as e:
            log.warning(e)

        log.info("Creating data for: {}".format(product_name))

        app = os.environ.get("AYON_HOST_NAME", "")

        if isinstance(col, list):
            render_file_name = os.path.basename(col[0])
        else:
            render_file_name = os.path.basename(col)
        aov_patterns = aov_filter

        preview = match_aov_pattern(app, aov_patterns, render_file_name)
        # toggle preview on if multipart is on
        if instance.data.get("multipartExr"):
            log.debug("Adding preview tag because its multipartExr")
            preview = True

        new_instance = deepcopy(skeleton)
        new_instance["productName"] = product_name
        new_instance["productGroup"] = group_name

        # explicitly disable review by user
        preview = preview and not do_not_add_review
        if preview:
            new_instance["review"] = True

        # create representation
        if isinstance(col, (list, tuple)):
            files = [os.path.basename(f) for f in col]
        else:
            files = os.path.basename(col)

        # Copy render product "colorspace" data to representation.
        colorspace = ""
        products = additional_data["renderProducts"].layer_data.products
        for product in products:
            if product.productName == aov:
                colorspace = product.colorspace
                break

        rep = {
            "name": ext,
            "ext": ext,
            "files": files,
            "frameStart": int(skeleton["frameStartHandle"]),
            "frameEnd": int(skeleton["frameEndHandle"]),
            # If expectedFile are absolute, we need only filenames
            "stagingDir": staging,
            "fps": new_instance.get("fps"),
            "tags": ["review", "shotgridreview"] if preview else [],
            "colorspaceData": {
                "colorspace": colorspace,
                "config": {
                    "path": additional_data["colorspaceConfig"],
                    "template": additional_data["colorspaceTemplate"]
                },
                "display": additional_data["display"],
                "view": additional_data["view"]
            }
        }

        # support conversion from tiled to scanline
        if instance.data.get("convertToScanline"):
            log.info("Adding scanline conversion.")
            rep["tags"].append("toScanline")

        # poor man exclusion
        if ext in skip_integration_repre_list:
            rep["tags"].append("delete")

        if preview:
            new_instance["families"] = _add_review_families(
                new_instance["families"])

        new_instance["representations"] = [rep]

        # if extending frames from existing version, copy files from there
        # into our destination directory
        if new_instance.get("extendFrames", False):
            copy_extend_frames(new_instance, rep)
        instances.append(new_instance)
        log.debug("instances:{}".format(instances))
    return instances


def get_resources(project_name, version_entity, extension=None):
    """Get the files from the specific version.

    This will return all get all files from representation.

    Todo:
        This is really weird function, and it's use is
        highly controversial. First, it will not probably work
        ar all in final release of AYON, second, the logic isn't sound.
        It should try to find representation matching the current one -
        because it is used to pull out files from previous version to
        be included in this one.

    .. deprecated:: 3.15.5
       This won't work in AYON and even the logic must be refactored.

    Args:
        project_name (str): Name of the project.
        version_entity (dict): Version entity.
        extension (str): extension used to filter
            representations.

    Returns:
        list: of files

    """
    warnings.warn((
        "This won't work in AYON and even "
        "the logic must be refactored."), DeprecationWarning)
    extensions = []
    if extension:
        extensions = [extension]

    # there is a `context_filter` argument that won't probably work in
    # final release of AYON. SO we'll rather not use it
    repre_entities = list(ayon_api.get_representations(
        project_name, version_ids={version_entity["id"]}
    ))

    filtered = []
    for repre_entity in repre_entities:
        if repre_entity["context"]["ext"] in extensions:
            filtered.append(repre_entity)

    representation = filtered[0]
    directory = get_representation_path(representation)
    print("Source: ", directory)
    resources = sorted(
        [
            os.path.normpath(os.path.join(directory, file_name))
            for file_name in os.listdir(directory)
        ]
    )

    return resources


def create_skeleton_instance_cache(instance, log=None):
    # type: (pyblish.api.Instance, list, dict) -> dict
    """Create skeleton instance from original instance data.

    This will create dictionary containing skeleton
    - common - data used for publishing rendered instances.
    This skeleton instance is then extended with additional data
    and serialized to be processed by farm job.

    Args:
        instance (pyblish.api.Instance): Original instance to
            be used as a source of data.

    Returns:
        dict: Dictionary with skeleton instance data.

    """
    # list of family names to transfer to new family if present

    context = instance.context
    data = instance.data.copy()
    anatomy = instance.context.data["anatomy"]  # type: Anatomy

    if not log:
        log = Logger.get_logger(__name__)

    # get time related data from instance (or context)
    time_data = get_time_data_from_instance_or_context(instance)

    if data.get("extendFrames", False):
        time_data.start, time_data.end = extend_frames(
            data["folderPath"],
            data["productName"],
            time_data.start,
            time_data.end,
        )

    source = data.get("source") or context.data.get("currentFile")
    success, rootless_path = (
        anatomy.find_root_template_from_path(source)
    )
    if success:
        source = rootless_path
    else:
        # `rootless_path` is not set to `source` if none of roots match
        log.warning(("Could not find root path for remapping \"{}\". "
                     "This may cause issues.").format(source))

    product_type = instance.data["productType"]
    # Make sure "render" is in the families to go through
    # validating expected and rendered files
    # during publishing job.
    families = ["render", product_type]

    instance_skeleton_data = {
        "productName": data["productName"],
        "productType": product_type,
        "family": product_type,
        "families": families,
        "folderPath": data["folderPath"],
        "frameStart": time_data.start,
        "frameEnd": time_data.end,
        "handleStart": time_data.handle_start,
        "handleEnd": time_data.handle_end,
        "frameStartHandle": time_data.start - time_data.handle_start,
        "frameEndHandle": time_data.end + time_data.handle_end,
        "comment": data.get("comment"),
        "fps": time_data.fps,
        "source": source,
        "extendFrames": data.get("extendFrames"),
        "overrideExistingFrame": data.get("overrideExistingFrame"),
        "jobBatchName": data.get("jobBatchName", ""),
        # map inputVersions `ObjectId` -> `str` so json supports it
        "inputVersions": list(map(str, data.get("inputVersions", []))),
    }

    # skip locking version if we are creating v01
    instance_version = data.get("version")  # take this if exists
    if instance_version != 1:
        instance_skeleton_data["version"] = instance_version

    representations = get_transferable_representations(instance)
    instance_skeleton_data["representations"] = representations

    persistent = instance.data.get("stagingDir_persistent") is True
    instance_skeleton_data["stagingDir_persistent"] = persistent

    return instance_skeleton_data


def prepare_cache_representations(skeleton_data, exp_files, anatomy, log=None):
    """Create representations for file sequences.

    This will return representations of expected files if they are not
    in hierarchy of aovs. There should be only one sequence of files for
    most cases, but if not - we create representation from each of them.

    Arguments:
        skeleton_data (dict): instance data for which we are
                         setting representations
        exp_files (list): list of expected files
        anatomy (Anatomy)
    Returns:
        list of representations

    """
    representations = []
    collections, remainders = clique.assemble(exp_files)

    if not log:
        log = Logger.get_logger(__name__)

    # create representation for every collected sequence
    for collection in collections:
        ext = collection.tail.lstrip(".")

        staging = os.path.dirname(list(collection)[0])
        success, rootless_staging_dir = (
            anatomy.find_root_template_from_path(staging)
        )
        if success:
            staging = rootless_staging_dir
        else:
            log.warning((
                "Could not find root path for remapping \"{}\"."
                " This may cause issues on farm."
            ).format(staging))

        frame_start = int(skeleton_data.get("frameStartHandle"))
        rep = {
            "name": ext,
            "ext": ext,
            "files": [os.path.basename(f) for f in list(collection)],
            "frameStart": frame_start,
            "frameEnd": int(skeleton_data.get("frameEndHandle")),
            # If expectedFile are absolute, we need only filenames
            "stagingDir": staging,
            "fps": skeleton_data.get("fps")
        }

        representations.append(rep)

    return representations


def create_instances_for_cache(instance, skeleton, log=None):
    """Create instance for cache.

    This will create new instance for every AOV it can detect in expected
    files list.

    Args:
        instance (pyblish.api.Instance): Original instance.
        skeleton (dict): Skeleton data for instance (those needed) later
            by collector.


    Returns:
        list of instances

    Throws:
        ValueError:

    """
    anatomy = instance.context.data["anatomy"]
    product_name = skeleton["productName"]
    product_type = skeleton["productType"]
    exp_files = instance.data["expectedFiles"]

    if not log:
        log = Logger.get_logger(__name__)

    instances = []
    # go through AOVs in expected files
    for _, files in exp_files[0].items():
        cols, rem = clique.assemble(files)
        # we shouldn't have any reminders. And if we do, it should
        # be just one item for single frame renders.
        if not cols and rem:
            if len(rem) != 1:
                raise ValueError("Found multiple non related files "
                                 "to render, don't know what to do "
                                 "with them.")
            col = rem[0]
            ext = os.path.splitext(col)[1].lstrip(".")
        else:
            # but we really expect only one collection.
            # Nothing else make sense.
            if len(cols) != 1:
                raise ValueError("Only one image sequence type is expected.")  # noqa: E501
            ext = cols[0].tail.lstrip(".")
            col = list(cols[0])

        if isinstance(col, (list, tuple)):
            staging = os.path.dirname(col[0])
        else:
            staging = os.path.dirname(col)

        try:
            staging = remap_source(staging, anatomy)
        except ValueError as e:
            log.warning(e)

        new_instance = deepcopy(skeleton)

        new_instance["productName"] = product_name
        log.info("Creating data for: {}".format(product_name))
        new_instance["productType"] = product_type
        new_instance["families"] = skeleton["families"]
        # create representation
        if isinstance(col, (list, tuple)):
            files = [os.path.basename(f) for f in col]
        else:
            files = os.path.basename(col)

        rep = {
            "name": ext,
            "ext": ext,
            "files": files,
            "frameStart": int(skeleton["frameStartHandle"]),
            "frameEnd": int(skeleton["frameEndHandle"]),
            # If expectedFile are absolute, we need only filenames
            "stagingDir": staging,
            "fps": new_instance.get("fps"),
            "tags": [],
        }

        new_instance["representations"] = [rep]

        # if extending frames from existing version, copy files from there
        # into our destination directory
        if new_instance.get("extendFrames", False):
            copy_extend_frames(new_instance, rep)
        instances.append(new_instance)
        log.debug("instances:{}".format(instances))
    return instances


def copy_extend_frames(instance, representation):
    """Copy existing frames from latest version.

    This will copy all existing frames from product's latest version back
    to render directory and rename them to what renderer is expecting.

    Arguments:
        instance (pyblish.plugin.Instance): instance to get required
            data from
        representation (dict): presentation to operate on

    """
    import speedcopy

    R_FRAME_NUMBER = re.compile(
        r".+\.(?P<frame>[0-9]+)\..+")

    log = Logger.get_logger("farm_publishing")
    log.info("Preparing to copy ...")
    start = instance.data.get("frameStart")
    end = instance.data.get("frameEnd")
    project_name = instance.context.data["project"]
    anatomy = instance.context.data["anatomy"]  # type: Anatomy

    folder_entity = ayon_api.get_folder_by_path(
        project_name, instance.data.get("folderPath")
    )

    # get latest version of product
    # this will stop if product wasn't published yet

    version_entity = ayon_api.get_last_version_by_product_name(
        project_name,
        instance.data.get("productName"),
        folder_entity["id"]
    )

    # get its files based on extension
    product_resources = get_resources(
        project_name, version_entity, representation.get("ext")
    )
    r_col, _ = clique.assemble(product_resources)

    # if override remove all frames we are expecting to be rendered,
    # so we'll copy only those missing from current render
    if instance.data.get("overrideExistingFrame"):
        for frame in range(start, end + 1):
            if frame not in r_col.indexes:
                continue
            r_col.indexes.remove(frame)

    # now we need to translate published names from representation
    # back. This is tricky, right now we'll just use same naming
    # and only switch frame numbers
    resource_files = []
    r_filename = os.path.basename(
        representation.get("files")[0])  # first file
    op = re.search(R_FRAME_NUMBER, r_filename)
    pre = r_filename[:op.start("frame")]
    post = r_filename[op.end("frame"):]
    assert op is not None, "padding string wasn't found"
    for frame in list(r_col):
        fn = re.search(R_FRAME_NUMBER, frame)
        # silencing linter as we need to compare to True, not to
        # type
        assert fn is not None, "padding string wasn't found"
        # list of tuples (source, destination)
        staging = representation.get("stagingDir")
        staging = anatomy.fill_root(staging)
        resource_files.append(
            (frame, os.path.join(
                staging, "{}{}{}".format(pre, fn["frame"], post)))
        )

    # test if destination dir exists and create it if not
    output_dir = os.path.dirname(representation.get("files")[0])
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    # copy files
    for source in resource_files:
        speedcopy.copy(source[0], source[1])
        log.info("  > {}".format(source[1]))

    log.info("Finished copying %i files" % len(resource_files))


def attach_instances_to_product(attach_to, instances):
    """Attach instance to product.

    If we are attaching to other products, create copy of existing
    instances, change data to match its product and replace
    existing instances with modified data.

    Args:
        attach_to (list): List of instances to attach to.
        instances (list): List of instances to attach.

    Returns:
          list: List of attached instances.

    """
    new_instances = []
    for attach_instance in attach_to:
        for i in instances:
            new_inst = copy.deepcopy(i)
            new_inst["version"] = attach_instance.get("version")
            new_inst["productName"] = attach_instance.get("productName")
            new_inst["productType"] = attach_instance.get("productType")
            new_inst["family"] = attach_instance.get("family")
            new_inst["append"] = True
            # don't set productGroup if we are attaching
            new_inst.pop("productGroup")
            new_instances.append(new_inst)
    return new_instances


def create_metadata_path(instance, anatomy, log=None):
    ins_data = instance.data
    # Ensure output dir exists
    output_dir = ins_data.get(
        "publishRenderMetadataFolder", ins_data["outputDir"])

    if not log:
        log = Logger.get_logger("farm_publishing")

    try:
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)
    except OSError:
        # directory is not available
        log.warning("Path is unreachable: `{}`".format(output_dir))

    ### Starts Alkemy-X Override ###
    # Prefixing metadata file with timestamp and asset so the .json files are
    # unique and not overwrite each other. This is necessary because in Hiero
    # we use the same working directory to publish multiple products at once
    # and when the product was called the same, it was overwriting the same file
    # over and over
    metadata_filename = "{}_{}_{}_metadata.json".format(
        datetime.datetime.now().strftime("%d%m%Y%H%M%S"),
        ins_data["anatomyData"]["folder"]["name"],
        ins_data["productName"]
    )
    ### Ends Alkemy-X Override ###

    metadata_path = os.path.join(output_dir, metadata_filename)

    # Convert output dir to `{root}/rest/of/path/...` with Anatomy
    success, rootless_mtdt_p = anatomy.find_root_template_from_path(
        metadata_path)
    if not success:
        # `rootless_path` is not set to `output_dir` if none of roots match
        log.warning((
            "Could not find root path for remapping \"{}\"."
            " This may cause issues on farm."
        ).format(output_dir))
        rootless_mtdt_p = metadata_path

    return metadata_path, rootless_mtdt_p
