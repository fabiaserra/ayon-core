"""Module for handling OP delivery of Shotgrid playlists"""
import copy
import collections
import click

from ayon_core.lib import Logger, collect_frames, get_datetime_data
from ayon_core.pipeline import Anatomy
from ayon_core.pipeline.load import get_representation_path_with_anatomy
from ayon_core.pipeline.delivery import (
    check_destination_path,
    deliver_single_file,
)
from ayon_shotgrid.lib import credentials


logger = Logger.get_logger(__name__)


def deliver_playlist_id(
    playlist_id,
    representation_names=None,
):
    """Given a SG playlist id, deliver all the versions associated to it.

    Args:
        playlist_id (int): Shotgrid playlist id to deliver.
        representation_names (list): List of representation names to deliver. If not
            given, it will just deliver all the representations that exist for the subset.

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the delivery was successful.
    """
    report_items = collections.defaultdict(list)

    sg = credentials.get_shotgrid_session()

    sg_playlist = sg.find_one(
        "Playlist",
        [
            ["id", "is", int(playlist_id)],
        ],
        ["project", "code"],
    )

    # Get the project name associated with the selected entities
    project_name = sg_playlist["project"]["name"]

    project_doc = ayon_api.get_project(project_name, fields=["name"])
    if not project_doc:
        return report_items[f"Didn't find project '{project_name}' in avalon."], False

    # Get the name of the playlist to use it as the name of the package to place all deliveries
    package_name = sg_playlist.get("code")

    # Get all the SG versions associated to the playlist
    sg_versions = sg.find(
        "Version",
        [["playlists", "in", sg_playlist]],
        ["sg_op_instance_id", "entity", "code"],
    )

    # Iterate over each SG version and deliver it
    success = True
    for sg_version in sg_versions:
        new_report_items, new_success = deliver_version(
            sg_version,
            project_name,
            report_items,
            representation_names,
            package_name
        )
        if new_report_items:
            report_items.update(new_report_items)

        if not new_success:
            success = False

    click.echo(report_items)
    return report_items, success


def deliver_version_id(
    version_id,
    representation_names=None,
):
    """Util function to deliver a single SG version given its id.

    Args:
        version_id (str): Shotgrid Version id to deliver.
        representation_names (list): List of representation names to deliver. If not
            given, it will just deliver all the representations that exist for the product.

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the delivery was successful.
    """
    report_items = collections.defaultdict(list)

    sg = credentials.get_shotgrid_session()

    # Get all the SG versions associated to the playlist
    sg_version = sg.find_one(
        "Version",
        [["id", "is", int(version_id)]],
        ["sg_op_instance_id", "entity", "code", "project"],
    )

    if not sg_version:
        report_items["SG Version not found"].append(version_id)
        return report_items, False

    return deliver_version(
        sg_version,
        sg_version["project"]["name"],
        report_items,
        representation_names,
    )


def deliver_version(
    sg_version,
    project_name,
    report_items,
    representation_names=None,
    package_name=None,
):
    """Deliver a single SG version.

    Args:
        sg_version (): Shotgrid Version object to deliver.
        project_name (str): Name of the project corresponding to the version being
            delivered.
        report_items (dict): Dictionary with the messages to show in the
            report.
        representation_names (list): List of representation names to deliver.
        package_name (str): Name of the package to place the delivered versions in.

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the delivery was successful.
    """
    # Grab the OP's id corresponding to the SG version
    op_version_id = sg_version["sg_op_instance_id"]
    if not op_version_id or op_version_id == "-":
        sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
        msg = "Missing 'sg_op_instance_id' field on SG Versions"
        report_items[msg].append(sub_msg)
        logger.error("%s: %s", msg, sub_msg)
        return report_items, False

    anatomy = Anatomy(project_name)

    if not representation_names:
        msg = "No representation names specified"
        sub_msg = "All representations will be delivered."
        logger.info("%s: %s", msg, sub_msg)
        report_items[msg] = [sub_msg]

    success = True

    # Find the OP representations we want to deliver
    repres_to_deliver = list(
        ayon_api.get_representations(
            project_name,
            representation_names=representation_names,
            version_ids=[op_version_id],
        )
    )
    if not repres_to_deliver:
        sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
        msg = "None of the representations requested found on SG Versions"
        report_items[msg].append(sub_msg)
        logger.error("%s: %s", msg, sub_msg)
        return report_items, False

    for repre in repres_to_deliver:
        source_path = repre.get("data", {}).get("path")
        debug_msg = "Processing representation {}".format(repre["id"])
        if source_path:
            debug_msg += " with published path {}.".format(source_path)
        click.echo(debug_msg)

        # Get source repre path
        frame = repre["context"].get("frame")

        if frame:
            repre["context"]["frame"] = len(str(frame)) * "#"

        delivery_template_name = None
        if frame:
            delivery_template_name = "sequence"
        else:
            delivery_template_name = "single_file"

        anatomy_data = copy.deepcopy(repre["context"])
        logger.debug("Anatomy data: %s" % anatomy_data)

        # Add package_name to anatomy data
        if not package_name:
            package_name = "sg_versions"
        anatomy_data["package_name"] = package_name

        repre_report_items, dest_path = check_destination_path(
            repre["id"],
            anatomy,
            anatomy_data,
            get_datetime_data(),
            delivery_template_name,
            return_dest_path=True,
        )

        if repre_report_items:
            report_items.update(repre_report_items)
            success = False
            continue

        repre_path = get_representation_path_with_anatomy(repre, anatomy)

        args = [
            repre_path,
            repre,
            anatomy,
            delivery_template_name,
            anatomy_data,
            None,
            report_items,
            logger,
        ]
        src_paths = []
        for repre_file in repre["files"]:
            src_path = anatomy.fill_root(repre_file["path"])
            src_paths.append(src_path)

        sources_and_frames = collect_frames(src_paths)
        for src_path, frame in sources_and_frames.items():
            args[0] = src_path
            if frame:
                anatomy_data["frame"] = frame
            new_report_items, new_success = deliver_single_file(*args)
            # If not new report items it means the delivery was successful
            # so we append it to the list of successful delivers
            if new_report_items:
                report_items.update(new_report_items)

            if not new_success:
                success = False
            else:
                sub_msg = f"{repre_path} -> {dest_path}<br>"
                report_items["Successful delivered representations"].append(sub_msg)

    return report_items, success


######################################################################################
##################################### DEPRECATED #####################################
######################################################################################


# Deprecated code but keeping it as it can be useful in the future for reference
# in case we need to republish versions or other debugging workflows

# def republish_playlist_id(
#     playlist_id, delivery_types, representation_names=None, force=False
# ):
#     """Given a SG playlist id, deliver all the versions associated to it.

#     Args:
#         playlist_id (int): Shotgrid playlist id to republish.
#         delivery_types (list[str]): What type(s) of delivery it is
#             (i.e., ["final", "review"])
#         representation_names (list): List of representation names that should exist on
#             the representations being published.
#         force (bool): Whether to force the creation of the delivery representations or not.

#     Returns:
#         tuple: A tuple containing a dictionary of report items and a boolean indicating
#             whether the republish was successful.
#     """
#     report_items = collections.defaultdict(list)

#     sg = credentials.get_shotgrid_session()

#     sg_playlist = sg.find_one(
#         "Playlist",
#         [
#             ["id", "is", int(playlist_id)],
#         ],
#         ["project"],
#     )

#     # Get the project name associated with the selected entities
#     project_name = sg_playlist["project"]["name"]

#     project_doc = ayon_api.get_project(project_name, fields=["name"])
#     if not project_doc:
#         return report_items[f"Didn't find project '{project_name}' in avalon."], False

#     # Get all the SG versions associated to the playlist
#     sg_versions = sg.find(
#         "Version",
#         [["playlists", "in", sg_playlist]],
#         ["project", "code", "entity", "sg_op_instance_id"],
#     )

#     success = True
#     for sg_version in sg_versions:
#         new_report_items, new_success = republish_version(
#             sg_version,
#             project_name,
#             delivery_types,
#             representation_names,
#             force,
#         )
#         if new_report_items:
#             report_items.update(new_report_items)

#         if not new_success:
#             success = False

#     click.echo(report_items)
#     return report_items, success


# def republish_version_id(
#     version_id,
#     delivery_types,
#     representation_names=None,
#     force=False,
# ):
#     """Given a SG version id, republish it so it triggers the OP publish pipeline again.

#     Args:
#         version_id (int): Shotgrid version id to republish.
#         delivery_types (list[str]): What type(s) of delivery it is so we
#             regenerate those representations.
#         representation_names (list): List of representation names that should exist on
#             the representations being published.
#         force (bool): Whether to force the creation of the delivery representations or not.

#     Returns:
#         tuple: A tuple containing a dictionary of report items and a boolean indicating
#             whether the republish was successful.
#     """
#     sg = credentials.get_shotgrid_session()

#     sg_version = sg.find_one(
#         "Version",
#         [
#             ["id", "is", int(version_id)],
#         ],
#         ["project", "code", "entity", "sg_op_instance_id"],
#     )
#     return republish_version(
#         sg_version,
#         sg_version["project"]["name"],
#         delivery_types,
#         representation_names,
#         force,
#     )


# def republish_version(
#     sg_version, project_name, delivery_types, representation_names=None, force=False
# ):
#     """
#     Republishes the given SG version by creating new review and/or final outputs.

#     Args:
#         sg_version (dict): The Shotgrid version to republish.
#         project_name (str): The name of the Shotgrid project.
#         delivery_types (list[str]): What type(s) of delivery it is
#             (i.e., ["final", "review"])
#         representation_names (list): List of representation names that should exist on
#             the representations being published.
#         force (bool): Whether to force the creation of the delivery representations or
#             not.

#     Returns:
#         tuple: A tuple containing a dictionary of report items and a boolean indicating
#             whether the republish was successful.
#     """
#     report_items = collections.defaultdict(list)

#     # Grab the OP's id corresponding to the SG version
#     op_version_id = sg_version["sg_op_instance_id"]
#     if not op_version_id or op_version_id == "-":
#         msg = "Missing 'sg_op_instance_id' field on SG Versions"
#         sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
#         logger.error("%s: %s", msg, sub_msg)
#         report_items[msg].append(sub_msg)
#         return report_items, False

#     # Get OP version corresponding to the SG version
#     version_doc = get_version_by_id(project_name, op_version_id)
#     if not version_doc:
#         msg = "No OP version found for SG versions"
#         sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
#         logger.error("%s: %s", msg, sub_msg)
#         report_items[msg].append(sub_msg)
#         return report_items, False

#     # Find the OP representations we want to deliver
#     exr_repre_doc = get_representation_by_name(
#         project_name,
#         "exr",
#         version_id=op_version_id,
#     )
#     if not exr_repre_doc:
#         msg = "No 'exr' representation found on SG versions"
#         sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
#         logger.error("%s: %s", msg, sub_msg)
#         report_items[msg].append(sub_msg)
#         return report_items, False

#     # If we are not forcing the creation of representations we validate whether the
#     # representations requested already exist
#     if not force:
#         if not representation_names:
#             sg = credentials.get_shotgrid_session()
#             representation_names, entity = delivery.get_representation_names(
#                 sg, sg_version["id"], "Version", delivery_types
#             )
#             logger.debug(
#                 "%s representation names found at '%s': %s",
#                 sg_version['code'],
#                 entity,
#                 representation_names
#             )

#         representations = ayon_api.get_representations(
#             project_name,
#             version_ids=[op_version_id],
#         )
#         existing_rep_names = {rep["name"] for rep in representations}
#         missing_rep_names = set(representation_names) - existing_rep_names
#         if not missing_rep_names:
#             msg = f"Requested '{delivery_types}' representations already exist"
#             sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
#             report_items[msg].append(sub_msg)
#             logger.info("%s: %s", msg, sub_msg)
#             return report_items, True

#     exr_path = exr_repre_doc["data"]["path"]
#     render_path = os.path.dirname(exr_path)

#     families = version_doc["data"]["families"]
#     families.append("review")

#     # Add family for each delivery type to control which publish plugins
#     # get executed
#     # for delivery_type in delivery_types:
#         # families.append(f"client_{delivery_type}")

#     instance_data = {
#         "project": project_name,
#         "family": exr_repre_doc["context"]["family"],
#         "subset": exr_repre_doc["context"]["subset"],
#         "families": families,
#         "asset": exr_repre_doc["context"]["asset"],
#         "task": exr_repre_doc["context"]["task"]["name"],
#         "frameStart": version_doc["data"]["frameStart"],
#         "frameEnd": version_doc["data"]["frameEnd"],
#         "handleStart": version_doc["data"]["handleStart"],
#         "handleEnd": version_doc["data"]["handleEnd"],
#         "frameStartHandle": int(
#             version_doc["data"]["frameStart"] - version_doc["data"]["handleStart"]
#         ),
#         "frameEndHandle": int(
#             version_doc["data"]["frameEnd"] + version_doc["data"]["handleEnd"]
#         ),
#         "comment": version_doc["data"]["comment"],
#         "fps": version_doc["data"]["fps"],
#         "source": version_doc["data"]["source"],
#         "overrideExistingFrame": False,
#         "jobBatchName": "Republish - {} - {}".format(
#             sg_version["code"],
#             version_doc["name"]
#         ),
#         "useSequenceForReview": True,
#         "colorspace": version_doc["data"].get("colorspace"),
#         "version": version_doc["name"],
#         "outputDir": render_path,
#     }

#     # Inject variables into session
#     legacy_io.Session["AVALON_ASSET"] = instance_data["asset"]
#     legacy_io.Session["AVALON_TASK"] = instance_data.get("task")
#     legacy_io.Session["AVALON_WORKDIR"] = render_path
#     legacy_io.Session["AVALON_PROJECT"] = project_name
#     legacy_io.Session["AVALON_APP"] = "traypublisher"

#     # Replace frame number with #'s for expected_files function
#     hashes_path = re.sub(
#         r"\d+(?=\.\w+$)", lambda m: "#" * len(m.group()) if m.group() else "#", exr_path
#     )

#     expected_files = utils.expected_files(
#         hashes_path,
#         instance_data["frameStartHandle"],
#         instance_data["frameEndHandle"],
#     )
#     logger.debug("__ expectedFiles: `{}`".format(expected_files))

#     representations = utils.ayon_api.get_representations(
#         instance_data,
#         expected_files,
#         add_review=True,
#         publish_to_sg=True,
#     )

#     # inject colorspace data
#     for rep in representations:
#         source_colorspace = instance_data["colorspace"] or "scene_linear"
#         logger.debug("Setting colorspace '%s' to representation", source_colorspace)
#         utils.set_representation_colorspace(
#             rep, project_name, colorspace=source_colorspace
#         )

#     if "representations" not in instance_data.keys():
#         instance_data["representations"] = []

#     # add representation
#     instance_data["representations"] += representations
#     instances = [instance_data]

#     render_job = {}
#     render_job["Props"] = {}
#     # Render job doesn't exist because we do not have prior submission.
#     # We still use data from it so lets fake it.
#     #
#     # Batch name reflect original scene name

#     render_job["Props"]["Batch"] = instance_data.get("jobBatchName")

#     # User is deadline user
#     render_job["Props"]["User"] = getpass.getuser()

#     # get default deadline webservice url from deadline module
#     deadline_url = get_system_settings()["modules"]["deadline"]["deadline_urls"][
#         "default"
#     ]

#     metadata_path = utils.create_metadata_path(instance_data)
#     logger.info("Metadata path: %s", metadata_path)

#     deadline_publish_job_id = utils.submit_deadline_post_job(
#         instance_data, render_job, render_path, deadline_url, metadata_path
#     )

#     report_items["Submitted republish job to Deadline"].append(deadline_publish_job_id)

#     # Inject deadline url to instances.
#     for inst in instances:
#         inst["deadlineUrl"] = deadline_url

#     # publish job file
#     publish_job = {
#         "asset": instance_data["asset"],
#         "frameStart": instance_data["frameStartHandle"],
#         "frameEnd": instance_data["frameEndHandle"],
#         "fps": instance_data["fps"],
#         "source": instance_data["source"],
#         "user": getpass.getuser(),
#         "version": None,  # this is workfile version
#         "intent": None,
#         "comment": instance_data["comment"],
#         "job": render_job or None,
#         "session": legacy_io.Session.copy(),
#         "instances": instances,
#     }

#     if deadline_publish_job_id:
#         publish_job["deadline_publish_job_id"] = deadline_publish_job_id

#     logger.info("Writing json file: {}".format(metadata_path))
#     with open(metadata_path, "w") as f:
#         json.dump(publish_job, f, indent=4, sort_keys=True)

#     # sg = credentials.get_shotgrid_session()
#     # neat_vid_reformat_tag = {"id": 6211, "name": "neat_vid_reformat", "type": "Tag"}
#     # sg.update("Version", sg_version["id"], {"tags": [neat_vid_reformat_tag]})

#     click.echo(report_items)
#     return report_items, True


# def generate_delivery_media_playlist_id(
#     playlist_id,
#     delivery_types,
#     representation_names=None,
#     force=False,
#     description=None,
#     override_version=None,
# ):
#     """Given a SG playlist id, deliver all the versions associated to it.

#     Args:
#         playlist_id (int): Shotgrid playlist id to republish.
#         delivery_types (list[str]): What type(s) of delivery it is
#             (i.e., ["final", "review"])
#         representation_names (list): List of representation names that should exist on
#             the representations being published.
#         force (bool): Whether to force the creation of the delivery representations or not.

#     Returns:
#         tuple: A tuple containing a dictionary of report items and a boolean indicating
#             whether the republish was successful.
#     """
#     report_items = collections.defaultdict(list)

#     sg = credentials.get_shotgrid_session()

#     sg_playlist = sg.find_one(
#         "Playlist",
#         [
#             ["id", "is", int(playlist_id)],
#         ],
#         ["project"],
#     )

#     # Get the project name associated with the selected entities
#     project_name = sg_playlist["project"]["name"]

#     project_doc = ayon_api.get_project(project_name, fields=["name"])
#     if not project_doc:
#         return report_items[f"Didn't find project '{project_name}' in avalon."], False

#     # Get all the SG versions associated to the playlist
#     sg_versions = sg.find(
#         "Version",
#         [["playlists", "in", sg_playlist]],
#         ["project", "code", "entity", "sg_op_instance_id"],
#     )

#     success = True
#     for sg_version in sg_versions:
#         new_report_items, new_success = generate_delivery_media_version(
#             sg_version,
#             project_name,
#             delivery_types,
#             representation_names,
#             force,
#             description,
#             override_version,
#         )
#         if new_report_items:
#             report_items.update(new_report_items)

#         if not new_success:
#             success = False

#     click.echo(report_items)
#     return report_items, success


# def generate_delivery_media_version_id(
#     version_id,
#     delivery_types,
#     representation_names=None,
#     force=False,
#     description=None,
#     override_version=None,
# ):
#     """Given a SG version id, generate its corresponding delivery so it
#         triggers the OP publish pipeline again.

#     Args:
#         version_id (int): Shotgrid version id to republish.
#         delivery_types (list[str]): What type(s) of delivery it is so we
#             regenerate those representations.
#         representation_names (list): List of representation names that should exist on
#             the representations being published.
#         force (bool): Whether to force the creation of the delivery representations or not.

#     Returns:
#         tuple: A tuple containing a dictionary of report items and a boolean indicating
#             whether the republish was successful.
#     """
#     sg = credentials.get_shotgrid_session()

#     sg_version = sg.find_one(
#         "Version",
#         [
#             ["id", "is", int(version_id)],
#         ],
#         ["project", "code", "entity", "sg_op_instance_id"],
#     )
#     return generate_delivery_media_version(
#         sg_version,
#         sg_version["project"]["name"],
#         delivery_types,
#         representation_names,
#         force,
#         description,
#         override_version,
#     )


# def generate_delivery_media_version(
#     sg_version,
#     project_name,
#     delivery_types,
#     representation_names=None,
#     force=False,
#     description=None,
#     override_version=None,
# ):
#     """
#     Generate the corresponding delivery version given SG version by creating a new
#         subset with review and/or final outputs.

#     Args:
#         sg_version (dict): The Shotgrid version to republish.
#         project_name (str): The name of the Shotgrid project.
#         delivery_types (list[str]): What type(s) of delivery it is
#             (i.e., ["final", "review"])
#         representation_names (list): List of representation names that should exist on
#             the representations being published.
#         force (bool): Whether to force the creation of the delivery representations or
#             not.

#     Returns:
#         tuple: A tuple containing a dictionary of report items and a boolean indicating
#             whether the republish was successful.
#     """
#     report_items = collections.defaultdict(list)

#     # Grab the OP's id corresponding to the SG version
#     op_version_id = sg_version["sg_op_instance_id"]
#     if not op_version_id or op_version_id == "-":
#         msg = "Missing 'sg_op_instance_id' field on SG Versions"
#         sub_msg = f"{project_name} - {sg_version['code']} - id: {sg_version['id']}<br>"
#         logger.error("%s: %s", msg, sub_msg)
#         report_items[msg].append(sub_msg)
#         return report_items, False

#     # Get OP version corresponding to the SG version
#     version_doc = get_version_by_id(project_name, op_version_id)
#     if not version_doc:
#         msg = "No OP version found for SG versions"
#         sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
#         logger.error("%s: %s", msg, sub_msg)
#         report_items[msg].append(sub_msg)
#         return report_items, False

#     # Find the OP representations we want to deliver
#     exr_repre_doc = get_representation_by_name(
#         project_name,
#         "exr",
#         version_id=op_version_id,
#     )
#     if not exr_repre_doc:
#         msg = "No 'exr' representation found on SG versions"
#         sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
#         logger.error("%s: %s", msg, sub_msg)
#         report_items[msg].append(sub_msg)
#         return report_items, False

#     # Query subset of the version so we can construct its equivalent delivery
#     # subset
#     subset_doc = get_subset_by_id(project_name, version_doc["parent"], fields=["name"])

#     delivery_subset_name = "delivery_{}".format(subset_doc["name"])
#     if description:
#         delivery_subset_name = "{}_{}".format(
#             delivery_subset_name, description
#         )

#     # If we are not forcing the creation of representations we validate whether
#     # the representations requested already exist
#     if not force:
#         if not representation_names:
#             sg = credentials.get_shotgrid_session()
#             representation_names, entity = delivery.get_representation_names(
#                 sg, sg_version["id"], "Version", delivery_types
#             )
#             logger.debug(
#                 "%s representation names found at '%s': %s",
#                 sg_version['code'],
#                 entity,
#                 representation_names
#             )

#         last_delivery_version = get_last_version_by_subset_name(
#             project_name,
#             delivery_subset_name
#         )
#         if last_delivery_version:
#             representations = ayon_api.get_representations(
#                 project_name,
#                 version_ids=[last_delivery_version["id"]],
#             )
#         else:
#             representations = []

#         existing_rep_names = {rep["name"] for rep in representations}
#         missing_rep_names = set(representation_names) - existing_rep_names
#         if not missing_rep_names:
#             msg = f"Requested '{delivery_types}' representations already exist"
#             sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
#             report_items[msg].append(sub_msg)
#             logger.info("%s: %s", msg, sub_msg)
#             return report_items, True

#     # Add family for each delivery type to control which publish plugins
#     # get executed
#     families = []
#     for delivery_type in delivery_types:
#         families.append(f"client_{delivery_type}")

#     frame_start_handle = int(
#         version_doc["data"]["frameStart"] - version_doc["data"]["handleStart"]
#     )
#     frame_end_handle = int(
#         version_doc["data"]["frameEnd"] + version_doc["data"]["handleEnd"]
#     )
#     logger.debug("Frame start handle: %s", frame_start_handle)
#     logger.debug("Frame end handle: %s", frame_end_handle)

#     instance_data = {
#         "project": project_name,
#         "family": exr_repre_doc["context"]["family"],
#         "subset": delivery_subset_name,
#         "families": families,
#         "asset": exr_repre_doc["context"]["asset"],
#         "task": exr_repre_doc["context"]["task"]["name"],
#         "frameStart": version_doc["data"]["frameStart"],
#         "frameEnd": version_doc["data"]["frameEnd"],
#         "handleStart": version_doc["data"]["handleStart"],
#         "handleEnd": version_doc["data"]["handleEnd"],
#         "frameStartHandle": frame_start_handle,
#         "frameEndHandle": frame_end_handle,
#         "comment": version_doc["data"]["comment"],
#         "fps": version_doc["data"]["fps"],
#         "source": version_doc["data"]["source"],
#         "overrideExistingFrame": False,
#         "jobBatchName": "Generate delivery media - {} - {}".format(
#             sg_version["code"],
#             delivery_subset_name
#         ),
#         "useSequenceForReview": True,
#         "colorspace": version_doc["data"].get("colorspace"),
#         "customData": {"description": description}
#     }

#     # Find the OP representations we want to deliver
#     thumbnail_repre_doc = get_representation_by_name(
#         project_name,
#         "thumbnail",
#         version_id=op_version_id,
#     )
#     if not thumbnail_repre_doc:
#         msg = "No 'thumbnail' representation found on SG versions"
#         sub_msg = f"{sg_version['code']} - id: {sg_version['id']}<br>"
#         logger.error("%s: %s", msg, sub_msg)
#         report_items[msg].append(sub_msg)
#         return report_items, False

#     instance_data["thumbnailSource"] = thumbnail_repre_doc["data"]["path"]

#     # If we are specifying the version to generate we set it on the instance
#     if override_version:
#         instance_data["version"] = override_version

#     # Copy source files from original version to a temporary location which will be used
#     # for staging
#     exr_path = exr_repre_doc["data"]["path"]
#     # Replace frame number with #'s for expected_files function
#     hashes_path = re.sub(
#         r"\d+(?=\.\w+$)", lambda m: "#" * len(m.group()) if m.group() else "#", exr_path
#     )
#     expected_files = utils.expected_files(
#         hashes_path,
#         frame_start_handle,
#         frame_end_handle,
#     )
#     logger.debug("__ Source expectedFiles: `{}`".format(expected_files))

#     # Inject variables into session
#     legacy_io.Session["AVALON_ASSET"] = instance_data["asset"]
#     legacy_io.Session["AVALON_TASK"] = instance_data.get("task")
#     legacy_io.Session["AVALON_PROJECT"] = project_name
#     legacy_io.Session["AVALON_APP"] = "traypublisher"

#     # Calculate temporary directory where we will copy the source files to
#     # and use as the delivery media staging directory while publishing
#     temp_delivery_dir = os.path.join(
#         context_tools.get_workdir_from_session(), "temp_delivery"
#     )
#     legacy_io.Session["AVALON_WORKDIR"] = temp_delivery_dir
#     # Set outputDir on instance data as that's used to define where
#     # to save the metadata path
#     instance_data["outputDir"] = temp_delivery_dir

#     logger.debug("__ expectedFiles: `{}`".format(expected_files))

#     representations = utils.ayon_api.get_representations(
#         instance_data,
#         expected_files,
#         add_review=False,
#     )

#     # inject colorspace data
#     for rep in representations:
#         source_colorspace = instance_data["colorspace"] or "scene_linear"
#         logger.debug("Setting colorspace '%s' to representation", source_colorspace)
#         utils.set_representation_colorspace(
#             rep, project_name, colorspace=source_colorspace
#         )

#     if "representations" not in instance_data.keys():
#         instance_data["representations"] = []

#     # add representation
#     instance_data["representations"] += representations
#     instances = [instance_data]

#     render_job = {}
#     render_job["Props"] = {}
#     # Render job doesn't exist because we do not have prior submission.
#     # We still use data from it so lets fake it.
#     #
#     # Batch name reflect original scene name

#     render_job["Props"]["Batch"] = instance_data.get("jobBatchName")

#     # User is deadline user
#     render_job["Props"]["User"] = getpass.getuser()

#     # get default deadline webservice url from deadline module
#     deadline_url = get_system_settings()["modules"]["deadline"]["deadline_urls"][
#         "default"
#     ]

#     metadata_path = utils.create_metadata_path(instance_data)
#     logger.info("Metadata path: %s", metadata_path)

#     deadline_publish_job_id = utils.submit_deadline_post_job(
#         instance_data, render_job, temp_delivery_dir, deadline_url, metadata_path
#     )

#     report_items["Submitted generate delivery media job to Deadline"].append(
#         deadline_publish_job_id
#     )

#     # Inject deadline url to instances.
#     for inst in instances:
#         inst["deadlineUrl"] = deadline_url

#     # publish job file
#     publish_job = {
#         "asset": instance_data["asset"],
#         "frameStart": instance_data["frameStartHandle"],
#         "frameEnd": instance_data["frameEndHandle"],
#         "fps": instance_data["fps"],
#         "source": instance_data["source"],
#         "user": getpass.getuser(),
#         "version": None,  # this is workfile version
#         "intent": None,
#         "comment": instance_data["comment"],
#         "job": render_job or None,
#         "session": legacy_io.Session.copy(),
#         "instances": instances,
#     }

#     if deadline_publish_job_id:
#         publish_job["deadline_publish_job_id"] = deadline_publish_job_id

#     logger.info("Writing json file: {}".format(metadata_path))
#     with open(metadata_path, "w") as f:
#         json.dump(publish_job, f, indent=4, sort_keys=True)

#     click.echo(report_items)
#     return report_items, True
