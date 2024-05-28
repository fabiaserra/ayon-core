"""
Main cleaner module. Includes functions for active project cleaning and archiving.
"""
import clique
import collections
import glob
import logging
import os
import pathlib
import re
import shutil
import time
import pprint
import pandas as pd

from ast import literal_eval
from datetime import datetime, timedelta

import ayon_api
from ayon_api.operations import (
    OperationsSession
)
from ayon_core.lib import Logger, run_subprocess, path_tools
from ayon_core.pipeline import Anatomy
from ayon_core.modules.delivery.scripts import media

from ayon_shotgrid.lib import credentials

from . import utils
from . import const

# Thresholds to warn about files that are older than this time to be marked for deletion
# lower numbers is less caution, higher numbers for files we want to be more careful about
# deleting
WARNING_THRESHOLDS = {
    0: datetime.today() - timedelta(days=2),
    1: datetime.today() - timedelta(days=3),
    2: datetime.today() - timedelta(days=8),
}

# Thresholds to keep files marked for deletion before they get deleted
# lower numbers is less caution, higher numbers for files we want to be more careful about
# deleting
DELETE_THRESHOLDS = {
    0: timedelta(days=2),
    1: timedelta(days=5),
    2: timedelta(days=10)
}

# Prefix to use for files that are marked for deletion
DELETE_PREFIX = "__DELETE__"

# Format to use for the date in the delete prefix
DATE_FORMAT = "%Y-%m-%d"

# Object that holds the current time
TIME_NOW = datetime.today()

# String that represents the current time
TIME_NOW_STR = TIME_NOW.strftime(DATE_FORMAT)

# Prefix to use for files that are marked for deletion with the current time
TIME_DELETE_PREFIX = f"{DELETE_PREFIX}({TIME_NOW_STR})"

# Regular expression used to remove the delete prefix from a path
DELETE_PREFIX_RE = re.compile(rf"{DELETE_PREFIX}\(.*\)")

# Set of file patterns to delete if we find them in the project and they are
# older than a certain time
TEMP_FILE_PATTERNS = {
    re.compile(r".*\.nk~$"),
    re.compile(r".*\.nk\.autosave\d*$"),
    re.compile(r".*_auto\d+\.hip$"),
    re.compile(r".*_bak\d+\.hip$"),
    re.compile(r".*\.hrox\.autosave$"),
    re.compile(r".*_metadata.json"),
    re.compile(r".*_exr_h264.mov"),
}

# Keywords to ignore when walking into directories to avoid deleting its versions
PROTECTED_OLD_VERSIONS = {
    "comp",
    "paint"
}

# Protected directories we never want to delete
PROTECTED_PATHS = {
    "/proj/{proj_code}/shots/_2d_shot",
    "/proj/{proj_code}/shots/_3d_shot",
    "/proj/{proj_code}/shots/_edit_shot",
}

logger = Logger.get_logger(__name__)


class ArchiveProject:

    def __init__(self, proj_code) -> None:

        self.sg = credentials.get_shotgrid_session()
        self.proj_code = proj_code

        sg_project = self.sg.find_one(
            "Project", [["sg_code", "is", proj_code]], ["name"]
        )
        if not sg_project:
            msg = f"SG Project with code '{proj_code}' not found, can't proceed"
            logger.error(msg)
            raise ValueError(msg)

        self.project_name = sg_project["name"]
        try:
            self.anatomy = Anatomy(self.project_name)
        except TypeError:
            self.anatomy = None
            pass

        self.target_root = os.path.join(const.PROJECTS_DIR, proj_code)

        self.summary_dir = os.path.join(self.target_root, "archive_logs")
        if not os.path.exists(self.summary_dir):
            os.makedirs(self.summary_dir)

        timestamp = time.strftime("%Y%m%d%H%M")
        self.summary_file = os.path.join(self.summary_dir, f"{timestamp}{'_debug' if const._debug else ''}.txt")

        self.delete_data_file = os.path.join(
            self.summary_dir, f"delete_data{'_debug' if const._debug else ''}.csv"
        )

        # Populate the self.archive_entries with the existing CSV document
        # in the project if it exists
        self.read_archive_data()

        # File to store entries that we want to protect and never delete
        self.protected_data_file = os.path.join(
            self.summary_dir, f"protected_data{'_debug' if const._debug else ''}.csv"
        )
        self.read_protected_data()

        self.total_size_deleted = 0

    def clean(self, archive=False):
        """Performs a routine cleaning of an active project"""
        # Create a file handler which logs the execution of this function
        file_handler = logging.FileHandler(self.summary_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )

        logger.addHandler(file_handler)

        logger.info(
            "======= Cleaning project '%s' (%s) ======= \n\n",
            self.project_name,
            self.proj_code,
        )
        start_time = time.time()

        # Comment out since it actually takes longer to pre-process
        # the existing entries to try delete them early than letting
        # the other functions discover the files again at this point
        # NOTE: this could change in the future once this script
        # runs daily and the archive is up to date
        # self.clean_existing_entries()

        # Delete files based on shot status in SG
        shots_status = self.get_shotgrid_data()
        self.clean_shots_by_status(shots_status, archive=archive)

        keep_versions = 5
        if archive:
            keep_versions = 3
        self.clean_old_versions(keep_versions, archive=archive)
        self.clean_work_files(archive=archive)
        self.clean_io_files(archive=archive)
        # Only try to clean published OP files if the project exists
        if self.anatomy:
            self.clean_published_file_sources(archive=archive)

        # Package the work directories so I/O is faster when treating a lot
        # of files as a single one
        # NOTE: removed for now as we aren't sure if the benefits are worth
        # the time it takes to package (and potentially unpackage) the files
        # if archive:
        #     self.package_workfiles()

        elapsed_time = time.time() - start_time
        logger.info("\n\nMore logging details at '%s'", self.summary_file)
        logger.info("Clean Time: %s", utils.time_elapsed(elapsed_time))
        logger.info(
            "Deleted %s", utils.format_bytes(self.total_size_deleted)
        )

        self.write_archive_data()
        self.write_protected_data()

        logger.removeHandler(file_handler)

    def purge(self):
        """
        Performs a deep cleaning of the project and preps if for archival by deleting
        all the unnecessary files and compressing the work directories. This should only
        be executed after a project has been finaled and no one is actively working on
        it.
        """
        self.clean(archive=True)

    def read_archive_data(self):
        """Read the archive data from the CSV file in the project as a dictionary"""
        self.archive_entries = {}

        if not os.path.exists(self.delete_data_file):
            logger.info(f"CSV file '{self.delete_data_file}' does not exist yet")
            return

        data_frame = pd.read_csv(self.delete_data_file)
        non_deleted_data = data_frame[~data_frame["is_deleted"]]
        data_list = non_deleted_data.to_dict(orient="records")
        for data_entry in data_list:
            data_entry["marked_time"] = pd.to_datetime(data_entry["marked_time"])
            data_entry["delete_time"] = pd.to_datetime(data_entry["delete_time"])
            data_entry["paths"] = literal_eval(data_entry["paths"])
            self.archive_entries[data_entry.pop("path")] = data_entry

    def read_protected_data(self):
        """Read the protected data from the CSV file in the project as a dictionary"""
        self.protected_entries = set()

        if not os.path.exists(self.protected_data_file):
            logger.info(f"CSV file '{self.protected_data_file}' does not exist yet")
            # Make sure we add the starting protected paths if the file was never created
            for path in PROTECTED_PATHS:
                self.protected_entries.add(path.format(proj_code=self.proj_code))
            return

        data_frame = pd.read_csv(self.protected_data_file)
        data_dict = data_frame.to_dict()
        self.protected_entries = set(data_dict["path"].values())

    def write_archive_data(self):
        """Stores the archive data dictionary as a CSV file in the project.

        This allows us to retrieve the data in the archive dialog and keep
        a history of all the files archived in the project.
        """
        start_time = time.time()

        # Create final dictionary to store in csv
        data_dict = {
            "path": [],
            "delete_time": [],
            "marked_time": [],
            "size": [],
            "is_deleted": [],
            "publish_dir": [],
            "publish_id": [],
            "reason": [],
            "paths": [],
        }
        for path, data_entries in self.archive_entries.items():
            data_dict["path"].append(path)
            data_dict["delete_time"].append(data_entries["delete_time"])
            data_dict["marked_time"].append(data_entries["marked_time"])
            data_dict["size"].append(data_entries["size"])
            data_dict["is_deleted"].append(data_entries["is_deleted"])
            data_dict["publish_dir"].append(data_entries.get("publish_dir", ""))
            data_dict["publish_id"].append(data_entries.get("publish_id", ""))
            data_dict["reason"].append(data_entries.get("reason", ""))
            data_dict["paths"].append(data_entries.get("paths", set()))

        # Create a pandas data frame from current archive data dictionary
        df = pd.DataFrame(data_dict)

        # Make sure we don't overwrite existing entries from the main CSV file
        if os.path.exists(self.delete_data_file):
            existing_df = pd.read_csv(self.delete_data_file)
            combined_df = pd.concat([existing_df, df])
            df = combined_df.drop_duplicates(subset=["path"], keep="last")

        # Write out data to CSV file
        df.to_csv(self.delete_data_file, index=False)

        elapsed_time = time.time() - start_time
        logger.info(
            "Saved CSV data in '%s', it took %s",
            self.delete_data_file,
            utils.time_elapsed(elapsed_time)
        )

    def write_protected_data(self):
        """Stores the protected data dictionary as a CSV file in the project."""
        start_time = time.time()

        # Create final dictionary to store in csv
        data_dict = {
            "path": list(self.protected_entries),
        }

        # Create a pandas data frame from current protected data dictionary
        df = pd.DataFrame(data_dict)

        # Make sure we don't overwrite existing entries from the main CSV file
        if os.path.exists(self.protected_data_file):
            existing_df = pd.read_csv(self.protected_data_file)
            combined_df = pd.concat([existing_df, df])
            df = combined_df.drop_duplicates(subset=["path"], keep="last")

        # Write out data to CSV file
        df.to_csv(self.protected_data_file, index=False)

        elapsed_time = time.time() - start_time
        logger.info(
            "Saved CSV data in '%s', it took %s",
            self.protected_data_file,
            utils.time_elapsed(elapsed_time)
        )

    def get_archive_data(self):
        """Retrieves the data stored in the project as a pd.DataFrame object
        """
        return pd.read_csv(self.delete_data_file)

    # ------------// Common Functions //------------
    def get_shotgrid_data(self):
        """Get the necessary data from Shotgrid for getting more info about how to
        clean the project.
        """
        logger.info(" - Getting Final list from Shotgrid")

        # Find if project is restricted from clean up
        if self.sg.find(
            "Project",
            [["sg_code", "is", self.proj_code], ["sg_auto_cleanup", "is", False]],
        ):
            return False

        shots_status = {}

        statuses_to_check = [
            "snt", "fin", "omt"
        ]

        # Find all entities that have been finaled
        filters = [
            ["project.Project.sg_code", "is", self.proj_code],
            ["sg_status_list", "in", statuses_to_check],
        ]
        fields = [
            "code",
            "entity",
            "sg_status_list",
            "sg_path_to_frames",
            media.SG_FIELD_MEDIA_GENERATED,
            media.SG_FIELD_MEDIA_PATH,
            media.SG_FIELD_OP_INSTANCE_ID,
        ]
        sg_versions = self.sg.find("Version", filters, fields)

        for sg_version in sg_versions:
            try:
                shot_name = sg_version["entity"]["name"]
            except TypeError:
                shot_name = "unassigned"

            version_status = sg_version["sg_status_list"]
            if version_status not in shots_status:
                shots_status[version_status] = {}
            if shot_name not in shots_status[version_status]:
                shots_status[version_status][shot_name] = []

            shots_status[version_status][shot_name].append(
                sg_version[media.SG_FIELD_OP_INSTANCE_ID]
            )

            # If the version is sent, we append it to the global protected entries
            if version_status == "snt":
                # From a path such as "/proj/<proj_code>/shots/<asset>/publish/render/<task>/<version>/exr/<name>.exr"
                # we want to store the path up to <version> as protected
                path_to_frames = sg_version.get("sg_path_to_frames")
                if not path_to_frames:
                    logger.warning(
                        "Path to frames for version '%s' not found.",
                        sg_version["code"]
                    )
                    continue
                version_path = os.path.dirname(
                    os.path.dirname(path_to_frames)
                )
                self.protected_entries.add(version_path)

        return shots_status

    def clean_existing_entries(self, archive=False):
        """Clean existing entries from self.archive_entries"""
        logger.info(" \n---- Cleaning files marked for archive from CSV ---- \n")

        for _, data_entry in self.archive_entries.items():
            # Skip entries that have already been marked deleted
            if data_entry["is_deleted"]:
                continue

            self.consider_filepaths_for_deletion(
                data_entry["paths"],
                caution_level=None,
                archive=archive,
            )

    def clean_old_versions(self, keep_versions=5, archive=False):
        """Clean old version folders (if more than 'keep_versions' are found)

        Args:
            keep_versions (int, optional): The number of versions to keep.
                Defaults to 5.
            archive (bool, optional): Whether to force delete the files.
                Defaults to False.
        """
        logger.info(" \n---- Cleaning old versions ---- \n")
        logger.info("Keeping only '%s' versions in total", keep_versions)

        caution_level = 1

        for folder in ["assets", "shots"]:
            target = os.path.join(self.target_root, folder)

            for dirpath, dirnames, _ in os.walk(target, topdown=True):
                # If we are not running an archive, skip the 'publish' folders
                if not archive and "/publish" in dirpath:
                    continue
                # Skip protected path entries
                elif dirpath in self.protected_entries:
                    logger.debug(f"Skipping '{dirpath}' as it's protected")
                    continue

                version_collections, _ = clique.assemble(
                    dirnames, patterns=[clique.PATTERNS["versions"]]
                )
                if not version_collections:
                    continue

                # Clear dirnames to not continue exploring the directories
                dirnames.clear()

                for version_collection in version_collections:
                    keep_versions_offset = 0

                    # All the marked deletion folders are considered for deletion
                    if DELETE_PREFIX in version_collection.head:
                        for folder in version_collection:
                            folder_to_delete = os.path.join(dirpath, folder)
                            self.consider_file_for_deletion(
                                folder_to_delete,
                                caution_level=caution_level,
                                archive=archive,
                                extra_data={
                                    "reason": "Old version folder"
                                }
                            )

                    # Otherwise, we simply try remove the oldest versions if
                    # there's more than 'keep_versions + keep_versions_offset'
                    else:
                        version_folders = list(version_collection)

                        # For certain keywords, we are a bit more careful and keep some extra versions
                        for protected_name in PROTECTED_OLD_VERSIONS:
                            if protected_name in dirpath.lower():
                                keep_versions_offset = 2
                                if len(version_folders) > keep_versions + keep_versions_offset:
                                    logger.debug(
                                        "Keeping '%s' extra versions due to extra caution.",
                                        keep_versions_offset
                                    )
                                break

                        while len(version_folders) > keep_versions + keep_versions_offset:
                            folder_to_delete = os.path.join(
                                dirpath, version_folders.pop(0)
                            )
                            self.consider_file_for_deletion(
                                folder_to_delete,
                                caution_level=caution_level,
                                archive=archive,
                                extra_data={
                                    "reason": "Old version folder"
                                }
                            )

    def get_version_path(self, version_id):
        """Get the path on disk of the version id by checking the path of the
        first representation found for that version.
        """
        # Create filepath from published file
        repre_docs = ayon_api.get_representations(
            self.project_name, version_ids=[version_id]
        )
        version_path = None
        for repre_doc in repre_docs:
            repre_name_path = os.path.dirname(
                repre_doc["data"]["path"]
            )
            version_path = os.path.dirname(repre_name_path)
            break

        return version_path

    def clean_published_file_sources(self, archive=False):
        """Cleans the source of the published files of the project.

        Args:
            archive (bool, optional): Whether to force delete the files.
                Defaults to False.
        """
        logger.info(" \n---- Finding already published files ---- \n")

        # Level of caution for published files
        caution_level_default = 1

        # TODO: enable after a while since `stagingDir` integrate on the
        # representations was just added recently
        # repre_docs = ayon_api.get_representations(
        #     project_name
        # )
        # # Iterate over all representations in the project and check if
        # # stagingDir is stored in its data and consider it for deletion
        # # if it's old enough
        # for repre_doc in repre_docs:
        #     staging_dir = repre_doc["data"].get("stagingDir")
        #     if staging_dir:
        #         staging_dir = anatomy.fill_root(staging_dir)
        #         # TODO: make sure to check if the staging dir is older than the publish!
        #         deleted, _, size = self.consider_file_for_deletion(
        #             staging_dir, caution_level=caution_level, archive
        #         )
        #         if deleted:
        #             logger.info(" - Published file in '%s'", )
        #             if calculate_size:
        #                 total_size += size

        version_entities = ayon_api.get_versions(self.project_name)
        for version_entity in version_entities:

            # Reset caution level every time
            caution_level_ = caution_level_default

            version_id = version_entity["id"]

            if version_entity["data"].get("source_deleted"):
                logger.debug(
                    "Skipping version '%s' as 'source_deleted' is true and that means it was already archived",
                    version_id
                )
                continue

            version_path = self.get_version_path(version_id)
            if not version_path:
                continue

            rootless_source_path = version_entity["data"].get("source")
            if not rootless_source_path:
                continue
            source_path = self.anatomy.fill_root(rootless_source_path)

            # Create a path of what we want to symlink the source path
            # to if we want to keep the source path but not the files
            symlink_paths = []

            # If source path is a Hiero workfile, we can infer that the publish
            # was a plate publish and a 'temp_transcode' folder was created next
            # to the workfile to store the transcodes before publish
            if source_path.endswith(".hrox"):
                product_entity = ayon_api.get_product_by_id(
                    self.project_name, product_id=version_entity["productId"]
                )
                if not product_entity:
                    logger.warning(
                        "Couldn't find product for version '%s' with id '%s for source path '%s'",
                        version_entity["name"], version_entity["productId"], source_path
                    )
                    continue
                # Hard-code the path to the temp_transcode folder
                source_files = glob.glob(os.path.join(
                    os.path.dirname(source_path),
                    "temp_transcode",
                    f"*{product_entity['name']}*",
                ))
                # Override caution file for temp_transcode files to be very low caution
                caution_level_ = 0
            # If source path is a Nuke work file, we can infer that the publish is
            # likely to be a render publish and the renders are stored in a
            # folder called 'renders' next to the Nuke file
            # NOTE: ignore the 'io' folder as it's used for the I/O of the project
            elif source_path.endswith(".nk") and "/io/" not in source_path:
                product_entity = ayon_api.get_product_by_id(
                    self.project_name, product_id=version_entity["productId"]
                )
                if not product_entity:
                    logger.warning(
                        "Couldn't find product for version '%s' with id '%s for source path '%s'",
                        version_entity["name"], version_entity["productId"], source_path
                    )
                    continue
                if product_entity["data"]["family"] == "workfile":
                    continue
                folder_entity = ayon_api.get_folder_by_id(
                    self.project_name, folder_id=product_entity["folderId"]
                )
                if not folder_entity:
                    logger.warning(
                        "Couldn't find folder for product '%s' with id '%s'",
                        product_entity["name"], product_entity["folderId"]
                    )
                    continue
                # Hard-code the path to the renders for Nuke files
                source_files = [os.path.join(
                    os.path.dirname(source_path),
                    "renders",
                    "nuke",
                    f"{folder_entity['name']}_{product_entity['name'].replace(' ', '_')}",
                    "v{:03}".format(version_entity["name"]),
                )]
                symlink_paths = [os.path.join(version_path, "exr")]
            # Otherwise, we just check the 'source' directly assuming that's
            # directly the source of the publish
            else:
                # Override /io entries and .hip sources so we don't try remove them
                if "/io/" in source_path or \
                        source_path.endswith(".hip") or \
                        not source_path.startswith(f"/proj/{self.proj_code}"):
                    continue

                source_files, _, _ = path_tools.convert_to_sequence(
                    source_path
                )
                # For source paths ending with .exr we try create a symlink path from
                # the original source to the publish path
                if source_path.endswith(".exr"):
                    symlink_paths = glob.glob(
                        os.path.join(version_path, "exr", "*")
                    )

            if not source_files or not os.path.exists(source_files[0]):
                logger.debug(
                    "Couldn't find files for file pattern '%s' from published path '%s'"
                    " checking if the files were marked for deletion.",
                    source_path, version_path
                )
                dir_path, original_name = os.path.split(source_path)
                delete_path = os.path.join(dir_path, f"{DELETE_PREFIX}*{original_name}")
                source_files, _, _ = path_tools.convert_to_sequence(
                    delete_path
                )
                if not source_files:
                    logger.debug(
                        "Couldn't find files marked for deletion at '%s' either.",
                        delete_path
                    )
                    continue
                # Override symlink paths to None so we ignore the symlinking
                # warning and delete files that were marked for deletion already
                symlink_paths = None

            elif os.path.islink(source_files[0]):
                logger.debug(
                    "Source files are already a symlink from publish path "
                    "but checking if there's some marked for deletion we should delete"
                )
                dir_path, original_name = os.path.split(source_files[0])
                delete_path = os.path.join(dir_path, f"{DELETE_PREFIX}*{original_name}")
                source_files = glob.glob(delete_path)
                if not source_files:
                    source_files, _, _ = path_tools.convert_to_sequence(
                        delete_path
                    )
                    if not source_files:
                        logger.debug(
                            "Couldn't find files for delete path '%s'.",
                            delete_path
                        )
                        continue

            version_created = datetime.strptime(
                version_entity["data"]["time"], "%Y%m%dT%H%M%SZ"
            )

            # If we found files, we consider them for deletion
            deleted, marked = self.consider_filepaths_for_deletion(
                source_files,
                caution_level=caution_level_,
                archive=archive,
                create_time=version_created,
                extra_data={
                    "publish_id": version_id,
                    "publish_dir": version_path,
                    "reason": "Already published"
                },
                symlink_paths=symlink_paths
            )

            if marked or deleted:
                logger.info(
                    "Published files for source '%s' in version with id '%s': '%s'",
                    source_path,
                    version_id,
                    version_path,
                )

            if deleted and not const._debug:
                # Add metadata to version so we can skip from inspecting it
                # in the future
                logger.debug("Updating OP entity with data.source_deleted=True")
                session = OperationsSession()
                session.update_entity(
                    self.project_name,
                    "version",
                    version_entity["id"],
                    {"data.source_deleted": True}
                )
                session.commit()

    def clean_io_files(self, archive=False):
        """Cleans the I/O directories of the project.

        Args:
            archive (bool, optional): Whether to force delete the files.
                Defaults to False.

        Returns:
            float: The total size of the deleted files in bytes.
        """
        logger.info(" \n---- Finding old files in I/O ----\n")

        # Level of caution for I/O files
        caution_level = 1

        if archive:
            target_folders = ["nyc-sync", "outgoing", "delivery", "outsource"]
        else:
            target_folders = ["outgoing", "delivery", "outsource"]

        for folder in target_folders:
            target = os.path.join(self.target_root, "io", folder)
            if os.path.exists(target):
                logger.debug(f"Scanning {target} folder")
            else:
                logger.warning(f"{target} folder does not exist")
                continue

            if archive:
                # Add entire folder
                self.consider_file_for_deletion(
                    target,
                    archive=True,
                    extra_data={
                        "reason": "Force delete on archive"
                    }
                )
            else:
                for dirpath, dirnames, filenames in os.walk(target, topdown=True):
                    # Check each subdirectory in the current directory
                    for dirname in list(
                        dirnames
                    ):  # Use a copy of the list for safe modification
                        subdirpath = os.path.join(dirpath, dirname)
                        deleted, marked = self.consider_file_for_deletion(
                            subdirpath,
                            caution_level=caution_level,
                            archive=archive,
                            extra_data={
                                "reason": "Routine clean up"
                            }
                        )
                        if deleted or marked:
                            # Remove from dirnames to prevent further exploration
                            dirnames.remove(dirname)

                    # Check each file in the current directory
                    filepaths = [os.path.join(dirpath, filename) for filename in filenames]
                    self.consider_filepaths_for_deletion(
                        filepaths,
                        caution_level=caution_level,
                        archive=archive,
                        extra_data={
                            "reason": "Routine clean up"
                        }
                    )

    def clean_work_files(self, archive=False):
        """Cleans the work directories of the project by removing old files and folders
        that we consider not relevant to keep for a long time.
        """
        logger.info(" \n---- Cleaning work files ----\n")

        # Folders that we want to clear all the files from inside them
        # that are older than our threshold and the number of caution
        # of removal to take for each
        folders_to_clean = {
            "backup": 1,
            "ifd": 1,
            "ifds": 1,
            "temp_transcode": 0,
            "nuke_review_script": 0,
        }

        # If archiving, we also want to clear some extra folders
        if archive:
            folders_to_clean.update(
                {
                    "img": 0,
                    "ass": 0,
                    "cache": 0,
                }
            )

        for folder in ["assets", "shots"]:
            target = os.path.join(self.target_root, folder)
            if os.path.exists(target):
                logger.debug(f" - Scanning {target} folder")
            else:
                logger.warning(f" - {target} folder does not exist")
                continue

            for dirpath, dirnames, filenames in os.walk(target, topdown=True):
                # Skip all folders that aren't within a 'work' directory
                if "/work" not in dirpath:
                    continue

                # Add files from the potential archive folders that are
                # older than 7 days
                for folder, caution_level in folders_to_clean.items():
                    if folder not in dirnames:
                        continue

                    filepaths = glob.glob(os.path.join(dirpath, folder, "*"))
                    deleted, marked = self.consider_filepaths_for_deletion(
                        filepaths,
                        caution_level=caution_level,
                        archive=archive,
                        extra_data={
                            "reason": "Transient file"
                        }
                    )
                    if deleted or marked:
                        # Remove from dirnames to prevent further exploration
                        dirnames.remove(folder)

                # Delete all files that match the patterns that we have decided
                # we should delete
                for filename in filenames:
                    for pattern in TEMP_FILE_PATTERNS:
                        if pattern.match(filename):
                            filepath = os.path.join(dirpath, filename)
                            deleted, marked = self.consider_file_for_deletion(
                                filepath,
                                caution_level=0,
                                archive=archive,
                                extra_data={
                                    "reason": "Transient file"
                                }
                            )

    def clean_shots_by_status(self, shots_status, archive=False):
        """Cleans publishes by having information about the status of shots in SG.

        If we know that a version was omitted, we delete that version.
        For final statuses, we delete all the versions that are not final.
        """
        logger.info(" \n---- Cleaning shots based on its SG status ----\n")

        # Level of caution for archive based on status
        caution_level = 0

        # Only clear versions based on 'final' status when running archive
        if archive:
            # For final status, we add all versions but the ones listed
            for shot_name, version_ids in shots_status.get("fin", {}).items():

                # TODO: add more logic to delete other versions from shot
                #folder_entity = ayon_api.get_asset_by_name(project_name, shot)

                for version_id in version_ids:
                    final_version_entity = ayon_api.get_version_by_id(
                        self.project_name, version_id=version_id, fields=["productId", "name"]
                    )
                    product_entity = ayon_api.get_product_by_id(
                        self.project_name, product_id=final_version_entity["productId"], fields=["id"]
                    )
                    version_entities = ayon_api.get_versions(
                        self.project_name, product_ids=[product_entity["id"]], fields=["id"]
                    )

                    for version_entity in version_entities:
                        # Skip all the versions that were marked as final
                        other_version_id = str(version_entity["id"])
                        if other_version_id in version_ids:
                            # And break the loop as soon as the final version is found
                            # so we don't delete any newer versions after that one
                            break

                        # Add the directory where all the representations live
                        version_path = self.get_version_path(other_version_id)
                        if version_path in self.protected_entries:
                            logger.debug(f"Skipping '{version_path}' as it's one of the protected entries")
                            continue
                        self.consider_file_for_deletion(
                            version_path,
                            caution_level=caution_level,
                            archive=archive,
                            extra_data={
                                "publish_id": other_version_id,
                                "reason": f"Old versions in final status (v{final_version_entity['name']})"
                            }
                        )

        # For omitted status, we add the versions listed directly
        for shot_name, version_ids in shots_status.get("omt", {}).items():
            version_entities = ayon_api.get_versions(
                self.project_name, version_ids=version_ids, fields=["id"]
            )

            for version_entity in version_entities:
                version_id = version_entity["id"]
                # Delete the directory where all the representations for that
                # version exist
                version_path = self.get_version_path(version_id)
                self.consider_file_for_deletion(
                    version_path,
                    caution_level=caution_level,
                    archive=archive,
                    extra_data={
                        "publish_id": version_id,
                        "reason": "Omitted status"
                    }
                )

    # ------------// Archival Functions //------------
    def generate_archive_media(self):
        """Runs the archive template on all final versions"""
        logger.info(" \n---- Generating media from all final versions before archive ----\n")

        # Find all entities that have been finaled
        filters = [
            ["project.Project.sg_code", "is", self.proj_code],
            ["sg_status_list", "in", ["fin"]],
        ]
        sg_versions = self.sg.find("Version", filters, media.SG_VERSION_IMPORTANT_FIELDS)

        delivery_data = {
            "output_names_ext": [("exr", "exr"), ("prores422", "mov")],
            "force_delivery_media": True,
            "force_override_files": False,
            "package_name_override": "{yyyy}{mm}{dd}",
            "filename_override": "{shot}_{task[short]}_v{version:0>3}",
            # The delivery staging dir is "/proj/{project[code]}/io/delivery/ready_to_deliver/{yyyy}{mm}{dd}"
            # so in order to write at /proj/{project[code]}/io/archive_qt_exr we prefix the path with ../../../
            "template_path": "../../../archive_qt_exr/{output}/<{is_sequence}<{filename}/>>{filename}<.{frame:0>4}>.{ext}",
            "nuke_template_script": "/pipe/nuke/templates/archive_template.nk"
        }

        report_items = collections.defaultdict(list)
        success = True
        for sg_version in sg_versions:
            new_report_items, new_success = media.generate_delivery_media_version(
                sg_version,
                self.project_name,
                delivery_data,
                report_items,
                update_sg_data=False,
            )
            if new_report_items:
                report_items.update(new_report_items)

            if not new_success:
                success = False

        if not success:
            logger.error(pprint.pprint(report_items))
        else:
            logger.info(pprint.pprint(report_items))

    def package_workfiles(self):
        """Package the work directories into .tar files."""

        logger.info(" \n---- Packaging work files ----\n")

        for folder in ["assets", "shots"]:
            target = os.path.join(self.target_root, folder)
            if os.path.exists(target):
                logger.debug(f" - Scanning {target} folder")
            else:
                logger.warning(f" - {target} folder does not exist")
                continue

            # Package every child folder under 'work'
            for dirpath, dirnames, _ in os.walk(target, topdown=True):
                # Skip all folders that aren't within a 'work' directory
                if "/work" not in dirpath:
                    continue

                for dirname in list(dirnames):
                    child_dir = os.path.join(dirpath, dirname)
                    logger.info(f"Packaging {child_dir}")
                    # The .zip and .tar.gz are added just for backwards compatibility
                    # but only .tar's should be created from now on
                    if child_dir.endswith(".tar") or child_dir.endswith(".zip") or child_dir.endswith(".tar.gz"):
                        logger.info("Skipping {child_dir} as it's already compressed")
                        continue

                    if not const._debug:
                        # Create a single .tar entry with all the symlinks and files
                        run_subprocess(
                            ["tar", "-cf", f"{dirname}.tar", dirname, "--remove-files"],
                            cwd=dirpath
                        )

                    # Remove directory from dirnames to prevent further exploration
                    dirnames.remove(dirname)

    def delete_filepath(self, filepath, silent=False):
        """Delete a file or directory"""
        try:
            if not const._debug:
                if os.path.isfile(filepath):
                    os.remove(filepath)  # Remove the file
                elif os.path.isdir(filepath):
                    shutil.rmtree(filepath)  # Remove the dir and all its contents
                else:
                    logger.info(f"'{filepath}' is not a valid file or directory.")
                    return False

            if not silent:
                logger.info(f"Deleted path: '{filepath}'.")

            if const._debug:
                return False

            return True
        except Exception as e:
            logger.error(f"Error deleting '{filepath}': {e}")

        return False

    def parse_date_from_filename(self, filename):
        """Parse the date from the filename if it has the DELETE_PREFIX in it."""
        match = re.search(rf'{DELETE_PREFIX}\((.*?)\).*', filename)
        if match:
            date_string = match.group(1)
            return datetime.strptime(date_string, DATE_FORMAT)

    def consider_filepaths_for_deletion(
        self,
        filepaths,
        caution_level=2,
        archive=False,
        create_time=None,
        extra_data=None,
        symlink_paths=None,
    ):
        """Consider a clique.filepaths for deletion based on its age"""
        deleted = False
        marked = False

        # Check if the symlink paths argument is a list (at the moment only coming from the clean_published_source_files)
        # and only try remove the filepaths if the symlink paths actually matches the list of paths
        if isinstance(symlink_paths, list):
            if not symlink_paths:
                logger.warning(
                    "The function was expecting some symlink paths but the list is empty, skipping."
                )
                return False, False
            elif len(symlink_paths) != len(filepaths):
                logger.warning(
                    "The number of symlink paths should be the same as the number of filepaths, skipping."
                )
                return False, False
            # Make sure that the symlink is from the same project folder
            symlink_path = pathlib.Path(symlink_paths[0])
            filepath = pathlib.Path(filepaths[0])
            # The second index should be the project code (i.e., /proj/<proj_code> -> ('/', 'proj', '<proj_code>'))
            if symlink_path.parts[2] != filepath.parts[2]:
                logger.warning(
                    "The root of the symlink comes from a different project, ignoring"
                )
                return False, False

        collections, remainders = clique.assemble(filepaths)
        for collection in collections:
            deleted_, marked_ = self.consider_collection_for_deletion(
                collection,
                caution_level,
                archive,
                create_time,
                extra_data=extra_data,
                symlink_paths=symlink_paths
            )
            if deleted_:
                deleted = True
            if marked_:
                marked = True

        for index, remainder in enumerate(remainders):
            deleted_, marked_ = self.consider_file_for_deletion(
                remainder,
                caution_level,
                archive,
                create_time,
                extra_data=extra_data,
                symlink_path=symlink_paths[index] if symlink_paths else None
            )
            if deleted_:
                deleted = True
            if marked_:
                marked = True

        return deleted, marked

    def consider_collection_for_deletion(
        self,
        collection,
        caution_level=2,
        archive=False,
        create_time=None,
        extra_data=None,
        symlink_paths=None,
    ):
        """Consider a clique.collection for deletion based on its age"""
        deleted = False
        marked = False

        for index, filepath in enumerate(collection):
            deleted_, marked_ = self.consider_file_for_deletion(
                filepath,
                caution_level,
                archive,
                create_time,
                silent=True,
                extra_data=extra_data,
                symlink_path=symlink_paths[index] if symlink_paths else None
            )
            if deleted_:
                deleted = True
            if marked_:
                marked = True

        if deleted:
            logger.info(f"Deleted collection '{collection}'")
        elif marked:
            logger.info(f"Marked collection for deletion: '{collection}' (caution: {caution_level})")

        return deleted, marked

    def get_filepath_size(self, filepath, filepath_stat):
        """Util function to retun size of a file by using 'du' if it's a directory or the stat
        object if it's a file
        """
        if os.path.isdir(filepath):
            return int(run_subprocess(["du", "-s", filepath]).split("\t")[0]) * 1024

        return filepath_stat.st_size

    def consider_file_for_deletion(
        self,
        filepath,
        caution_level=2,
        archive=False,
        create_time=None,
        silent=False,
        extra_data=None,
        symlink_path=None,
    ):
        """Consider a file for deletion based on its age

        Args:
            filepath (str): The path to the file
            caution_level (int, optional): The level of caution to take when deleting
                the file. Defaults to 2.
            archive (bool, optional): Whether we are running an archive or just a
                routine clean up.
            create_time (datetime, optional): The time the publish file was created so
                we can compare it to the modification time of the file we are considering
                for deletion. Defaults to None.
            silent (bool, optional): Whether to suppress the log messages. Defaults to
                False.
            extra_data (dict, optional): Extra data to store in the archive entries.
                Defaults to None.
            symlink_path (str, optional): The path to the file to create a symlink to.

        Returns:
            bool: Whether the file was deleted
            bool: Whether the file was marked for deletion
            float: The size of the deleted file
        """
        if not filepath:
            return False, False

        # Extract the directory path and the original name
        dir_path, original_name = os.path.split(filepath)

        try:
            filepath_stat = os.stat(filepath)
        except FileNotFoundError:
            logger.warning(f"File not found: '{filepath}'")
            try:
                filepath = glob.glob(os.path.join(dir_path, f"*{original_name}"))[0]
                filepath_stat = os.stat(filepath)
                logger.info(f"But found its marked for deletion equivalent: '{filepath}'")
                dir_path, original_name = os.path.split(filepath)
            except (IndexError, FileNotFoundError):
                logger.warning(f"Marked for deletion file not found either")
                return False, False

        if os.path.islink(filepath):
            logger.debug(f"Skipping symlink: '{filepath}'")
            return False, False

        # Replace frame with token to save file ranges under the same entry
        path_entry = path_tools.replace_frame_number_with_token(filepath, "*")

        # If the file is already marked for deletion, we want to store it in the same
        # entry as the original file
        if DELETE_PREFIX in original_name:
            new_name = DELETE_PREFIX_RE.sub("", original_name)
            path_entry = os.path.join(dir_path, new_name)

        if path_entry in self.protected_entries:
            logger.debug(f"Skipping '{path_entry}' as it's protected")
            return

        # If the entry already exists, we want to add the file to the existing entry
        # if the path wasn't added to the set of paths
        if path_entry in self.archive_entries:
            data_entry = self.archive_entries[path_entry]
        # Otherwise, we want to create a new entry for the file
        else:
            if caution_level is None:
                logger.error(
                    "No caution level was passed to the function, probably due "
                    "to assuming the file was already marked for deletion but it "
                    "wasn't found on the existing entries. Skipping!")
                return False, False

            data_entry = {
                "marked_time": TIME_NOW,
                "delete_time": TIME_NOW + DELETE_THRESHOLDS[caution_level],
                "is_deleted": False,
            }
            if extra_data:
                data_entry.update(extra_data)

        # If the file is already marked for deletion, we want to check if it's time
        # to delete it
        if DELETE_PREFIX in original_name or archive:
            # If we are passed the time marked for deletion or archive is True, delete it
            if datetime.today() > data_entry.get("delete_time") or archive:
                if not silent:
                    if not archive:
                        logger.info(
                            f"File has been marked for deletion enough time, deleting it."
                        )
                success = self.delete_filepath(filepath, silent=silent)
                if success:
                    data_entry["is_deleted"] = True
                    size_deleted = self.get_filepath_size(filepath, filepath_stat)
                    self.total_size_deleted += size_deleted
                    # Add filepath to set of paths for data entry
                    if data_entry.get("paths"):
                        data_entry["paths"].add(filepath)
                        data_entry["size"] += size_deleted
                    else:
                        data_entry["paths"] = {filepath}
                        data_entry["size"] = size_deleted
                    self.archive_entries[path_entry] = data_entry
                    return True, False
                return False, False

            return False, False
        # If file was modified after the creation time (publish), ignore removal to be safe
        elif create_time and filepath_stat.st_mtime > create_time.timestamp():
            logger.debug(
                "File '%s' was modified after it was published, ignoring the removal",
                filepath
            )
            return False, False
        # If file is newer than warning, ignore
        elif filepath_stat.st_mtime > WARNING_THRESHOLDS.get(caution_level, WARNING_THRESHOLDS[2]).timestamp():
            return False, False

        # Create the new name with the prefix
        new_name = f"{TIME_DELETE_PREFIX}{original_name}"

        # Construct the full path for the new name
        new_filepath = os.path.join(dir_path, new_name)

        # Add new filepath to set of paths for data entry
        if data_entry.get("paths"):
            data_entry["size"] += self.get_filepath_size(filepath, filepath_stat)
            data_entry["paths"].add(new_filepath if not const._debug else filepath)
        else:
            data_entry["size"] = self.get_filepath_size(filepath, filepath_stat)
            data_entry["paths"] = {new_filepath if not const._debug else filepath}

        # Rename the file or folder
        if not const._debug:
            os.rename(filepath, new_filepath)

        # If we are passing a symlink path, we want to create a symlink from
        # the source path to the new path
        if symlink_path:
            logger.debug(
                "Created symlink from '%s' to '%s'", symlink_path, filepath
            )
            if not const._debug:
                os.symlink(symlink_path, filepath)

        if not silent:
            logger.info(
                f"Marked for deletion: '{filepath}' -> '{new_name}' (caution: {caution_level})"
            )

        self.archive_entries[path_entry] = data_entry

        return False, True


# ------------// Callable Functions //------------
def clean_all():
    """Cleans all the projects in the projects directory."""
    scan_start = time.time()

    timestamp = time.strftime("%Y%m%d%H%M")
    summary_file = os.path.join(const.EXPORT_DIR, f"{timestamp}{'_debug' if const._debug else ''}.txt")

    # Create a file handler which logs even debug messages
    file_handler = logging.FileHandler(summary_file)
    file_handler.setLevel(logging.info)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )

    logger.addHandler(file_handler)

    logger.info("======= CLEAN ALL PROJECTS =======")

    for proj in sorted(os.listdir(const.PROJECTS_DIR)):
        archive_project = ArchiveProject(proj)
        archive_project.clean(archive=False)

    elapsed_time = time.time() - scan_start
    logger.info("Total Clean Time %s", utils.time_elapsed(elapsed_time))
