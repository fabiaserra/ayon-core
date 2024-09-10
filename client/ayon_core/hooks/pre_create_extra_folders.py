"""Custom Alkemy-X hook to create other templated folders next to the 'work' directory"""
import os
from ayon_applications import PreLaunchHook, LaunchTypes
from ayon_core.pipeline import Anatomy


class CreateExtraFoldersHierarchy(PreLaunchHook):
    """Create extra folders next to the work directory on all the hierarchy.

    This is to allow us to create other templated folders next to the 'work'
    directory that gets created automatically when a host is launched on
    a context so we can have some predefined folders for people to be able
    to dump data without having to ingest it.

    For example this can be used to create a `references`, `elements` or `config`
    folder under the context.

    Currently we are hard-coding the list of extra folders but in the future we
    could choose to expose it on the ayon-core settings.

    Example:
        If we are launching shot `001_tst_020` on `dev_000` and we have only set
        the `elements` and `references` subfolders, this hook should create the
        following folders:

        /proj/dev_000/shots/elements
        /proj/dev_000/shots/references
        /proj/dev_000/shots/001/elements
        /proj/dev_000/shots/001/references
        /proj/dev_000/shots/001/tst/elements
        /proj/dev_000/shots/001/tst/references
        /proj/dev_000/shots/001/tst/001_tst_020/elements
        /proj/dev_000/shots/001/tst/001_tst_020/references

    """

    order = 15
    launch_types = {LaunchTypes.local}
    extra_folders = {"elements", "references"}

    def execute(self):        
        if not self.application.is_host:
            return

        env = self.data.get("env") or {}
        workdir = env.get("AYON_WORKDIR")
        if not workdir:
            return

        # Start at the second level up from the work directory
        # excluding 'work' and its parent folder which is the task entity
        entity_path = os.path.dirname(os.path.dirname(workdir))

        # Get project name and anatomy for folder path construction
        project_name = self.data["project_name"]
        anatomy = Anatomy(project_name)
        project_rootless = f"{{root[work]}}/{env['SHOW']}"
        project_root = anatomy.fill_root(project_rootless)

        # Loop while the current path is still within the project root
        while entity_path.startswith(project_root):
            
            # Skip folder creation if the current entity_path equals the project_root
            if entity_path == project_root:
                self.log.info(f"Skipping folder creation for project root: {project_root}")
                break  # Break the loop to stop processing further

            # Create each extra folders in the current directory
            for extra_folder in self.extra_folders:
                extra_folder_path = os.path.join(entity_path, extra_folder)

                # Create the folder if it doesn't already exist
                os.makedirs(extra_folder_path, exist_ok=True)
                self.log.info(f"Created or verified existence of: {extra_folder_path}")
            
            # Move one level up the directory tree
            entity_path = os.path.dirname(entity_path)