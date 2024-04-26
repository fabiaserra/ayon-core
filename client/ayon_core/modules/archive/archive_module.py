import sys

from ayon_core.lib import get_ayon_launcher_args
from ayon_core.lib.execute import run_detached_process
from ayon_core.addon import (
    AYONAddon,
    ITrayAction,
    click_wrap
)


class ArchiveModule(AYONAddon, ITrayAction):
    label = "Archive"
    name = "archive"

    def initialize(self, settings):
        self.enabled = True

    def cli(self, click_group):
        click_group.add_command(cli_main.to_click_obj())

    def tray_init(self):
        return

    def launch_archive_tool(self):
        args = get_ayon_launcher_args(
            "addon", self.name, "launch"
        )
        run_detached_process(args)

    def on_action_trigger(self):
        self.launch_archive_tool()


@click_wrap.command("clean_project")
@click_wrap.argument("proj_code")
@click_wrap.option("--archive/--no-archive", default=False)
def clean_project_command(
    proj_code,
    archive,
):
    """Perform a routine clean up of project by removing old files and folders
    that we consider irrelevant to keep through a production lifecycle.
    """
    sys.path.insert(0, "/sw/python/3.9.17/lib/python3.9/site-packages")
    from ayon_core.modules.archive.lib import expunge
    archive_proj = expunge.ArchiveProject(proj_code)
    return archive_proj.clean(archive=archive)


@click_wrap.command("purge_project")
@click_wrap.argument("proj_code")
def purge_project_command(
    proj_code,
):
    """Perform deep cleaning of the project by force deleting all the unnecessary
    files and folders and compressing the work directories.
    """
    sys.path.insert(0, "/sw/python/3.9.17/lib/python3.9/site-packages")
    from ayon_core.modules.archive.lib import expunge
    archive_proj = expunge.ArchiveProject(proj_code)
    return archive_proj.purge()


@click_wrap.command("generate_archive_media")
@click_wrap.argument("proj_code")
def generate_archive_media(
    proj_code,
):
    """Generate deliveries for all the final versions of the project.
    """
    sys.path.insert(0, "/sw/python/3.9.17/lib/python3.9/site-packages")
    from ayon_core.modules.archive.lib import expunge
    archive_proj = expunge.ArchiveProject(proj_code)
    return archive_proj.generate_archive_media()


@click_wrap.group(ArchiveModule.name, help="Archive CLI")
def cli_main():
    pass


@cli_main.command()
def launch():
    """Launch TrayPublish tool UI."""
    sys.path.insert(0, "/sw/python/3.9.17/lib/python3.9/site-packages")
    from ayon_core.modules.archive.tray import archive_dialog
    archive_dialog.main()


cli_main.add_command(clean_project_command)
cli_main.add_command(purge_project_command)
cli_main.add_command(generate_archive_media)

