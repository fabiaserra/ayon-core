from ayon_core.modules import (
    AYONAddon,
    ITrayAddon,
    click_wrap
)


class IngestModule(AYONAddon, ITrayAddon):
    label = "Ingest"
    name = "ingest"
    tray_wrapper = None

    def initialize(self, settings):
        self.enabled = True

    def cli(self, click_group):
        click_group.add_command(cli_main.to_click_obj())

    def tray_init(self):
        from .tray.ingest_tray import IngestTrayWrapper

        self.tray_wrapper = IngestTrayWrapper(self)

    def tray_start(self):
        return

    def tray_exit(self, *args, **kwargs):
        return self.tray_wrapper

    def tray_menu(self, tray_menu):
        return self.tray_wrapper.tray_menu(tray_menu)


@click_wrap.command("ingest_folder_path")
@click_wrap.argument("folder_path", type=str)
def ingest_folder_path(
    folder_path,
):
    """Given a folder, try ingest all its contents.

    Args:
        path (str): Path to the folder we want to ingest.

    """
    from ayon_core.modules.ingest.scripts import ingest
    return ingest.ingest_folder_path(
        folder_path
    )

@click_wrap.command("launch_batch_ingester")
def launch_batch_ingester():
    """Launch batch ingester tool UI."""
    from ayon_core.modules.ingest.tray import batch_ingester
    batch_ingester.main()


@click_wrap.command("launch_texture_publisher")
def launch_texture_publisher():
    """Launch Outsource Delivery tool UI."""
    from ayon_core.modules.ingest.tray import texture_publisher
    texture_publisher.main()


@click_wrap.group(IngestModule.name, help="Ingest CLI")
def cli_main():
    pass


cli_main.add_command(ingest_folder_path)
cli_main.add_command(launch_batch_ingester)
cli_main.add_command(launch_texture_publisher)
