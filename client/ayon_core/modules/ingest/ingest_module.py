import click

from ayon_core.modules import (
    AYONAddon,
    ITrayAddon
)


class IngestModule(AYONAddon, ITrayAddon):
    label = "Ingest"
    name = "ingest"
    tray_wrapper = None

    def initialize(self, settings):
        self.enabled = True

    def cli(self, click_group):
        click_group.add_command(cli_main)

    def tray_init(self):
        from .tray.ingest_tray import IngestTrayWrapper

        self.tray_wrapper = IngestTrayWrapper(self)

    def tray_start(self):
        return

    def tray_exit(self, *args, **kwargs):
        return self.tray_wrapper

    def tray_menu(self, tray_menu):
        return self.tray_wrapper.tray_menu(tray_menu)


@click.command("ingest_folder_path")
@click.argument("folder_path", type=click.Path(exists=True))
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


@click.group(IngestModule.name, help="Ingest CLI")
def cli_main():
    pass


cli_main.add_command(ingest_folder_path)


if __name__ == "__main__":
    cli_main()


# Examples:
# /proj/uni/io/incoming/20230926/From_rotomaker/A/uni_pg_0430_plt_01_roto_output_v001
# /proj/uni/io/incoming/20230926/From_rotomaker/A/uni_pg_0440_plt_01_roto_output_v001
# /proj/uni/io/incoming/20230926/From_rotomaker/C/uni_pg_0380_denoise_dn_plt_01_v004_paint_v001
# /proj/uni/io/incoming/20230926/From_rotomaker/C/workfile/uni_pg_0380_denoise_dn_plt_01_v004_paint_v001_workfile
# /proj/uni/io/incoming/20230928/B/uni_ci_4088_plt_01_v001_MM_v001
# /proj/uni/io/incoming/20230928/B/uni_ci_4098_plt_01_v001_MM_v001
