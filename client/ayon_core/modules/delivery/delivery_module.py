import click

from ayon_core.lib import get_ayon_launcher_args
from ayon_core.lib.execute import run_detached_process
from ayon_core.modules import (
    AYONAddon,
    ITrayAddon
)
from ayon_core.modules.delivery.scripts import sg_delivery


class DeliveryModule(AYONAddon, ITrayAddon):
    label = "Delivery"
    name = "delivery"
    tray_wrapper = None

    def initialize(self, settings):
        self.enabled = True

    def cli(self, click_group):
        click_group.add_command(cli_main)

    def tray_init(self):
        from .tray.delivery_tray import DeliveryTrayWrapper
        self.tray_wrapper = DeliveryTrayWrapper(self)

    def tray_start(self):
        return

    def tray_exit(self, *args, **kwargs):
        return self.tray_wrapper

    def tray_menu(self, tray_menu):
        return self.tray_wrapper.tray_menu(tray_menu)


@click.command("deliver_playlist_id")
@click.option(
    "--playlist_id",
    "-p",
    required=True,
    type=int,
    help="Shotgrid playlist id to deliver.",
)
@click.option(
    "--delivery_types",
    "-types",
    type=click.Choice(["final", "review"]),
    required=False,
    multiple=True,
    default=["final", "review"],
)
@click.option(
    "--representation_names",
    "-r",
    multiple=True,
    required=False,
    help="List of representation names that we want to deliver",
    default=None,
)
def deliver_playlist_id_command(
    playlist_id,
    delivery_types,
    representation_names=None,
):
    """Given a SG playlist id, deliver all the versions associated to it.

    Args:
        playlist_id (int): Shotgrid playlist id to deliver.
        delivery_types (list[str]): What type(s) of delivery it is
        representation_names (list): List of representation names to deliver.
            (i.e., ["final", "review"])

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the delivery was successful.
    """
    return sg_delivery.deliver_playlist_id(
        playlist_id, delivery_types, representation_names
    )


@click.command("deliver_version_id")
@click.option(
    "--version_id",
    "-v",
    required=True,
    type=int,
    help="Shotgrid version id to deliver.",
)
@click.option(
    "--delivery_types",
    "-types",
    type=click.Choice(["final", "review"]),
    required=False,
    multiple=True,
    default=["final", "review"],
)
@click.option(
    "--representation_names",
    "-r",
    multiple=True,
    required=False,
    help="List of representation names that should be delivered.",
    default=None,
)
def deliver_version_id_command(
    version_id,
    delivery_types,
    representation_names=None,
):
    """Given a SG version id, deliver it so it triggers the OP publish pipeline again.

    Args:
        version_id (int): Shotgrid version id to deliver.
        delivery_types (list[str]): What type(s) of delivery it is so we
            regenerate those representations.
        representation_names (list): List of representation names that should exist on
            the representations being published.
        force (bool): Whether to force the creation of the delivery representations or not.

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the deliver was successful.
    """
    return sg_delivery.deliver_version_id(
        version_id, delivery_types, representation_names
    )


@click.command("republish_playlist_id")
@click.option(
    "--playlist_id",
    "-p",
    required=True,
    type=int,
    help="Shotgrid playlist id to republish.",
)
@click.option(
    "--representation_names",
    "-r",
    multiple=True,
    required=False,
    help="List of representation names that should exist on the republished version",
    default=None,
)
@click.option(
    "--delivery_types",
    "-types",
    type=click.Choice(["final", "review"]),
    required=False,
    multiple=True,
    default=["final", "review"],
)
@click.option("--override/--no-override", default=False)
def republish_playlist_id_command(
    playlist_id,
    delivery_types,
    representation_names=None,
    override=False,
):
    """Given a SG playlist id, republish all the versions associated to it.

    Args:
        playlist_id (int): Shotgrid playlist id to republish.
        delivery_types (list[str]): What type(s) of delivery it is
            (i.e., ["final", "review"])
        representation_names (list): List of representation names that should exist on
            the representations being published.
        force (bool): Whether to force the creation of the delivery representations or not.


    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the republish was successful.
    """
    return sg_delivery.republish_playlist_id(
        playlist_id, delivery_types, representation_names, override
    )


@click.command("republish_version_id")
@click.option(
    "--version_id",
    "-v",
    required=True,
    type=int,
    help="Shotgrid version id to republish.",
)
@click.option(
    "--delivery_types",
    "-types",
    type=click.Choice(["final", "review"]),
    required=False,
    multiple=True,
    default=["final", "review"],
)
@click.option(
    "--representation_names",
    "-r",
    multiple=True,
    required=False,
    help="List of representation names that should exist on the republished version",
    default=None,
)
@click.option("--force/--no-force", default=False)
def republish_version_id_command(
    version_id,
    delivery_types,
    representation_names=None,
    force=False,
):
    """Given a SG version id, republish it so it triggers the OP publish pipeline again.

    Args:
        version_id (int): Shotgrid version id to republish.
        delivery_types (list[str]): What type(s) of delivery it is so we
            regenerate those representations.
        representation_names (list): List of representation names that should exist on
            the representations being published.
        force (bool): Whether to force the creation of the delivery representations or not.

    Returns:
        tuple: A tuple containing a dictionary of report items and a boolean indicating
            whether the republish was successful.
    """
    return sg_delivery.republish_version_id(
        version_id, delivery_types, representation_names, force
    )


@click.group(DeliveryModule.name, help="Delivery CLI")
def cli_main():
    pass

cli_main.add_command(deliver_playlist_id_command)
cli_main.add_command(deliver_version_id_command)
cli_main.add_command(republish_version_id_command)
cli_main.add_command(republish_playlist_id_command)

@cli_main.command()
def launch_sg_delivery():
    """Launch SG Delivery tool UI."""
    from ayon_core.modules.delivery.tray.delivery_dialog import DeliveryDialog
    DeliveryDialog.main()


@cli_main.command()
def launch_outsource():
    """Launch Outsource Delivery tool UI."""
    from ayon_core.modules.delivery.tray.outsource_dialog import OutsourceDialog
    OutsourceDialog.main()


if __name__ == "__main__":
    cli_main()
