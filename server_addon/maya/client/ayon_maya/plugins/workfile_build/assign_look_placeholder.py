from maya import cmds

from ayon_core.lib import (
    UISeparatorDef,
    UILabelDef,
    TextDef,
    BoolDef
)
from ayon_core.lib.events import weakref_partial
from ayon_maya.api.workfile_template_builder import MayaPlaceholderPlugin
from ayon_maya.api.lib import (
    get_all_children,
    assign_look,
)


class AssignLookPlaceholderPlugin(MayaPlaceholderPlugin):
    """Assign a look product to members of the placeholder set.

    Creates an objectSet. Any members will get the look assigned with the given
    product name if it exists.

    Any containers loaded from other template placeholders will get the look
    assigned to their loaded containers.

    """

    identifier = "maya.assignlook"
    label = "Assign Look"

    def get_placeholder_options(self, options=None):
        options = options or {}
        return [
            UISeparatorDef(),
            UILabelDef(label="<b>Description</b>"),
            UISeparatorDef(),
            UILabelDef(
                label=(
                    "Creates an objectSet. Any members will get the look\n"
                    "assigned with the given product name if it exists.\n\n"
                    "Any containers loaded from other template placeholders\n"
                    "will get the look assigned to their loaded containers."
                    ""
                )
            ),
            UISeparatorDef(),
            UILabelDef(label="<b>Settings</b>"),
            UISeparatorDef(),
            TextDef(
                "product_name",
                label="Product Name",
                tooltip="Look product to assign to containers loaded by "
                        "contained placeholders",
                multiline=False,
                default=options.get("product_name", "lookMain")
            ),
            BoolDef(
                "recurse",
                label="Recursive",
                tooltip="Assign look also to potential sub containers / "
                        "placeholders loaded from the load placeholder.\n"
                        "This will make sure that any placeholder contained "
                        "that itself loaded new geometry will recursively "
                        "also get the look assignment triggered.",
                default=options.get("recurse", False)
            ),
        ]

    def create_placeholder(self, placeholder_data):
        placeholder_data["plugin_identifier"] = self.identifier

        # Create maya objectSet on selection
        selection = cmds.ls(selection=True, long=True)
        product_name = placeholder_data["product_name"]
        name = "AssignLook_{}".format(product_name)
        node = cmds.sets(selection, name=name)

        self.imprint(node, placeholder_data)

    def populate_placeholder(self, placeholder):
        callback = weakref_partial(self.assign_look, placeholder)
        self.builder.add_on_depth_processed_callback(
            callback, order=placeholder.order)

        # If placeholder should be deleted, delete it after finish
        if not placeholder.data.get("keep_placeholder", True):
            delete_callback = weakref_partial(self.delete_placeholder,
                                              placeholder)
            self.builder.add_on_finished_callback(
                delete_callback, order=placeholder.order)

    def assign_look(self, placeholder):
        if placeholder.data.get("finished", False):
            # If not recursive we mark it finished after the first depth
            # iteration - otherwise run it again to find any new members
            return

        product_name = placeholder.data["product_name"]
        assert product_name, "Must have defined look product name to assign"

        members = cmds.ls(
            cmds.sets(placeholder.scene_identifier, query=True), long=True
        )
        if not members:
            return

        # Allow any children of members in the set to get assignments,
        # e.g. when a group is included there. Whenever a load placeholder
        # finishes it also adds loaded content into the object set the
        # placeholder was in, so this will also assign to loaded content
        # during this build.
        assign_nodes = set(members)
        assign_nodes.update(get_all_children(members))

        processed = placeholder.data.setdefault("processed", set())
        assign_nodes.difference_update(processed)
        processed.update(assign_nodes)

        if assign_nodes:
            self.log.info(
                "Assigning look {} for placeholder: {}".format(product_name,
                                                               placeholder)
            )
            assign_nodes = list(assign_nodes)
            assign_look(assign_nodes, product_name=product_name)

        if not placeholder.data.get("recurse", False):
            placeholder.data["finished"] = True
