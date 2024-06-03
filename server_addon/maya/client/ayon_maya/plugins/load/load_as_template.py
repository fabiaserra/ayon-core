from ayon_core.lib import BoolDef
from ayon_core.pipeline import registered_host
from ayon_maya.api import plugin
from ayon_maya.api.workfile_template_builder import MayaTemplateBuilder


class LoadAsTemplate(plugin.Loader):
    """Load workfile as a template """

    product_types = {"workfile", "mayaScene"}
    label = "Load as template"
    representations = ["ma", "mb"]
    icon = "wrench"
    color = "#775555"
    order = 10

    options = [
        BoolDef("keep_placeholders",
                label="Keep Placeholders",
                default=False),
        BoolDef("create_first_version",
                label="Create First Version",
                default=False),
    ]

    def load(self, context, name, namespace, data):
        keep_placeholders = data.get("keep_placeholders", False)
        create_first_version = data.get("create_first_version", False)
        path = self.filepath_from_context(context)
        builder = MayaTemplateBuilder(registered_host())
        builder.build_template(template_path=path,
                               keep_placeholders=keep_placeholders,
                               create_first_version=create_first_version)
