from ayon_core.pipeline.publish import (
    OptionalPyblishPluginMixin,
    PublishValidationError,
    RepairAction,
    ValidateContentsOrder,
)
from ayon_maya.api import lib
from ayon_maya.api import plugin


class ValidateArnoldSceneSourceCbid(plugin.MayaInstancePlugin,
                                    OptionalPyblishPluginMixin):
    """Validate Arnold Scene Source Cbid.

    It is required for the proxy and content nodes to share the same cbid.
    """

    order = ValidateContentsOrder
    families = ["assProxy"]
    label = "Validate Arnold Scene Source CBID"
    actions = [RepairAction]
    optional = False

    @classmethod
    def apply_settings(cls, project_settings):
        # Disable plug-in if cbId workflow is disabled
        if not project_settings["maya"].get("use_cbid_workflow", True):
            cls.enabled = False
            return

    @staticmethod
    def _get_nodes_by_name(nodes):
        nodes_by_name = {}
        for node in nodes:
            node_name = node.rsplit("|", 1)[-1].rsplit(":", 1)[-1]
            nodes_by_name[node_name] = node

        return nodes_by_name

    @classmethod
    def get_invalid_couples(cls, instance):
        nodes_by_name = cls._get_nodes_by_name(instance.data["members"])
        proxy_nodes_by_name = cls._get_nodes_by_name(instance.data["proxy"])

        invalid_couples = []
        for content_name, content_node in nodes_by_name.items():
            proxy_node = proxy_nodes_by_name.get(content_name, None)

            if not proxy_node:
                cls.log.debug(
                    "Content node '{}' has no matching proxy node.".format(
                        content_node
                    )
                )
                continue

            content_id = lib.get_id(content_node)
            proxy_id = lib.get_id(proxy_node)
            if content_id != proxy_id:
                invalid_couples.append((content_node, proxy_node))

        return invalid_couples

    def process(self, instance):
        if not self.is_active(instance.data):
            return
        # Proxy validation.
        if not instance.data["proxy"]:
            return

        # Validate for proxy nodes sharing the same cbId as content nodes.
        invalid_couples = self.get_invalid_couples(instance)
        if invalid_couples:
            raise PublishValidationError(
                "Found proxy nodes with mismatching cbid:\n{}".format(
                    invalid_couples
                )
            )

    @classmethod
    def repair(cls, instance):
        for content_node, proxy_node in cls.get_invalid_couples(instance):
            lib.set_id(proxy_node, lib.get_id(content_node), overwrite=True)
