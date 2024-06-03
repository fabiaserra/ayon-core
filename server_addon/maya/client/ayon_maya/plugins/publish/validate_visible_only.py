import ayon_maya.api.action
from ayon_core.pipeline.publish import (
    OptionalPyblishPluginMixin,
    PublishValidationError,
    ValidateContentsOrder,
)
from ayon_maya.api.lib import iter_visible_nodes_in_range
from ayon_maya.api import plugin


class ValidateAlembicVisibleOnly(plugin.MayaInstancePlugin,
                                 OptionalPyblishPluginMixin):
    """Validates at least a single node is visible in frame range.

    This validation only validates if the `visibleOnly` flag is enabled
    on the instance - otherwise the validation is skipped.

    """
    order = ValidateContentsOrder + 0.05
    label = "Alembic Visible Only"
    families = ["pointcache", "animation"]
    actions = [ayon_maya.api.action.SelectInvalidAction]
    optional = False

    def process(self, instance):
        if not self.is_active(instance.data):
            return
        if not instance.data.get("visibleOnly", False):
            self.log.debug("Visible only is disabled. Validation skipped..")
            return

        invalid = self.get_invalid(instance)
        if invalid:
            start, end = self.get_frame_range(instance)
            raise PublishValidationError(
                f"No visible nodes found in frame range {start}-{end}."
            )

    @classmethod
    def get_invalid(cls, instance):

        if instance.data["productType"] == "animation":
            # Special behavior to use the nodes in out_SET
            nodes = instance.data["out_hierarchy"]
        else:
            nodes = instance[:]

        start, end = cls.get_frame_range(instance)
        if not any(iter_visible_nodes_in_range(nodes, start, end)):
            # Return the nodes we have considered so the user can identify
            # them with the select invalid action
            return nodes

    @staticmethod
    def get_frame_range(instance):
        data = instance.data
        return data["frameStartHandle"], data["frameEndHandle"]
