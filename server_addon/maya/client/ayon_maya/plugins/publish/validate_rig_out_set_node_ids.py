import ayon_maya.api.action
import maya.cmds as cmds
from ayon_core.pipeline.publish import (
    OptionalPyblishPluginMixin,
    PublishXmlValidationError,
    RepairAction,
    ValidateContentsOrder,
    apply_plugin_settings_automatically,
    get_plugin_settings,
)
from ayon_maya.api import lib
from ayon_maya.api import plugin


class ValidateRigOutSetNodeIds(plugin.MayaInstancePlugin,
                               OptionalPyblishPluginMixin):
    """Validate if deformed shapes have related IDs to the original shapes.

    When a deformer is applied in the scene on a referenced mesh that already
    had deformers then Maya will create a new shape node for the mesh that
    does not have the original id. This validator checks whether the ids are
    valid on all the shape nodes in the instance.

    """

    order = ValidateContentsOrder
    families = ["rig"]
    label = 'Rig Out Set Node Ids'
    actions = [
        ayon_maya.api.action.SelectInvalidAction,
        RepairAction
    ]
    allow_history_only = False
    optional = False

    @classmethod
    def apply_settings(cls, project_settings):
        # Preserve automatic settings applying logic
        settings = get_plugin_settings(plugin=cls,
                                       project_settings=project_settings,
                                       log=cls.log,
                                       category="maya")
        apply_plugin_settings_automatically(cls, settings, logger=cls.log)

        # Disable plug-in if cbId workflow is disabled
        if not project_settings["maya"].get("use_cbid_workflow", True):
            cls.enabled = False
            return

    def process(self, instance):
        """Process all meshes"""
        if not self.is_active(instance.data):
            return
        # Ensure all nodes have a cbId and a related ID to the original shapes
        # if a deformer has been created on the shape
        invalid = self.get_invalid(instance)
        if invalid:

            # Use the short names
            invalid = cmds.ls(invalid)
            invalid.sort()

            # Construct a human-readable list
            invalid = "\n".join("- {}".format(node) for node in invalid)

            raise PublishXmlValidationError(
                plugin=ValidateRigOutSetNodeIds,
                message=(
                    "Rig nodes have different IDs than their input "
                    "history: \n{0}".format(invalid)
                )
            )

    @classmethod
    def get_invalid(cls, instance):
        """Get all nodes which do not match the criteria"""

        out_set = cls.get_node(instance)
        if not out_set:
            return []

        invalid = []
        members = cmds.sets(out_set, query=True)
        shapes = cmds.ls(members,
                         dag=True,
                         leaf=True,
                         shapes=True,
                         long=True,
                         noIntermediate=True)

        for shape in shapes:
            sibling_id = lib.get_id_from_sibling(
                shape,
                history_only=cls.allow_history_only
            )
            if sibling_id:
                current_id = lib.get_id(shape)
                if current_id != sibling_id:
                    invalid.append(shape)

        return invalid

    @classmethod
    def repair(cls, instance):

        for node in cls.get_invalid(instance):
            # Get the original id from sibling
            sibling_id = lib.get_id_from_sibling(
                node,
                history_only=cls.allow_history_only
            )
            if not sibling_id:
                cls.log.error("Could not find ID in siblings for '%s'", node)
                continue

            lib.set_id(node, sibling_id, overwrite=True)

    @classmethod
    def get_node(cls, instance):
        """Get target object nodes from out_SET

        Args:
            instance (str): instance

        Returns:
            list: list of object nodes from out_SET
        """
        return instance.data["rig_sets"].get("out_SET")


class ValidateSkeletonRigOutSetNodeIds(ValidateRigOutSetNodeIds):
    """Validate if deformed shapes have related IDs to the original shapes
    from skeleton set.

    When a deformer is applied in the scene on a referenced mesh that already
    had deformers then Maya will create a new shape node for the mesh that
    does not have the original id. This validator checks whether the ids are
    valid on all the shape nodes in the instance.

    """

    order = ValidateContentsOrder
    families = ["rig.fbx"]
    hosts = ['maya']
    label = 'Skeleton Rig Out Set Node Ids'
    optional = False

    @classmethod
    def get_node(cls, instance):
        """Get target object nodes from skeletonMesh_SET

        Args:
            instance (str): instance

        Returns:
            list: list of object nodes from skeletonMesh_SET
        """
        return instance.data["rig_sets"].get(
            "skeletonMesh_SET")
