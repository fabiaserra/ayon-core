import ayon_maya.api.action
from ayon_core.pipeline.publish import (
    OptionalPyblishPluginMixin,
    PublishValidationError,
    RepairAction,
    ValidateMeshOrder,
)
from ayon_maya.api import plugin
from maya import cmds


class ValidateShapeRenderStats(plugin.MayaInstancePlugin,
                               OptionalPyblishPluginMixin):
    """Ensure all render stats are set to the default values."""

    order = ValidateMeshOrder
    families = ['model']
    label = 'Shape Default Render Stats'
    actions = [ayon_maya.api.action.SelectInvalidAction,
               RepairAction]

    defaults = {'castsShadows': 1,
                'receiveShadows': 1,
                'motionBlur': 1,
                'primaryVisibility': 1,
                'smoothShading': 1,
                'visibleInReflections': 1,
                'visibleInRefractions': 1,
                'doubleSided': 1,
                'opposite': 0}

    @classmethod
    def get_invalid(cls, instance):
        # It seems the "surfaceShape" and those derived from it have
        # `renderStat` attributes.
        shapes = cmds.ls(instance, long=True, type='surfaceShape')
        invalid = set()
        for shape in shapes:
            for attr, default_value in cls.defaults.items():
                if cmds.attributeQuery(attr, node=shape, exists=True):
                    value = cmds.getAttr('{}.{}'.format(shape, attr))
                    if value != default_value:
                        invalid.add(shape)

        return invalid

    def process(self, instance):
        if not self.is_active(instance.data):
            return
        invalid = self.get_invalid(instance)
        if not invalid:
            return

        defaults_str = "\n".join(
            "- {}: {}\n".format(key, value)
            for key, value in self.defaults.items()
        )
        description = (
            "## Shape Default Render Stats\n"
            "Shapes are detected with non-default render stats.\n\n"
            "To ensure a model's shapes behave like a shape would by default "
            "we require the render stats to have not been altered in "
            "the published models.\n\n"
            "### How to repair?\n"
            "You can reset the default values on the shapes by using the "
            "repair action."
        )

        raise PublishValidationError(
            "Shapes with non-default renderStats "
            "found: {0}".format(", ".join(sorted(invalid))),
            description=description,
            detail="The expected default values "
                   "are:\n\n{}".format(defaults_str)
        )

    @classmethod
    def repair(cls, instance):
        for shape in cls.get_invalid(instance):
            for attr, default_value in cls.defaults.items():
                if cmds.attributeQuery(attr, node=shape, exists=True):
                    plug = '{0}.{1}'.format(shape, attr)
                    value = cmds.getAttr(plug)
                    if value != default_value:
                        cmds.setAttr(plug, default_value)
