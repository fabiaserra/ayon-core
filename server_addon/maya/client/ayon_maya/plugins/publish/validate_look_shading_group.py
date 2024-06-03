import ayon_maya.api.action
from ayon_core.pipeline.publish import (
    OptionalPyblishPluginMixin,
    PublishValidationError,
    RepairAction,
    ValidateContentsOrder,
)
from ayon_maya.api import plugin
from maya import cmds


class ValidateShadingEngine(plugin.MayaInstancePlugin,
                            OptionalPyblishPluginMixin):
    """Validate all shading engines are named after the surface material.

    Shading engines should be named "{surface_shader}SG"
    """

    order = ValidateContentsOrder
    families = ["look"]
    label = "Look Shading Engine Naming"
    actions = [
        ayon_maya.api.action.SelectInvalidAction, RepairAction
    ]
    optional = True

    # The default connections to check
    def process(self, instance):
        if not self.is_active(instance.data):
            return

        invalid = self.get_invalid(instance)
        if invalid:
            raise PublishValidationError(
                "Found shading engines with incorrect naming:"
                "\n{}".format(invalid)
            )

    @classmethod
    def get_invalid(cls, instance):
        shapes = cmds.ls(instance, type=["nurbsSurface", "mesh"], long=True)
        invalid = []
        for shape in shapes:
            shading_engines = cmds.listConnections(
                shape, destination=True, type="shadingEngine"
            ) or []
            for shading_engine in shading_engines:
                materials = cmds.listConnections(
                    shading_engine + ".surfaceShader",
                    source=True, destination=False
                )
                if not materials:
                    cls.log.warning(
                        "Shading engine '{}' has no material connected to its "
                        ".surfaceShader attribute.".format(shading_engine))
                    continue

                material = materials[0]  # there should only ever be one input
                name = material + "SG"
                if shading_engine != name:
                    invalid.append(shading_engine)

        return list(set(invalid))

    @classmethod
    def repair(cls, instance):
        shading_engines = cls.get_invalid(instance)
        for shading_engine in shading_engines:
            name = (
                cmds.listConnections(shading_engine + ".surfaceShader")[0]
                + "SG"
            )
            cmds.rename(shading_engine, name)
