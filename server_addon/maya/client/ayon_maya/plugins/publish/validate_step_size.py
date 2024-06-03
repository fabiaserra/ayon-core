import ayon_maya.api.action
from ayon_core.pipeline.publish import (
    OptionalPyblishPluginMixin,
    PublishValidationError,
    ValidateContentsOrder,
)
from ayon_maya.api import plugin


class ValidateStepSize(plugin.MayaInstancePlugin,
                       OptionalPyblishPluginMixin):
    """Validates the step size for the instance is in a valid range.

    For example the `step` size should never be lower or equal to zero.

    """

    order = ValidateContentsOrder
    label = 'Step size'
    families = ['camera',
                'pointcache',
                'animation']
    actions = [ayon_maya.api.action.SelectInvalidAction]
    optional = False
    MIN = 0.01
    MAX = 1.0

    @classmethod
    def get_invalid(cls, instance):

        objset = instance.data['instance_node']
        step = instance.data.get("step", 1.0)

        if step < cls.MIN or step > cls.MAX:
            cls.log.warning("Step size is outside of valid range: {0} "
                            "(valid: {1} to {2})".format(step,
                                                         cls.MIN,
                                                         cls.MAX))
            return objset

        return []

    def process(self, instance):
        if not self.is_active(instance.data):
            return
        invalid = self.get_invalid(instance)
        if invalid:
            raise PublishValidationError(
                "Instance found with invalid step size: {0}".format(invalid))
