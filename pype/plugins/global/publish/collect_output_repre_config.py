import os
import json
import pyblish.api
from pypeapp import config


class CollectOutputRepreConfig(pyblish.api.ContextPlugin):
    """Inject the current working file into context"""

    order = pyblish.api.CollectorOrder
    label = "Collect Config for representation"
    hosts = ["shell"]

    def process(self, context):
        config_data = config.get_presets()["ftrack"]["output_representation"]
        context.data['output_repre_config'] = config_data
