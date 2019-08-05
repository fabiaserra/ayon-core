import nuke
import pyblish.api


class RemoveOutputNode(pyblish.api.ContextPlugin):
    """Removing output node for each ouput write node

    """
    label = 'Output Node Remove'
    order = pyblish.api.IntegratorOrder
    families = ["workfile"]
    hosts = ['nuke']

    def process(self, context):
        try:
            output_node = context.data["outputNode"]
            name = output_node["name"].value()
            self.log.info("Removing output node: '{}'".format(name))

            nuke.delete(output_node)
        except Exception:
            return
