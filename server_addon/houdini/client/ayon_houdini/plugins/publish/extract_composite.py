import os
import hou
import pyblish.api

from ayon_core.pipeline import publish
from ayon_houdini.api import plugin
from ayon_houdini.api.lib import render_rop, splitext


class ExtractComposite(plugin.HoudiniExtractorPlugin,
                       publish.ColormanagedPyblishPluginMixin):

    order = pyblish.api.ExtractorOrder
    label = "Extract Composite (Image Sequence)"
    families = ["imagesequence"]

    def process(self, instance):

        ropnode = hou.node(instance.data["instance_node"])

        # Get the filename from the copoutput parameter
        # `.evalParm(parameter)` will make sure all tokens are resolved
        output = ropnode.evalParm("copoutput")
        staging_dir = os.path.dirname(output)
        instance.data["stagingDir"] = staging_dir
        file_name = os.path.basename(output)

        self.log.info("Writing comp '%s' to '%s'" % (file_name, staging_dir))

        render_rop(ropnode)

        output = instance.data["frames"]
        _, ext = splitext(output[0], [])
        ext = ext.lstrip(".")

        if "representations" not in instance.data:
            instance.data["representations"] = []

        representation = {
            "name": ext,
            "ext": ext,
            "files": output,
            "stagingDir": staging_dir,
            "frameStart": instance.data["frameStartHandle"],
            "frameEnd": instance.data["frameEndHandle"],
        }

        if ext.lower() == "exr":
            # Inject colorspace with 'scene_linear' as that's the
            # default Houdini working colorspace and all extracted
            # OpenEXR images should be in that colorspace.
            # https://www.sidefx.com/docs/houdini/render/linear.html#image-formats
            self.set_representation_colorspace(
                representation, instance.context,
                colorspace="scene_linear"
            )

        instance.data["representations"].append(representation)
