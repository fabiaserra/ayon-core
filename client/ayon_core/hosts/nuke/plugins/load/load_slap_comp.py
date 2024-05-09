import os

import nuke

from ayon_core.pipeline import load
from ayon_core.hosts.nuke.api import pipeline
from ayon_core.hosts.nuke.plugins.load import load_clip


class LoadSlapComp(load.LoaderPlugin):
    """Load a render through SlapComp template"""

    product_types = {"render"}
    representations = {"exr"}
    extension = {"exr"}

    label = "Load Slap Comp"
    order = -19
    icon = "file"
    color = "#cc0000"

    DEFAULT_SLAPCOMP_TEMPLATE = "/pipe/nuke/templates/{render_pass}_slap_comp_template.nk"
    PROJ_SLAPCOMP_TEMPLATE = "/proj/{proj_code}/resources/slap_comp/{render_pass}_slap_comp_template.nk"

    def swap_node(self, orig_node, new_node):
        # set the input of node 2 to be the same as node 1
        new_node.setInput(0, orig_node.input(0))

        # set the outputs of node 1 to connect to node 2 instead (assuming they are all using input 0, which is most likely not the case)
        for dependent in orig_node.dependent(nuke.INPUTS):
            dependent.setInput(0, new_node)

        # Set on the same position
        new_node.setXpos(orig_node.xpos())
        new_node.setYpos(orig_node.ypos())

        # Delete original node
        nuke.delete(orig_node)

    def load(self, context, name, namespace, data):

        file = self.filepath_from_context(context).replace("\\", "/")
        self.log.info("file: {}\n".format(file))

        clip_loader = load_clip.LoadClip()
        load_node = clip_loader.load(context, name, namespace, data)

        render_pass = "bty"
        if "util" in name:
            render_pass = "util"

        # Get the Nuke script to use to generate the review
        # First try to see if there's one set on the show, otherwise
        # we just use the default global one
        nuke_slapcomp_script = self.DEFAULT_SLAPCOMP_TEMPLATE.format(render_pass=render_pass)
        proj_slapcomp_script = self.PROJ_SLAPCOMP_TEMPLATE.format(
            proj_code=os.getenv("SHOW"), render_pass=render_pass
        )
        if os.path.exists(proj_slapcomp_script):
            nuke_slapcomp_script = proj_slapcomp_script
        else:
            self.log.warning(
                "Project Nuke template for slap comps not found at '%s'",
                proj_slapcomp_script
            )

        nuke.scriptReadFile(nuke_slapcomp_script)

        # Fill up placeholders from template script
        pipeline.build_workfile_template()

        # Replace Beauty read node placeholder with load clip node
        read_node = nuke.toNode("__Read__")
        self.swap_node(read_node, load_node)
