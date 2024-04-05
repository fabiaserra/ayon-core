
import nuke
import pyblish.api


class IncrementScriptVersion(pyblish.api.ContextPlugin):
    """Increment current script version."""

    order = pyblish.api.IntegratorOrder + 0.9
    label = "Increment Script Version"
    optional = True
    ### Starts Alkemy-X Override ###
    # Add other families as well so script version gets incremented also
    # when doing a publish of those
    families = [
        # "workfile", # commenting it out for now as we don't want to bump it if publishing prerender + workfile
        "render",
        "render.farm",
        "render.frames_farm",
    ]
    ### Ends Alkemy-X Override ###
    hosts = ['nuke']

    def process(self, context):

        assert all(result["success"] for result in context.data["results"]), (
            "Publishing not successful so version is not increased.")

        from ayon_core.lib import version_up
        path = context.data["currentFile"]
        nuke.scriptSaveAs(version_up(path))
        self.log.info('Incrementing script version')
