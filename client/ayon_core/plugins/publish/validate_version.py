import pyblish.api
from ayon_core.pipeline.publish import PublishValidationError


class ValidateVersion(pyblish.api.InstancePlugin):
    """Validate instance version.

    AYON does not allow overwriting previously published versions.
    """

    order = pyblish.api.ValidatorOrder

    label = "Validate Version"
    hosts = ["nuke", "maya", "houdini", "blender",
             "photoshop", "aftereffects"]

    optional = True
    active = True

    def process(self, instance):

        if not self.is_active(instance.data):
            return

        ### Starts Alkemy-X Override ###
        # Ignore workfile versions for now due to our
        # setup in Nuke where we publish workfiles as the same version (prerender)
        # quite often and this would throw warnings on every publish
        if instance.data.get("family") == "workfile":
            return
        ### Ends Alkemy-X Override ###

        version = instance.data.get("version")
        latest_version = instance.data.get("latestVersion")

        if latest_version is not None and int(version) <= int(latest_version):
            # TODO: Remove full non-html version upon drop of old publisher
            msg = (
                "Version '{0}' from instance '{1}' that you are "
                "trying to publish is lower or equal to an existing version "
                "in the database. Version in database: '{2}'."
                "Please version up the file to a higher version number "
                "than: '{2}' or disable the 'Validate version' on the instance."
            ).format(version, instance.data["name"], latest_version)

            msg_html = (
                "Version <b>{0}</b> from instance <b>{1}</b> that you are "
                "trying to publish is lower or equal to an existing version "
                "in the database. Version in database: <b>{2}</b>.<br><br>"
                "Please version up the file to a higher version number "
                "than: <b>{2}</b> or disable the 'Validate version' on the instance."
            ).format(version, instance.data["name"], latest_version)
            raise PublishValidationError(
                title="Higher version of publish already exists",
                message=msg,
                description=msg_html
            )
