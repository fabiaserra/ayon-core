import os
import pyblish.api

from ayon_core.lib import get_ayon_username


class CollectCurrentAYONUser(pyblish.api.ContextPlugin):
    """Inject the currently logged on user into the Context"""

    # Order must be after default pyblish-base CollectCurrentUser
    order = pyblish.api.CollectorOrder + 0.001
    label = "Collect AYON User"

    def process(self, context):
        ### Starts Alkemy-X Override ###
        # Pick up user from env if it exists as otherwise in Deadline `get_ayon_username`
        # returns the "service" user
        user = os.getenv("AYON_USERNAME")
        if not user:
            user = get_ayon_username()
        ### Ends Alkemy-X Override ###
        context.data["user"] = user
        self.log.debug("Collected user \"{}\"".format(user))
