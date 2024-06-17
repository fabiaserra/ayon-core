# -*- coding: utf-8 -*-
"""Collect user credentials

Requires:
    context -> project_settings
    instance.data["deadline"]["url"]

Provides:
    instance.data["deadline"] -> require_authentication (bool)
    instance.data["deadline"] -> auth (tuple (str, str)) -
        (username, password) or None
"""
import pyblish.api

from ayon_api import get_server_api_connection

from ayon_deadline.lib import FARM_FAMILIES


class CollectDeadlineUserCredentials(pyblish.api.InstancePlugin):
    """Collects user name and password for artist if DL requires authentication
    """
    order = pyblish.api.CollectorOrder + 0.250
    label = "Collect Deadline User Credentials"

    targets = ["local"]
    hosts = ["aftereffects",
             "blender",
             "fusion",
             "harmony",
             "nuke",
             "maya",
             "max",
             "houdini",
             "hiero",
    ]

    families = FARM_FAMILIES

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Should not be processed on farm, skipping.")
            return

        collected_deadline_url = instance.data["deadline"]["url"]
        if not collected_deadline_url:
            raise ValueError("Instance doesn't have '[deadline][url]'.")
        context_data = instance.context.data
        deadline_settings = context_data["project_settings"]["deadline"]

        deadline_server_name = None
        # deadline url might be set directly from instance, need to find
        # metadata for it
        for deadline_info in deadline_settings["deadline_urls"]:
            dl_settings_url = deadline_info["value"].strip().rstrip("/")
            if dl_settings_url == collected_deadline_url:
                deadline_server_name = deadline_info["name"]
                break

        if not deadline_server_name:
            raise ValueError(f"Collected {collected_deadline_url} doesn't " 
                              "match any site configured in Studio Settings")

        instance.data["deadline"]["require_authentication"] = (
            deadline_info["require_authentication"]
        )
        instance.data["deadline"]["auth"] = None

        instance.data["deadline"]["verify"] = (
            not deadline_info["not_verify_ssl"])

        if not deadline_info["require_authentication"]:
            return

        addons_manager = instance.context.data["ayonAddonsManager"]
        deadline_addon = addons_manager["deadline"]
        # TODO import 'get_addon_site_settings' when available
        #   in public 'ayon_api'
        local_settings = get_server_api_connection().get_addon_site_settings(
            deadline_addon.name, deadline_addon.version)
        local_settings = local_settings["local_settings"]
        for server_info in local_settings:
            if deadline_server_name == server_info["server_name"]:
                instance.data["deadline"]["auth"] = (server_info["username"],
                                                     server_info["password"])
