# -*- coding: utf-8 -*-
"""Collect Deadline servers from instance.

This is resolving index of server lists stored in `deadlineServers` instance
attribute or using default server if that attribute doesn't exists.

"""
import pyblish.api
from ayon_core.pipeline.publish import KnownPublishError
from ayon_core.pipeline.context_tools import get_current_host_name


class CollectDeadlineServerFromInstance(pyblish.api.InstancePlugin):
    """Collect Deadline Webservice URL from instance."""

    # Run before collect_render.
    order = pyblish.api.CollectorOrder + 0.005
    label = "Deadline Webservice from the Instance"
    targets = ["local"]
    families = ["render",
                "rendering",
                "render.farm",
                "renderFarm",
                "renderlayer",
                "maxrender",
                "usdrender",
                "redshift_rop",
                "arnold_rop",
                "mantra_rop",
                "karma_rop",
                "vray_rop",
                "publish.hou",
                "image"]  # for Fusion

    def process(self, instance):
        if not "deadline" in instance.data:
            instance.data["deadline"] = {}

        host_name = get_current_host_name()
        if host_name == "maya":
            deadline_url = self._collect_deadline_url(instance)
        else:
            deadline_url = (instance.data.get("deadlineUrl") or  # backwards
                            instance.data.get("deadline", {}).get("url"))
        if deadline_url:
            instance.data["deadline"]["url"] = deadline_url.strip().rstrip("/")
        else:
            instance.data["deadline"]["url"] = instance.context.data["deadline"]["defaultDeadline"]  # noqa
        self.log.debug(
            "Using {} for submission".format(instance.data["deadline"]["url"]))

    def _collect_deadline_url(self, render_instance):
        # type: (pyblish.api.Instance) -> str
        """Get Deadline Webservice URL from render instance.

        This will get all configured Deadline Webservice URLs and create
        subset of them based upon project configuration. It will then take
        `deadlineServers` from render instance that is now basically `int`
        index of that list.

        Args:
            render_instance (pyblish.api.Instance): Render instance created
                by Creator in Maya.

        Returns:
            str: Selected Deadline Webservice URL.

        """
        # Not all hosts can import this module.
        from maya import cmds
        deadline_settings = (
            render_instance.context.data
            ["project_settings"]
            ["deadline"]
        )
        default_server = (render_instance.context.data["deadline"]
                                                      ["defaultDeadline"])
        # QUESTION How and where is this is set? Should be removed?
        instance_server = render_instance.data.get("deadlineServers")
        if not instance_server:
            self.log.debug("Using default server.")
            return default_server

        # Get instance server as sting.
        if isinstance(instance_server, int):
            instance_server = cmds.getAttr(
                "{}.deadlineServers".format(render_instance.data["objset"]),
                asString=True
            )

        default_servers = {
            url_item["name"]: url_item["value"]
            for url_item in deadline_settings["deadline_server_info"]
        }
        project_servers = (
            render_instance.context.data
            ["project_settings"]
            ["deadline"]
            ["deadline_servers"]
        )
        if not project_servers:
            self.log.debug("Not project servers found. Using default servers.")
            return default_servers[instance_server]

        project_enabled_servers = {
            k: default_servers[k]
            for k in project_servers
            if k in default_servers
        }

        if instance_server not in project_enabled_servers:
            msg = (
                "\"{}\" server on instance is not enabled in project settings."
                " Enabled project servers:\n{}".format(
                    instance_server, project_enabled_servers
                )
            )
            raise KnownPublishError(msg)

        self.log.debug("Using project approved server.")
        return project_enabled_servers[instance_server]
