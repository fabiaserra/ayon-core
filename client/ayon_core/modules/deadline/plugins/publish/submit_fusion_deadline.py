import os
import json
import getpass

import pyblish.api

from openpype_modules.deadline.abstract_submit_deadline import requests_post
from ayon_core.pipeline.publish import (
    AYONPyblishPluginMixin
)
from ayon_core.lib import NumberDef


class FusionSubmitDeadline(
    pyblish.api.InstancePlugin,
    AYONPyblishPluginMixin
):
    """Submit current Comp to Deadline

    Renders are submitted to a Deadline Web Service as
    supplied via settings key "DEADLINE_REST_URL".

    """

    label = "Submit Fusion to Deadline"
    order = pyblish.api.IntegratorOrder
    hosts = ["fusion"]
    families = ["render"]
    targets = ["local"]

    # presets
    plugin = None

    priority = 50
    chunk_size = 1
    concurrent_tasks = 1
    group = ""

    @classmethod
    def get_attribute_defs(cls):
        return [
            NumberDef(
                "priority",
                label="Priority",
                default=cls.priority,
                decimals=0
            ),
            NumberDef(
                "chunk",
                label="Frames Per Task",
                default=cls.chunk_size,
                decimals=0,
                minimum=1,
                maximum=1000
            ),
            NumberDef(
                "concurrency",
                label="Concurrency",
                default=cls.concurrent_tasks,
                decimals=0,
                minimum=1,
                maximum=10
            )
        ]

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Skipping local instance.")
            return

        attribute_values = self.get_attr_values_from_data(
            instance.data)

        context = instance.context

        key = "__hasRun{}".format(self.__class__.__name__)
        if context.data.get(key, False):
            return
        else:
            context.data[key] = True

        from ayon_fusion.api.lib import get_frame_path

        deadline_url = instance.data["deadline"]["url"]
        assert deadline_url, "Requires Deadline Webservice URL"

        # Collect all saver instances in context that are to be rendered
        saver_instances = []
        for inst in context:
            if inst.data["productType"] != "render":
                # Allow only saver family instances
                continue

            if not inst.data.get("publish", True):
                # Skip inactive instances
                continue

            self.log.debug(inst.data["name"])
            saver_instances.append(inst)

        if not saver_instances:
            raise RuntimeError("No instances found for Deadline submission")

        comment = instance.data.get("comment", "")
        deadline_user = context.data.get("deadlineUser", getpass.getuser())

        script_path = context.data["currentFile"]

        anatomy = instance.context.data["anatomy"]
        publish_template = anatomy.get_template_item(
            "publish", "default", "path"
        )
        for item in context:
            if "workfile" in item.data["families"]:
                msg = "Workfile (scene) must be published along"
                assert item.data["publish"] is True, msg

                template_data = item.data.get("anatomyData")
                rep = item.data.get("representations")[0].get("name")
                template_data["representation"] = rep
                template_data["ext"] = rep
                template_data["comment"] = None
                template_filled = publish_template.format_strict(
                    template_data
                )
                script_path = os.path.normpath(template_filled)

                self.log.info(
                    "Using published scene for render {}".format(script_path)
                )

        filename = os.path.basename(script_path)

        # Documentation for keys available at:
        # https://docs.thinkboxsoftware.com
        #    /products/deadline/8.0/1_User%20Manual/manual
        #    /manual-submission.html#job-info-file-options
        payload = {
            "JobInfo": {
                # Top-level group name
                "BatchName": filename,

                # Asset dependency to wait for at least the scene file to sync.
                "AssetDependency0": script_path,

                # Job name, as seen in Monitor
                "Name": filename,

                "Priority": attribute_values.get(
                    "priority", self.priority),
                "ChunkSize": attribute_values.get(
                    "chunk", self.chunk_size),
                "ConcurrentTasks": attribute_values.get(
                    "concurrency",
                    self.concurrent_tasks
                ),

                # User, as seen in Monitor
                "UserName": deadline_user,

                "Pool": instance.data.get("primaryPool"),
                "SecondaryPool": instance.data.get("secondaryPool"),
                "Group": self.group,

                "Plugin": self.plugin,
                "Frames": "{start}-{end}".format(
                    start=int(instance.data["frameStartHandle"]),
                    end=int(instance.data["frameEndHandle"])
                ),

                "Comment": comment,
            },
            "PluginInfo": {
                # Input
                "FlowFile": script_path,

                # Mandatory for Deadline
                "Version": str(instance.data["app_version"]),

                # Render in high quality
                "HighQuality": True,

                # Whether saver output should be checked after rendering
                # is complete
                "CheckOutput": True,

                # Proxy: higher numbers smaller images for faster test renders
                # 1 = no proxy quality
                "Proxy": 1
            },

            # Mandatory for Deadline, may be empty
            "AuxFiles": []
        }

        # Enable going to rendered frames from Deadline Monitor
        for index, instance in enumerate(saver_instances):
            head, padding, tail = get_frame_path(
                instance.data["expectedFiles"][0]
            )
            path = "{}{}{}".format(head, "#" * padding, tail)
            folder, filename = os.path.split(path)
            payload["JobInfo"]["OutputDirectory%d" % index] = folder
            payload["JobInfo"]["OutputFilename%d" % index] = filename

        # Include critical variables with submission
        keys = [
            "FTRACK_API_KEY",
            "FTRACK_API_USER",
            "FTRACK_SERVER",
            "AYON_BUNDLE_NAME",
            "AYON_DEFAULT_SETTINGS_VARIANT",
            "AYON_PROJECT_NAME",
            "AYON_FOLDER_PATH",
            "AYON_TASK_NAME",
            "AYON_WORKDIR",
            "AYON_APP_NAME",
            "AYON_LOG_NO_COLORS",
            "IS_TEST",
            "AYON_BUNDLE_NAME",
        ]

        environment = {
            key: os.environ[key]
            for key in keys
            if key in os.environ
        }

        # to recognize render jobs
        environment["AYON_RENDER_JOB"] = "1"

        payload["JobInfo"].update({
            "EnvironmentKeyValue%d" % index: "{key}={value}".format(
                key=key,
                value=environment[key]
            ) for index, key in enumerate(environment)
        })

        self.log.debug("Submitting..")
        self.log.debug(json.dumps(payload, indent=4, sort_keys=True))

        # E.g. http://192.168.0.1:8082/api/jobs
        url = "{}/api/jobs".format(deadline_url)
        auth = instance.data["deadline"]["auth"]
        verify = instance.data["deadline"]["verify"]
        response = requests_post(url, json=payload, auth=auth, verify=verify)
        if not response.ok:
            raise Exception(response.text)

        # Store the response for dependent job submission plug-ins
        for instance in saver_instances:
            instance.data["deadlineSubmissionJob"] = response.json()
