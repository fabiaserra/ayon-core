import os
import re
import json
import getpass
from datetime import datetime

import pyblish.api

from ayon_core.pipeline.publish import (
    AYONPyblishPluginMixin
)
from ayon_core.lib import (
    is_in_tests,
    BoolDef,
    NumberDef
)
from ayon_deadline.abstract_submit_deadline import requests_post


class NukeSubmitDeadline(pyblish.api.InstancePlugin,
                         AYONPyblishPluginMixin):
    """Submit write to Deadline

    Renders are submitted to a Deadline Web Service as
    supplied via settings key "DEADLINE_REST_URL".

    """

    label = "Submit Nuke to Deadline"
    order = pyblish.api.IntegratorOrder + 0.1
    hosts = ["nuke"]
    families = ["render", "prerender"]
    optional = True
    targets = ["local"]
    settings_category = "deadline"

    # presets
    priority = 50
    chunk_size = 1
    concurrent_tasks = 1
    group = ""
    department = ""
    limit_groups = []
    use_gpu = False
    env_allowed_keys = []
    env_search_replace_values = []
    workfile_dependency = True
    use_published_workfile = True

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
            ),
            BoolDef(
                "use_gpu",
                default=cls.use_gpu,
                label="Use GPU"
            ),
            BoolDef(
                "workfile_dependency",
                default=cls.workfile_dependency,
                label="Workfile Dependency"
            ),
            BoolDef(
                "use_published_workfile",
                default=cls.use_published_workfile,
                label="Use Published Workfile"
            )
        ]

    def process(self, instance):
        if not instance.data.get("farm"):
            self.log.debug("Skipping local instance.")
            return
        instance.data["attributeValues"] = self.get_attr_values_from_data(
            instance.data)

        families = instance.data["families"]

        node = instance.data["transientData"]["node"]
        context = instance.context

        deadline_url = instance.data["deadline"]["url"]
        assert deadline_url, "Requires Deadline Webservice URL"

        self.deadline_url = "{}/api/jobs".format(deadline_url)
        self._comment = context.data.get("comment", "")
        self._ver = re.search(r"\d+\.\d+", context.data.get("hostVersion"))
        self._deadline_user = context.data.get(
            "deadlineUser", getpass.getuser())
        submit_frame_start = int(instance.data["frameStartHandle"])
        submit_frame_end = int(instance.data["frameEndHandle"])

        # get output path
        render_path = instance.data['path']
        script_path = context.data["currentFile"]

        use_published_workfile = instance.data["attributeValues"].get(
            "use_published_workfile", self.use_published_workfile
        )
        if use_published_workfile:
            script_path = self._get_published_workfile_path(context)

        # only add main rendering job if target is not frames_farm
        r_job_response_json = None
        if instance.data["render_target"] != "frames_farm":
            r_job_response = self.payload_submit(
                instance,
                script_path,
                render_path,
                node.name(),
                submit_frame_start,
                submit_frame_end
            )
            r_job_response_json = r_job_response.json()
            instance.data["deadlineSubmissionJob"] = r_job_response_json

            # Store output dir for unified publisher (filesequence)
            instance.data["outputDir"] = os.path.dirname(
                render_path).replace("\\", "/")
            instance.data["publishJobState"] = "Suspended"

        if instance.data.get("bakingNukeScripts"):
            for baking_script in instance.data["bakingNukeScripts"]:
                render_path = baking_script["bakeRenderPath"]
                script_path = baking_script["bakeScriptPath"]
                exe_node_name = baking_script["bakeWriteNodeName"]

                b_job_response = self.payload_submit(
                    instance,
                    script_path,
                    render_path,
                    exe_node_name,
                    submit_frame_start,
                    submit_frame_end,
                    r_job_response_json,
                    baking_submission=True
                )

                # Store output dir for unified publisher (filesequence)
                instance.data["deadlineSubmissionJob"] = b_job_response.json()

                instance.data["publishJobState"] = "Suspended"

                # add to list of job Id
                if not instance.data.get("bakingSubmissionJobs"):
                    instance.data["bakingSubmissionJobs"] = []

                instance.data["bakingSubmissionJobs"].append(
                    b_job_response.json()["_id"])

        # redefinition of families
        ### Starts Alkemy-X Override ###
        # Remove redefinition of 'family' as it breaks family filtering and
        # we don't see the need for it. TODO: create PR against main repo
        if "render" in instance.data["productType"]:
            # instance.data["family"] = "write"
            # instance.data["productType"] = "write"
            families.insert(0, "render2d")
        elif "prerender" in instance.data["productType"]:
            # instance.data["family"] = "write"
            # instance.data["productType"] = "write"
            families.insert(0, "prerender")
        ### Ends Alkemy-X Override ###
        instance.data["families"] = families

    def _get_published_workfile_path(self, context):
        """This method is temporary while the class is not inherited from
        AbstractSubmitDeadline"""
        anatomy = context.data["anatomy"]
        # WARNING Hardcoded template name 'default' > may not be used
        publish_template = anatomy.get_template_item(
            "publish", "default", "path"
        )
        for instance in context:
            if (
                instance.data["productType"] != "workfile"
                # Disabled instances won't be integrated
                or instance.data("publish") is False
            ):
                continue
            template_data = instance.data["anatomyData"]
            # Expect workfile instance has only one representation
            representation = instance.data["representations"][0]
            # Get workfile extension
            repre_file = representation["files"]
            self.log.info(repre_file)
            ext = os.path.splitext(repre_file)[1].lstrip(".")

            # Fill template data
            template_data["representation"] = representation["name"]
            template_data["ext"] = ext
            template_data["comment"] = None

            template_filled = publish_template.format(template_data)
            script_path = os.path.normpath(template_filled)
            self.log.info(
                "Using published scene for render {}".format(
                    script_path
                )
            )
            return script_path

        return None

    def payload_submit(
        self,
        instance,
        script_path,
        render_path,
        exe_node_name,
        start_frame,
        end_frame,
        response_data=None,
        baking_submission=False,
    ):
        """Submit payload to Deadline

        Args:
            instance (pyblish.api.Instance): pyblish instance
            script_path (str): path to nuke script
            render_path (str): path to rendered images
            exe_node_name (str): name of the node to render
            start_frame (int): start frame
            end_frame (int): end frame
            response_data Optional[dict]: response data from
                                          previous submission
            baking_submission Optional[bool]: if it's baking submission

        Returns:
            requests.Response
        """
        render_dir = os.path.normpath(os.path.dirname(render_path))

        # batch name
        src_filepath = instance.context.data["currentFile"]
        batch_name = os.path.basename(src_filepath)
        job_name = os.path.basename(render_path)

        if is_in_tests():
            batch_name += datetime.now().strftime("%d%m%Y%H%M%S")

        output_filename_0 = self.preview_fname(render_path)

        if not response_data:
            response_data = {}

        try:
            # Ensure render folder exists
            os.makedirs(render_dir)
        except OSError:
            pass

        # resolve any limit groups
        limit_groups = self.get_limit_groups()
        self.log.debug("Limit groups: `{}`".format(limit_groups))

        payload = {
            "JobInfo": {
                # Top-level group name
                "BatchName": batch_name,

                # Job name, as seen in Monitor
                "Name": job_name,

                # Arbitrary username, for visualisation in Monitor
                "UserName": self._deadline_user,

                "Priority": instance.data["attributeValues"].get(
                    "priority", self.priority),
                "ChunkSize": instance.data["attributeValues"].get(
                    "chunk", self.chunk_size),
                "ConcurrentTasks": instance.data["attributeValues"].get(
                    "concurrency",
                    self.concurrent_tasks
                ),

                "Department": self.department,

                "Pool": instance.data.get("primaryPool"),
                "SecondaryPool": instance.data.get("secondaryPool"),
                "Group": self.group,

                "Plugin": "AxNuke",
                "Frames": "{start}-{end}".format(
                    start=start_frame,
                    end=end_frame
                ),
                "Comment": self._comment,

                # Optional, enable double-click to preview rendered
                # frames from Deadline Monitor
                "OutputFilename0": output_filename_0.replace("\\", "/"),

                # limiting groups
                "LimitGroups": ",".join(limit_groups)

            },
            "PluginInfo": {
                # Input
                "SceneFile": script_path,

                # Output directory and filename
                "OutputFilePath": render_dir.replace("\\", "/"),
                # "OutputFilePrefix": render_variables["filename_prefix"],

                # Mandatory for Deadline
                "Version": self._ver.group(),

                # Resolve relative references
                "ProjectPath": script_path,
                "AWSAssetFile0": render_path,

                # using GPU by default
                "UseGpu": instance.data["attributeValues"].get(
                    "use_gpu", self.use_gpu),

                # Only the specific write node is rendered.
                "WriteNode": exe_node_name
            },

            # Mandatory for Deadline, may be empty
            "AuxFiles": []
        }

        # Add workfile dependency.
        workfile_dependency = instance.data["attributeValues"].get(
            "workfile_dependency", self.workfile_dependency
        )
        if workfile_dependency:
            payload["JobInfo"].update({"AssetDependency0": script_path})

        # TODO: rewrite for baking with sequences
        if baking_submission:
            payload["JobInfo"].update({
                "JobType": "Normal",
                "ChunkSize": 99999999
            })

        if response_data.get("_id"):
            payload["JobInfo"].update({
                "BatchName": response_data["Props"]["Batch"],
                "JobDependency0": response_data["_id"],
            })

        # Include critical environment variables with submission
        keys = [
            "PYTHONPATH",
            "PATH",
            "AYON_BUNDLE_NAME",
            "AYON_DEFAULT_SETTINGS_VARIANT",
            "AYON_PROJECT_NAME",
            "AYON_FOLDER_PATH",
            "AYON_TASK_NAME",
            "AYON_APP_NAME",
            "FTRACK_API_KEY",
            "FTRACK_API_USER",
            "FTRACK_SERVER",
            "PYBLISHPLUGINPATH",
            "NUKE_PATH",
            "TOOL_ENV",
            "FOUNDRY_LICENSE",
            "OPENPYPE_SG_USER",
        ]

        # add allowed keys from preset if any
        if self.env_allowed_keys:
            keys += self.env_allowed_keys

        environment = {
            key: os.environ[key]
            for key in keys
            if key in os.environ
        }

        # to recognize render jobs
        environment["AYON_RENDER_JOB"] = "1"

        # finally search replace in values of any key
        if self.env_search_replace_values:
            for key, value in environment.items():
                for item in self.env_search_replace_values:
                    environment[key] = value.replace(
                        item["name"], item["value"]
                    )

        payload["JobInfo"].update({
            "EnvironmentKeyValue%d" % index: "{key}={value}".format(
                key=key,
                value=environment[key]
            ) for index, key in enumerate(environment)
        })

        plugin = payload["JobInfo"]["Plugin"]
        self.log.debug("using render plugin : {}".format(plugin))

        self.log.debug("Submitting..")
        self.log.debug(json.dumps(payload, indent=4, sort_keys=True))

        # adding expected files to instance.data
        self.expected_files(
            instance,
            render_path,
            start_frame,
            end_frame
        )

        self.log.debug("__ expectedFiles: `{}`".format(
            instance.data["expectedFiles"]))
        auth = instance.data["deadline"]["auth"]
        verify = instance.data["deadline"]["verify"]
        response = requests_post(self.deadline_url,
                                 json=payload,
                                 timeout=10,
                                 auth=auth,
                                 verify=verify)

        if not response.ok:
            raise Exception(response.text)

        return response

    def preflight_check(self, instance):
        """Ensure the startFrame, endFrame and byFrameStep are integers"""

        for key in ("frameStart", "frameEnd"):
            value = instance.data[key]

            if int(value) == value:
                continue

            self.log.warning(
                "%f=%d was rounded off to nearest integer"
                % (value, int(value))
            )

    def preview_fname(self, path):
        """Return output file path with #### for padding.

        Deadline requires the path to be formatted with # in place of numbers.
        For example `/path/to/render.####.png`

        Args:
            path (str): path to rendered images

        Returns:
            str

        """
        self.log.debug("_ path: `{}`".format(path))
        if "%" in path:
            ### Starts Alkemy-X Override ###
            hashes_path = re.sub(r"%(\d*)d", lambda m: "#" * int(m.group(1)) if m.group(1) else "#", path)

            return hashes_path
            ### Ends Alkemy-X Override ###

        if "#" in path:
            self.log.debug("_ path: `{}`".format(path))
        return path

    def expected_files(
        self,
        instance,
        filepath,
        start_frame,
        end_frame
    ):
        """ Create expected files in instance data
        """
        if not instance.data.get("expectedFiles"):
            instance.data["expectedFiles"] = []

        dirname = os.path.dirname(filepath)
        file = os.path.basename(filepath)

        # since some files might be already tagged as publish_on_farm
        # we need to avoid adding them to expected files since those would be
        # duplicated into metadata.json file
        representations = instance.data.get("representations", [])
        # check if file is not in representations with publish_on_farm tag
        for repre in representations:
            # Skip if 'publish_on_farm' not available
            if "publish_on_farm" not in repre.get("tags", []):
                continue

            # in case where single file (video, image) is already in
            # representation file. Will be added to expected files via
            # submit_publish_job.py
            if file in repre.get("files", []):
                self.log.debug(
                    "Skipping expected file: {}".format(filepath))
                return

        # in case path is hashed sequence expression
        # (e.g. /path/to/file.####.png)
        if "#" in file:
            pparts = file.split("#")
            padding = "%0{}d".format(len(pparts) - 1)
            file = pparts[0] + padding + pparts[-1]

        # in case input path was single file (video or image)
        if "%" not in file:
            instance.data["expectedFiles"].append(filepath)
            return

        # shift start frame by 1 if slate is present
        if instance.data.get("slate"):
            start_frame -= 1

        # add sequence files to expected files
        for i in range(start_frame, (end_frame + 1)):
            instance.data["expectedFiles"].append(
                os.path.join(dirname, (file % i)).replace("\\", "/"))

    def get_limit_groups(self):
        """Search for limit group nodes and return group name.
        Limit groups will be defined as pairs in Nuke deadline submitter
        presents where the key will be name of limit group and value will be
        a list of plugin's node class names. Thus, when a plugin uses more
        than one node, these will be captured and the triggered process
        will add the appropriate limit group to the payload jobinfo attributes.
        Returning:
            list: captured groups list
        """
        # Not all hosts can import this module.
        import nuke

        captured_groups = []
        for limit_group in self.limit_groups:
            lg_name = limit_group["name"]

            for node_class in limit_group["value"]:
                for node in nuke.allNodes(recurseGroups=True):
                    # ignore all nodes not member of defined class
                    if node.Class() not in node_class:
                        continue
                    # ignore all disabled nodes
                    if node["disable"].value():
                        continue
                    # add group name if not already added
                    if lg_name not in captured_groups:
                        captured_groups.append(lg_name)
        return captured_groups
