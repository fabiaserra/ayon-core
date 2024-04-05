import os
import clique
import glob
import re

import nuke
import pyblish.api

from ayon_core.hosts.nuke import api as napi
from ayon_core.pipeline import publish, PublishXmlValidationError
from ayon_core.lib import path_tools, get_ffprobe_streams, convert_ffprobe_fps_value

class CollectNukeWrites(pyblish.api.InstancePlugin,
                        publish.ColormanagedPyblishPluginMixin):
    """Collect all write nodes."""

    order = pyblish.api.CollectorOrder + 0.0021
    label = "Collect Writes"
    hosts = ["nuke", "nukeassist"]
    families = ["render", "prerender", "image"]

    # cache
    _write_nodes = {}
    _frame_ranges = {}

    def process(self, instance):

        group_node = instance.data["transientData"]["node"]
        render_target = instance.data["render_target"]

        write_node = self._write_node_helper(instance)

        if write_node is None:
            self.log.warning(
                "Created node '{}' is missing write node!".format(
                    group_node.name()
                )
            )
            return

        # get colorspace and add to version data
        colorspace = napi.get_colorspace_from_node(write_node)

        if render_target == "frames":
            self._set_existing_files_data(instance, colorspace)

        elif render_target == "frames_farm":
            collected_frames = self._set_existing_files_data(
                instance, colorspace)

            self._set_expected_files(instance, collected_frames)

            self._add_farm_instance_data(instance)

        elif render_target == "farm":
            self._add_farm_instance_data(instance)

        # set additional instance data
        self._set_additional_instance_data(instance, render_target, colorspace)

    def _set_existing_files_data(self, instance, colorspace):
        """Set existing files data to instance data.

        Args:
            instance (pyblish.api.Instance): pyblish instance
            colorspace (str): colorspace

        Returns:
            list: collected frames
        """
        collected_frames = self._get_collected_frames(instance)

        representation = self._get_existing_frames_representation(
            instance, collected_frames
        )

        # inject colorspace data
        self.set_representation_colorspace(
            representation, instance.context,
            colorspace=colorspace
        )

        instance.data["representations"].append(representation)

        return collected_frames

    def _set_expected_files(self, instance, collected_frames):
        """Set expected files to instance data.

        Args:
            instance (pyblish.api.Instance): pyblish instance
            collected_frames (list): collected frames
        """
        write_node = self._write_node_helper(instance)

        write_file_path = nuke.filename(write_node)
        output_dir = os.path.dirname(write_file_path)

        instance.data["expectedFiles"] = [
            os.path.join(output_dir, source_file)
            for source_file in collected_frames
        ]

    ### Starts Alkemy-X Override ###
    def _set_frame_range_data(self, instance, first_frame, last_frame):
        """Sets frame range data to the class instance. Later during calls
        to get frame range data if instance has frame range it will use the
        stored data instead. This method allows the explicit setting of frame
        range data that may already exist.
        """
        instance_name = instance.data["name"]

        self._frame_ranges[instance_name] = (first_frame, last_frame)
    ### Ends Alkemy-X Override ###

    def _get_frame_range_data(self, instance):
        """Get frame range data from instance.

        Args:
            instance (pyblish.api.Instance): pyblish instance

        Returns:
            tuple: first_frame, last_frame
        """

        instance_name = instance.data["name"]

        if self._frame_ranges.get(instance_name):
            # return cashed write node
            return self._frame_ranges[instance_name]

        write_node = self._write_node_helper(instance)

        # Get frame range from workfile
        first_frame = int(nuke.root()["first_frame"].getValue())
        last_frame = int(nuke.root()["last_frame"].getValue())

        # Get frame range from write node if activated
        if write_node["use_limit"].getValue():
            first_frame = int(write_node["first"].getValue())
            last_frame = int(write_node["last"].getValue())

        # add to cache
        self._frame_ranges[instance_name] = (first_frame, last_frame)

        return first_frame, last_frame

    def _set_additional_instance_data(
        self, instance, render_target, colorspace
    ):
        """Set additional instance data.

        Args:
            instance (pyblish.api.Instance): pyblish instance
            render_target (str): render target
            colorspace (str): colorspace
        """
        product_type = instance.data["productType"]

        # add targeted family to families
        instance.data["families"].append(
            "{}.{}".format(product_type, render_target)
        )
        self.log.debug("Appending render target to families: {}.{}".format(
            product_type, render_target)
        )

        write_node = self._write_node_helper(instance)

        # Determine defined file type
        ext = write_node["file_type"].value()

        # get frame range data
        ### Starts Alkemy-X Override ###
        # handle_start = instance.context.data["handleStart"]
        # handle_end = instance.context.data["handleEnd"]
        ### Ends Alkemy-X Override ###
        first_frame, last_frame = self._get_frame_range_data(instance)

        # get output paths
        write_file_path = nuke.filename(write_node)
        output_dir = os.path.dirname(write_file_path)

        # TODO: remove this when we have proper colorspace support
        version_data = {
            "colorspace": colorspace
        }

        instance.data.update({
            "versionData": version_data,
            "path": write_file_path,
            "outputDir": output_dir,
            "ext": ext,
            "colorspace": colorspace
        })

        ### Starts Alkemy-X Override ###
        instance.data.update({
            "handleStart": 0,
            "handleEnd": 0,
            "frameStart": first_frame,
            "frameEnd": last_frame,
            "frameStartHandle": first_frame,
            "frameEndHandle": last_frame,
        })
        ### Ends Alkemy-X Override ###


        # TODO temporarily set stagingDir as persistent for backward
        # compatibility. This is mainly focused on `renders`folders which
        # were previously not cleaned up (and could be used in read notes)
        # this logic should be removed and replaced with custom staging dir
        instance.data["stagingDir_persistent"] = True

    def _write_node_helper(self, instance):
        """Helper function to get write node from instance.

        Also sets instance transient data with child nodes.

        Args:
            instance (pyblish.api.Instance): pyblish instance

        Returns:
            nuke.Node: write node
        """
        instance_name = instance.data["name"]

        if self._write_nodes.get(instance_name):
            # return cashed write node
            return self._write_nodes[instance_name]

        # get all child nodes from group node
        child_nodes = napi.get_instance_group_node_childs(instance)

        # set child nodes to instance transient data
        instance.data["transientData"]["childNodes"] = child_nodes

        if child_nodes:
            write_node = None
            for node_ in child_nodes:
                if node_.Class() == "Write":
                    write_node = node_
        else:
            write_node = instance.data["transientData"]["node"]

        if write_node:
            # for slate frame extraction
            instance.data["transientData"]["writeNode"] = write_node
            # add to cache
            self._write_nodes[instance_name] = write_node

            return self._write_nodes[instance_name]

    def _get_existing_frames_representation(
        self,
        instance,
        collected_frames
    ):
        """Get existing frames representation.

        Args:
            instance (pyblish.api.Instance): pyblish instance
            collected_frames (list): collected frames

        Returns:
            dict: representation
        """

        first_frame, last_frame = self._get_frame_range_data(instance)

        write_node = self._write_node_helper(instance)

        write_file_path = nuke.filename(write_node)
        output_dir = os.path.dirname(write_file_path)

        # Determine defined file type
        ext = write_node["file_type"].value()

        representation = {
            "name": ext,
            "ext": ext,
            "stagingDir": output_dir,
            "tags": ["shotgridreview", "review"]
        }

        # set slate frame
        collected_frames = self._add_slate_frame_to_collected_frames(
            instance,
            collected_frames,
            first_frame,
            last_frame
        )

        if len(collected_frames) == 1:
            ### Starts Alkemy-X Override ###
            representation['files'] = collected_frames[0]
            ### Ends Alkemy-X Override ###
        else:
            representation['files'] = collected_frames

        return representation

    def _get_frame_start_str(self, first_frame, last_frame):
        """Get frame start string.

        Args:
            first_frame (int): first frame
            last_frame (int): last frame

        Returns:
            str: frame start string
        """
        # convert first frame to string with padding
        return (
            "{{:0{}d}}".format(len(str(last_frame)))
        ).format(first_frame)

    def _add_slate_frame_to_collected_frames(
        self,
        instance,
        collected_frames,
        first_frame,
        last_frame
    ):
        """Add slate frame to collected frames.

        Args:
            instance (pyblish.api.Instance): pyblish instance
            collected_frames (list): collected frames
            first_frame (int): first frame
            last_frame (int): last frame

        Returns:
            list: collected frames
        """
        frame_start_str = self._get_frame_start_str(first_frame, last_frame)
        frame_length = int(last_frame - first_frame + 1)

        # this will only run if slate frame is not already
        # rendered from previews publishes
        if (
            "slate" in instance.data["families"]
            and frame_length == len(collected_frames)
        ):
            frame_slate_str = self._get_frame_start_str(
                first_frame - 1,
                last_frame
            )

            slate_frame = collected_frames[0].replace(
                frame_start_str, frame_slate_str)
            collected_frames.insert(0, slate_frame)

        return collected_frames

    def _add_farm_instance_data(self, instance):
        """Add farm publishing related instance data.

        Args:
            instance (pyblish.api.Instance): pyblish instance
        """

        # make sure rendered sequence on farm will
        # be used for extract review
        if not instance.data.get("review"):
            instance.data["useSequenceForReview"] = False

        # Farm rendering
        instance.data.update({
            "transfer": False,
            "farm": True  # to skip integrate
        })
        self.log.info("Farm rendering ON ...")

    def _get_collected_frames(self, instance):
        """Get collected frames.

        Args:
            instance (pyblish.api.Instance): pyblish instance

        Returns:
            list: collected frames
        """

        first_frame, last_frame = self._get_frame_range_data(instance)

        write_node = self._write_node_helper(instance)

        ### Starts Alkemy-X Override ###
        write_file_path = nuke.filename(write_node)

        # Extension may not match write node file type
        extension = os.path.splitext(write_file_path)[-1]
        output_path_pattern = re.sub(fr"(.*\.)(#+)(\{extension})", r"\1*\3",  write_file_path)
        output_files = glob.glob(output_path_pattern)
        if not output_files:
            raise PublishXmlValidationError(
                self,
                "No frames found on disk to publish matching write node output path",
                formatting_data={
                    "output_path": write_file_path,
                    "write_node_name": write_node.fullName(),
                },
                key="no_render_files"
            )

        first_frame = 1
        last_frame = 1

        collections, remainders = clique.assemble(output_files)
        if collections:
            collected_frame_paths = list(collections[0])
            collection_indexes = list(collections[0].indexes)
            first_frame = collection_indexes[0]
            last_frame = collection_indexes[-1]
        elif remainders:
            collected_frame_paths = [remainders[0]]
            if f".{extension}" in napi.constants.VIDEO_FILE_EXTENSIONS:
                duration = self._get_number_of_frames(collected_frame_paths[0])
                first_frame = 1
                last_frame = duration
            else:
                match = path_tools.RE_FRAME_NUMBER.match(
                    os.path.basename(remainders[0])
                )
                if match:
                    try:
                        frame = int(match.group("frame"))
                        first_frame = frame
                        last_frame = frame
                    except ValueError:
                        pass

        # Update frame instance frame range with collected frame range
        self._set_frame_range_data(instance, first_frame, last_frame)

        collected_frames = [os.path.basename(frame) for frame in collected_frame_paths]
        self.log.info("Collected frames: %s", collected_frames)
        ### Ends Alkemy-X Override ###
        return collected_frames

    ### Starts Alkemy-X Override ###
    def _get_number_of_frames(self, file_url):
        """Return duration in frames"""
        try:
            streams = get_ffprobe_streams(file_url, self.log)
        except Exception as exc:
            raise AssertionError(
                (
                    'FFprobe couldn\'t read information about input file: "{}".'
                    " Error message: {}"
                ).format(file_url, str(exc))
            )

        first_video_stream = None
        for stream in streams:
            if "width" in stream and "height" in stream:
                first_video_stream = stream
                break

        if first_video_stream:
            nb_frames = stream.get("nb_frames")
            if nb_frames:
                try:
                    return int(nb_frames)
                except ValueError:
                    self.log.warning(
                        "nb_frames {} not convertible".format(nb_frames)
                    )

                    duration = stream.get("duration")
                    frame_rate = convert_ffprobe_fps_value(
                        stream.get("r_frame_rate", "0/0")
                    )
                    self.log.debug(
                        "duration:: {} frame_rate:: {}".format(
                            duration, frame_rate
                        )
                    )
                    try:
                        return float(duration) * float(frame_rate)
                    except ValueError:
                        self.log.warning(
                            "{} or {} cannot be converted".format(
                                duration, frame_rate
                            )
                        )

        self.log.warning("Cannot get number of frames")
        ### Ends Alkemy-X Override ###
