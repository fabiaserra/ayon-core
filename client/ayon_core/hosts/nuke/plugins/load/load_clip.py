from copy import deepcopy

import nuke
import qargparse
import ayon_api

from ayon_core.lib import Logger
from ayon_core.pipeline import (
    get_representation_path,
)
from ayon_core.pipeline.colorspace import (
    get_imageio_file_rules_colorspace_from_filepath
)
from ayon_core.hosts.nuke.api.lib import (
    get_imageio_input_colorspace,
    maintained_selection,
    get_all_dependent_nodes,
    reset_selection,
)
from ayon_core.hosts.nuke.api import (
    containerise,
    update_container,
    viewer_update_and_undo_stop,
    colorspace_exists_on_node
)
from ayon_core.lib.transcoding import (
    VIDEO_EXTENSIONS,
    IMAGE_EXTENSIONS
)
from ayon_core.hosts.nuke.api import plugin


class LoadClip(plugin.NukeLoader):
    """Load clip into Nuke

    Either it is image sequence or video file.
    """
    log = Logger.get_logger(__name__)

    product_types = {
        "source",
        "plate",
        "reference",
        "render",
        "prerender",
        "review",
    }
    representations = {"*"}
    extensions = set(
        ext.lstrip(".") for ext in IMAGE_EXTENSIONS.union(VIDEO_EXTENSIONS)
    )

    ### Starts Alkemy-X Override ###
    # Change label so it's more user friendly
    label = "Load Sequence"
    ### Ends Alkemy-X Override ###
    order = -20
    icon = "file-video-o"
    color = "white"

    # Loaded from settings
    representations_include = []

    script_start = int(nuke.root()["first_frame"].value())

    # option gui
    options_defaults = {
        "start_at_workfile": True,
        "add_retime": True
    }

    node_name_template = "{class_name}_{ext}"

    @classmethod
    def get_options(cls, *args):
        return [
            qargparse.Boolean(
                "start_at_workfile",
                help="Load at workfile start frame",
                default=cls.options_defaults["start_at_workfile"]
            ),
            qargparse.Boolean(
                "add_retime",
                help="Load with retime",
                default=cls.options_defaults["add_retime"]
            ),
            qargparse.Boolean(
                "load_deep",
                help="Load with DeepRead",
                default=cls.options_defaults.get("load_deep", False)
            ),
        ]

    @classmethod
    def get_representations(cls):
        return cls.representations_include or cls.representations

    def load(self, context, name, namespace, options):
        """Load asset via database."""
        project_name = context["project"]["name"]
        repre_entity = context["representation"]
        version_entity = context["version"]
        version_attributes = version_entity["attrib"]
        version_data = version_entity["data"]

        # reset container id so it is always unique for each instance
        self.reset_container_id()

        is_sequence = len(repre_entity["files"]) > 1

        if is_sequence:
            context["representation"] = (
                self._representation_with_hash_in_frame(repre_entity)
            )

        filepath = self.filepath_from_context(context)
        filepath = filepath.replace("\\", "/")
        self.log.debug("_ filepath: {}".format(filepath))

        start_at_workfile = options.get(
            "start_at_workfile", self.options_defaults["start_at_workfile"])

        add_retime = options.get(
            "add_retime", self.options_defaults["add_retime"])

        repre_id = repre_entity["id"]

        self.log.debug(
            "Representation id `{}` ".format(repre_id))

        self.handle_start = version_attributes.get("handleStart", 0)
        self.handle_end = version_attributes.get("handleEnd", 0)

        first = version_attributes.get("frameStart")
        last = version_attributes.get("frameEnd")
        first -= self.handle_start
        last += self.handle_end
        ### Starts Alkemy-x override ###
        # Make sure first and last are integers
        first = int(first)
        last = int(last)

        if not is_sequence:
            duration = last - first
            first = 1
            last = first + duration
        ### Ends Alkemy-x override ###

        # Fallback to folder name when namespace is None
        if namespace is None:
            namespace = context["folder"]["name"]

        if not filepath:
            self.log.warning(
                "Representation id `{}` is failing to load".format(repre_id))
            return

        read_name = self._get_node_name(context)

        # Create the Loader with the filename path set
        read_node = nuke.createNode(
            "DeepRead" if options.get("load_deep", False) else "Read",
            "name {}".format(read_name),
            inpanel=False
        )

        # get colorspace
        colorspace = (
            repre_entity["data"].get("colorspace")
            or version_attributes.get("colorSpace")
        )

        # to avoid multiple undo steps for rest of process
        # we will switch off undo-ing
        with viewer_update_and_undo_stop():
            ### Starts Alkemy-x override ###
            if is_sequence:
                formatted_filepath = f"{filepath} {first}-{last}"
            else:
                formatted_filepath = filepath

            # fromUserText let's Nuke automatically fill frame details
            read_node["file"].fromUserText(formatted_filepath)

            # Override root setting of format. Read format shouldn't be dynamic
            for format in nuke.formats():
                if read_node.height() == format.height() and \
                        read_node.width() == format.width() and \
                        read_node.pixelAspect() == format.pixelAspect():
                    self.log.info(
                        "Setting format '%s' (%sx%sx%s) in read node %s.",
                            format.name() or "",
                            format.width(),
                            format.height(),
                            format.pixelAspect(),
                            read_node.name()
                    )
                    try:
                        read_node["format"].setValue(format)
                        break
                    except TypeError:
                        self.log.error("Couldn't set format")

            self.set_colorspace_to_node(
                read_node,
                filepath,
                project_name,
                version_entity,
                repre_entity
            )
            
            product_entity = context["product"]
            if product_entity["productType"] == "reference":
                load_first_frame = version_data.get("frameStart", None)
                load_handle_start = version_data.get("handleStart", None)
                if load_first_frame and load_handle_start:
                    start_frame = load_first_frame - load_handle_start
                else:
                    start_frame = self.script_start
                self._loader_shift(read_node, int(start_frame), start_at_workfile)
            ### Ends Alkemy-x override ###
            
            version_name = version_entity["version"]
            if version_name < 0:
                version_name = "hero"

            data_imprint = {
                "version": version_name,
                "db_colorspace": colorspace
            }

            # add attributes from the version to imprint metadata knob
            for key in [
                "frameStart",
                "frameEnd",
                "source",
                "author",
                "fps",
                "handleStart",
                "handleEnd",
            ]:
                value = version_attributes.get(key, str(None))
                if isinstance(value, str):
                    value = value.replace("\\", "/")
                data_imprint[key] = value

            if add_retime and version_data.get("retime"):
                data_imprint["addRetime"] = True

            read_node["tile_color"].setValue(int("0x4ecd25ff", 16))

            container = containerise(
                read_node,
                name=name,
                namespace=namespace,
                context=context,
                loader=self.__class__.__name__,
                data=data_imprint)

        if add_retime and version_data.get("retime"):
            self._make_retimes(read_node, version_data)

        self.set_as_member(read_node)

        return container

    def switch(self, container, context):
        self.update(container, context)

    def _representation_with_hash_in_frame(self, repre_entity):
        """Convert frame key value to padded hash

        Args:
            repre_entity (dict): Representation entity.

        Returns:
            dict: altered representation data

        """
        new_repre_entity = deepcopy(repre_entity)
        context = new_repre_entity["context"]

        # Get the frame from the context and hash it
        frame = context["frame"]
        hashed_frame = "#" * len(str(frame))

        # Replace the frame with the hash in the originalBasename
        if (
            "{originalBasename}" in new_repre_entity["attrib"]["template"]
        ):
            origin_basename = context["originalBasename"]
            context["originalBasename"] = origin_basename.replace(
                frame, hashed_frame
            )

        # Replace the frame with the hash in the frame
        new_repre_entity["context"]["frame"] = hashed_frame
        return new_repre_entity

    def update(self, container, context):
        """Update the Loader's path

        Nuke automatically tries to reset some variables when changing
        the loader's path to a new file. These automatic changes are to its
        inputs:

        """

        project_name = context["project"]["name"]
        version_entity = context["version"]
        repre_entity = context["representation"]

        version_attributes = version_entity["attrib"]
        version_data = version_entity["data"]

        is_sequence = len(repre_entity["files"]) > 1

        read_node = container["node"]

        if is_sequence:
            repre_entity = self._representation_with_hash_in_frame(
                repre_entity
            )

        filepath = (
            get_representation_path(repre_entity)
        ).replace("\\", "/")
        self.log.debug("_ filepath: {}".format(filepath))

        start_at_workfile = "start at" in read_node['frame_mode'].value()

        add_retime = [
            key for key in read_node.knobs().keys()
            if "addRetime" in key
        ]

        repre_id = repre_entity["id"]

        # colorspace profile
        colorspace = (
            repre_entity["data"].get("colorspace")
            or version_attributes.get("colorSpace")
        )

        self.handle_start = version_attributes.get("handleStart", 0)
        self.handle_end = version_attributes.get("handleEnd", 0)

        first = version_attributes.get("frameStart")
        last = version_attributes.get("frameEnd")
        ### Starts Alkemy-x override ###
        # Make sure first and last are integers
        first = int(first)
        last = int(last)
        ### Ends Alkemy-x override ###

        first -= self.handle_start
        last += self.handle_end

        if not is_sequence:
            duration = last - first
            first = 1
            last = first + duration

        if not filepath:
            self.log.warning(
                "Representation id `{}` is failing to load".format(repre_id))
            return

        read_node["file"].setValue(filepath)

        # to avoid multiple undo steps for rest of process
        # we will switch off undo-ing
        with viewer_update_and_undo_stop():
            self.set_colorspace_to_node(
                read_node,
                filepath,
                project_name,
                version_entity,
                repre_entity
            )

            ### Starts Alkemy-x override ###
            product_entity = context["product"]
            if product_entity["productType"] == "reference":
                load_first_frame = version_data.get("frameStart", None)
                load_handle_start = version_data.get("handleStart", None)
                if load_first_frame and load_handle_start:
                    start_frame = int(load_first_frame - load_handle_start)
                else:
                    start_frame = self.script_start

                self._loader_shift(read_node, start_frame, start_at_workfile)
            ### Ends Alkemy-x override ###

            updated_dict = {
                "representation": repre_entity["id"],
                "frameStart": str(first),
                "frameEnd": str(last),
                "version": str(version_entity["version"]),
                "db_colorspace": colorspace,
                "source": version_attributes.get("source"),
                "handleStart": str(self.handle_start),
                "handleEnd": str(self.handle_end),
                "fps": str(version_attributes.get("fps")),
                "author": version_attributes.get("author")
            }

            last_version_entity = ayon_api.get_last_version_by_product_id(
                project_name, version_entity["productId"], fields={"id"}
            )
            # change color of read_node
            if version_entity["id"] == last_version_entity["id"]:
                color_value = "0x4ecd25ff"
            else:
                color_value = "0xd84f20ff"
            read_node["tile_color"].setValue(int(color_value, 16))

            # Update the imprinted representation
            update_container(read_node, updated_dict)
            self.log.info(
                "updated to version: {}".format(version_entity["version"])
            )

        if add_retime and version_data.get("retime"):
            self._make_retimes(read_node, version_data)
        else:
            self.clear_members(read_node)

        self.set_as_member(read_node)

    def set_colorspace_to_node(
        self,
        read_node,
        filepath,
        project_name,
        version_entity,
        repre_entity,
    ):
        """Set colorspace to read node.

        Sets colorspace with available names validation.

        Args:
            read_node (nuke.Node): The nuke's read node
            filepath (str): File path.
            project_name (str): Project name.
            version_entity (dict): Version entity.
            repre_entity (dict): Representation entity.

        """
        used_colorspace = self._get_colorspace_data(
            project_name, version_entity, repre_entity, filepath
        )
        if (
            used_colorspace
            and colorspace_exists_on_node(read_node, used_colorspace)
        ):
            self.log.info(f"Used colorspace: {used_colorspace}")
            read_node["colorspace"].setValue(used_colorspace)
        else:
            self.log.info("Colorspace not set...")

    def remove(self, container):
        read_node = container["node"]
        assert read_node.Class() == "Read", "Must be Read"

        with viewer_update_and_undo_stop():
            members = self.get_members(read_node)
            nuke.delete(read_node)
            for member in members:
                nuke.delete(member)

    def _set_range_to_node(self, read_node, first, last, start_at_workfile):
        read_node['origfirst'].setValue(int(first))
        read_node['first'].setValue(int(first))
        read_node['origlast'].setValue(int(last))
        read_node['last'].setValue(int(last))

        ### Starts Alkemy-x override ###
        # # set start frame depending on workfile or version
        # self._loader_shift(read_node, start_at_workfile)
        ### Ends Alkemy-x override ###

    def _make_retimes(self, parent_node, version_data):
        ''' Create all retime and timewarping nodes with copied animation '''
        speed = version_data.get('speed', 1)
        time_warp_nodes = version_data.get('timewarps', [])
        last_node = None
        source_id = self.get_container_id(parent_node)
        self.log.debug("__ source_id: {}".format(source_id))
        self.log.debug("__ members: {}".format(
            self.get_members(parent_node)))

        dependent_nodes = self.clear_members(parent_node)

        with maintained_selection():
            parent_node['selected'].setValue(True)

            if speed != 1:
                rtn = nuke.createNode(
                    "Retime",
                    "speed {}".format(speed))

                rtn["before"].setValue("continue")
                rtn["after"].setValue("continue")
                rtn["input.first_lock"].setValue(True)
                rtn["input.first"].setValue(
                    self.script_start
                )
                self.set_as_member(rtn)
                last_node = rtn

            if time_warp_nodes != []:
                start_anim = self.script_start + (self.handle_start / speed)
                for timewarp in time_warp_nodes:
                    twn = nuke.createNode(
                        timewarp["Class"],
                        "name {}".format(timewarp["name"])
                    )
                    if isinstance(timewarp["lookup"], list):
                        # if array for animation
                        twn["lookup"].setAnimated()
                        for i, value in enumerate(timewarp["lookup"]):
                            twn["lookup"].setValueAt(
                                (start_anim + i) + value,
                                (start_anim + i))
                    else:
                        # if static value `int`
                        twn["lookup"].setValue(timewarp["lookup"])

                    self.set_as_member(twn)
                    last_node = twn

            if dependent_nodes:
                # connect to original inputs
                for i, n in enumerate(dependent_nodes):
                    last_node.setInput(i, n)

    ### Starts Alkemy-x override ###
    def _loader_shift(self, read_node, start_frame, workfile_start=False):
        """ Set start frame of read node to a workfile start

        Args:
            read_node (nuke.Node): The nuke's read node
            workfile_start (bool): set workfile start frame if true

        """
        with maintained_selection():
            # Remove selection
            reset_selection()

            read_node["selected"].setValue(True)

            time_offset = None
            reformat = None
            read_name = read_node.name()
            # Creates time_offset instead of read in-node operations
            if workfile_start:
                dependent_nodes = get_all_dependent_nodes(read_node)
                for dependent_node in dependent_nodes:
                    if dependent_node.Class() == "TimeOffset" and \
                        dependent_node.name().startswith(read_name):
                        time_offset = dependent_node
                    elif dependent_node.Class() == "Reformat" and \
                        dependent_node.name().startswith(read_name):
                        reformat = dependent_node

                    elif time_offset and reformat:
                        break

                # If time_offset and reformat found then update
                # Account for video type starting at 1 instead of 0
                start_frame -= 1

                if time_offset:
                    if time_offset["time_offset"].value() != start_frame:
                        time_offset.setValue(start_frame)

                else:
                    nuke.createNode(
                        "TimeOffset",
                        f"name {read_name}_Ref_Offset time_offset {start_frame}",
                        inpanel=False,
                    )

                # Only create reformat
                if not reformat:
                    # Plugin in plugins until loaded for the first time
                    tmp_node = None
                    try:
                        tmp_node = nuke.createNode(
                            "reference_reformat",
                            f"name {read_name}_Ref_Reformat",
                            inpanel=False,
                        )
                    except RuntimeError:
                        nuke.createNode(
                            "Reformat",
                            f"name {read_name}_Ref_Reformat",
                            inpanel=False,
                        )
                        # Incase the reference_reformat was made before error
                        if tmp_node:
                            nuke.delete(tmp_node)
    ### Ends Alkemy-x override ###
    def _get_node_name(self, context):
        folder_entity = context["folder"]
        product_name = context["product"]["name"]
        repre_entity = context["representation"]

        folder_name = folder_entity["name"]
        repre_cont = repre_entity["context"]
        name_data = {
            "folder": {
                "name": folder_name,
            },
            "product": {
                "name": product_name,
            },
            "asset": folder_name,
            "subset": product_name,
            "representation": repre_entity["name"],
            "ext": repre_cont["representation"],
            "id": repre_entity["id"],
            "class_name": self.__class__.__name__
        }

        return self.node_name_template.format(**name_data)

    def _get_colorspace_data(
        self, project_name, version_entity, repre_entity, filepath
    ):
        """Get colorspace data from version and representation documents

        Args:
            project_name (str): Project name.
            version_entity (dict): Version entity.
            repre_entity (dict): Representation entity.
            filepath (str): File path.

        Returns:
            Any[str,None]: colorspace name or None
        """
        # Get backward compatible colorspace key.
        colorspace = repre_entity["data"].get("colorspace")
        self.log.debug(
            f"Colorspace from representation colorspace: {colorspace}"
        )

        # Get backward compatible version data key if colorspace is not found.
        if not colorspace:
            colorspace = version_entity["attrib"].get("colorSpace")
            self.log.debug(
                f"Colorspace from version colorspace: {colorspace}"
            )

        # Get colorspace from representation colorspaceData if colorspace is
        # not found.
        if not colorspace:
            colorspace_data = repre_entity["data"].get("colorspaceData", {})
            colorspace = colorspace_data.get("colorspace")
            self.log.debug(
                f"Colorspace from representation colorspaceData: {colorspace}"
            )

        # check if any filerules are not applicable
        new_parsed_colorspace = get_imageio_file_rules_colorspace_from_filepath( # noqa
            filepath, "nuke", project_name
        )
        self.log.debug(f"Colorspace new filerules: {new_parsed_colorspace}")

        # colorspace from `project_settings/nuke/imageio/regexInputs`
        old_parsed_colorspace = get_imageio_input_colorspace(filepath)
        self.log.debug(f"Colorspace old filerules: {old_parsed_colorspace}")

        return (
            new_parsed_colorspace
            or old_parsed_colorspace
            or colorspace
        )
