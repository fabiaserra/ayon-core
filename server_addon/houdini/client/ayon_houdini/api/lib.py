# -*- coding: utf-8 -*-
import sys
import os
import errno
import re
import logging
import json
from contextlib import contextmanager

import six
import ayon_api

from ayon_core.lib import StringTemplate, env_value_to_bool
from ayon_core.tools.utils import host_tools
from ayon_core.settings import get_current_project_settings
from ayon_core.pipeline import (
    Anatomy,
    get_current_project_name,
    get_current_folder_path,
    registered_host,
    get_current_context,
    get_current_host_name,
)
from ayon_core.pipeline.create import CreateContext
from ayon_core.pipeline.template_data import get_template_data
from ayon_core.pipeline.context_tools import get_current_folder_entity
from ayon_core.tools.utils import PopupUpdateKeys, SimplePopup
from ayon_core.tools.utils.host_tools import get_tool_by_name

import hou


self = sys.modules[__name__]
self._parent = None
log = logging.getLogger(__name__)
JSON_PREFIX = "JSON:::"


def get_folder_fps(folder_entity=None):
    """Return current folder fps."""

    if folder_entity is None:
        folder_entity = get_current_folder_entity(fields=["attrib.fps"])
    return folder_entity["attrib"]["fps"]


def get_export_parameter(node):
    """Return the export output parameter of the given node

    Example:
        root = hou.node("/obj")
        my_alembic_node = root.createNode("alembic")
        get_export_parameter(my_alembic_node)
        # Result: "output"

    Args:
        node(hou.Node): node instance

    Returns:
        hou.Parm

    """
    node_type = node.type().description()

    # Ensures the proper Take is selected for each ROP to retrieve the correct
    # ifd
    try:
        rop_take = hou.takes.findTake(node.parm("take").eval())
        if rop_take is not None:
            hou.takes.setCurrentTake(rop_take)
    except AttributeError:
        # hou object doesn't always have the 'takes' attribute
        pass

    if node_type == "Mantra" and node.parm("soho_outputmode").eval():
        return node.parm("soho_diskfile")
    elif node_type == "USD" or node_type == "USD Render ROP" or node_type == "USD Render":
        return node.parm("lopoutput")
    elif node_type == "Alfred":
        return node.parm("alf_diskfile")
    elif (node_type == "RenderMan" or node_type == "RenderMan RIS"):
        pre_ris22 = node.parm("rib_outputmode") and \
            node.parm("rib_outputmode").eval()
        ris22 = node.parm("diskfile") and node.parm("diskfile").eval()
        if pre_ris22 or ris22:
            return node.parm("soho_diskfile")
    elif node_type == "Redshift" and node.parm("RS_archive_enable").eval():
        return node.parm("RS_archive_file")
    elif node_type == "Wedge" and node.parm("driver").eval():
        return get_export_parameter(node.node(node.parm("driver").eval()))
    elif node_type == "Arnold":
        return node.parm("ar_ass_file")
    elif node_type == "Alembic" and node.parm("use_sop_path").eval():
        return node.parm("sop_path")
    elif node_type == "Shotgun Mantra" and node.parm("soho_outputmode").eval():
        return node.parm("sgtk_soho_diskfile")
    elif node_type == "Shotgun Alembic" and node.parm("use_sop_path").eval():
        return node.parm("sop_path")
    elif node.type().nameWithCategory() == "Driver/vray_renderer":
        return node.parm("render_export_filepath")

    raise TypeError("Node type '%s' not supported" % node_type)


def get_output_parameter(node):
    """Return the render output parameter of the given node

    Example:
        root = hou.node("/obj")
        my_alembic_node = root.createNode("alembic")
        get_output_parameter(my_alembic_node)
        # Result: "output"

    Args:
        node(hou.Node): node instance

    Returns:
        hou.Parm

    """
    node_type = node.type().description()
    category = node.type().category().name()

    # Figure out which type of node is being rendered
    if node_type in {"Geometry", "Filmbox FBX", "File Cache", "Labs File Cache"} or \
            (node_type == "ROP Output Driver" and category == "Sop"):
        return node.parm("sopoutput")
    elif node_type == "USD" or node_type == "HuskStandalone":
        return node.parm("lopoutput")
    elif node_type == "USD Render ROP" or node_type == "USD Render":
        return node.parm("outputimage")
    elif node_type == "Composite":
        return node.parm("copoutput")
    elif node_type == "Channel":
        return node.parm("chopoutput")
    elif node_type == "Dynamics" or \
            (node_type == "ROP Output Driver" and category == "Dop"):
        return node.parm("dopoutput")
    elif node_type == "Alfred":
        return node.parm("alf_diskfile")
    elif node_type == "RenderMan" or node_type == "RenderMan RIS":
        return node.parm("ri_display")
    elif node_type == "Redshift":
        return node.parm("RS_returnmePrefix")
    elif node_type == "Mantra":
        return node.parm("vm_picture")
    elif node_type == "Wedge":
        driver_node = node.node(node.parm("driver").eval())
        if driver_node:
            return get_output_parameter(driver_node)
    elif node_type == "Arnold":
        return node.parm("ar_picture")
    elif node_type == "Arnold Denoiser":
        return node.parm("output")
    elif node_type == "HQueue Simulation":
        inner_node = node.node(node.parm("hq_driver").eval())
        if inner_node:
            return get_output_parameter(inner_node)
    elif node_type == "ROP Alembic Output":
        return node.parm("filename")
    elif node_type == "Redshift":
        return node.parm("RS_returnmePrefix")
    elif node_type == "Alembic":
        return node.parm("filename")
    elif node_type == "Shotgun Mantra":
        return node.parm("sgtk_vm_picture")
    elif node_type == "Shotgun Alembic":
        return node.parm("filename")
    elif node_type == "Bake Texture":
        return node.parm("vm_uvoutputpicture1")
    elif node_type == "OpenGL":
        return node.parm("picture")
    elif node_type == "Octane":
        return node.parm("HO_img_fileName")
    elif node_type == "Fetch":
        inner_node = node.node(node.parm("source").eval())
        if inner_node:
            try:
                return get_output_parameter(inner_node)
            except TypeError:
                raise TypeError("Fetch source '%s' not supported" % inner_node)

    elif node.type().nameWithCategory() == "Driver/vray_renderer":
        return node.parm("SettingsOutput_img_file_path")
    elif node_type == "File Cache":
        return node.parm("sopoutput")

    raise TypeError("Node type '%s' not supported" % node_type)


def set_scene_fps(fps):
    hou.setFps(fps)


# Valid FPS
def validate_fps():
    """Validate current scene FPS and show pop-up when it is incorrect

    Returns:
        bool

    """

    fps = get_folder_fps()
    current_fps = hou.fps()  # returns float

    if current_fps != fps:

        # Find main window
        parent = hou.ui.mainQtWindow()
        if parent is None:
            pass
        else:
            dialog = PopupUpdateKeys(parent=parent)
            dialog.setModal(True)
            dialog.setWindowTitle("Houdini scene does not match project FPS")
            dialog.set_message("Scene %i FPS does not match project %i FPS" %
                              (current_fps, fps))
            dialog.set_button_text("Fix")

            # on_show is the Fix button clicked callback
            dialog.on_clicked_state.connect(lambda: set_scene_fps(fps))

            dialog.show()

            return False

    return True


def create_remote_publish_node(force=True):
    """Function to create a remote publish node in /out

    This is a hacked "Shell" node that does *nothing* except for triggering
    `colorbleed.lib.publish_remote()` as pre-render script.

    All default attributes of the Shell node are hidden to the Artist to
    avoid confusion.

    Additionally some custom attributes are added that can be collected
    by a Collector to set specific settings for the publish, e.g. whether
    to separate the jobs per instance or process in one single job.

    """

    cmd = "import colorbleed.lib; colorbleed.lib.publish_remote()"

    existing = hou.node("/out/REMOTE_PUBLISH")
    if existing:
        if force:
            log.warning("Removing existing '/out/REMOTE_PUBLISH' node..")
            existing.destroy()
        else:
            raise RuntimeError("Node already exists /out/REMOTE_PUBLISH. "
                               "Please remove manually or set `force` to "
                               "True.")

    # Create the shell node
    out = hou.node("/out")
    node = out.createNode("shell", node_name="REMOTE_PUBLISH")
    node.moveToGoodPosition()

    # Set color make it stand out (avalon/pyblish color)
    node.setColor(hou.Color(0.439, 0.709, 0.933))

    # Set the pre-render script
    node.setParms({
        "prerender": cmd,
        "lprerender": "python"  # command language
    })

    # Lock the attributes to ensure artists won't easily mess things up.
    node.parm("prerender").lock(True)
    node.parm("lprerender").lock(True)

    # Lock up the actual shell command
    command_parm = node.parm("command")
    command_parm.set("")
    command_parm.lock(True)
    shellexec_parm = node.parm("shellexec")
    shellexec_parm.set(False)
    shellexec_parm.lock(True)

    # Get the node's parm template group so we can customize it
    template = node.parmTemplateGroup()

    # Hide default tabs
    template.hideFolder("Shell", True)
    template.hideFolder("Scripts", True)

    # Hide default settings
    template.hide("execute", True)
    template.hide("renderdialog", True)
    template.hide("trange", True)
    template.hide("f", True)
    template.hide("take", True)

    # Add custom settings to this node.
    parm_folder = hou.FolderParmTemplate("folder", "Submission Settings")

    # Separate Jobs per Instance
    parm = hou.ToggleParmTemplate(name="separateJobPerInstance",
                                  label="Separate Job per Instance",
                                  default_value=False)
    parm_folder.addParmTemplate(parm)

    # Add our custom Submission Settings folder
    template.append(parm_folder)

    # Apply template back to the node
    node.setParmTemplateGroup(template)


def render_rop(ropnode):
    """Render ROP node utility for Publishing.

    This renders a ROP node with the settings we want during Publishing.
    """
    # Print verbose when in batch mode without UI
    verbose = not hou.isUIAvailable()

    # Render
    try:
        ropnode.render(verbose=verbose,
                       # Allow Deadline to capture completion percentage
                       output_progress=verbose,
                       # Render only this node
                       # (do not render any of its dependencies)
                       ignore_inputs=True)
    except hou.Error as exc:
        # The hou.Error is not inherited from a Python Exception class,
        # so we explicitly capture the houdini error, otherwise pyblish
        # will remain hanging.
        import traceback
        traceback.print_exc()
        raise RuntimeError("Render failed: {0}".format(exc))


def imprint(node, data, update=False):
    """Store attributes with value on a node

    Depending on the type of attribute it creates the correct parameter
    template. Houdini uses a template per type, see the docs for more
    information.

    http://www.sidefx.com/docs/houdini/hom/hou/ParmTemplate.html

    Because of some update glitch where you cannot overwrite existing
    ParmTemplates on node using:
        `setParmTemplates()` and `parmTuplesInFolder()`
    update is done in another pass.

    Args:
        node(hou.Node): node object from Houdini
        data(dict): collection of attributes and their value
        update (bool, optional): flag if imprint should update
            already existing data or leave them untouched and only
            add new.

    Returns:
        None

    """
    if not data:
        return
    if not node:
        self.log.error("Node is not set, calling imprint on invalid data.")
        return

    current_parms = {p.name(): p for p in node.spareParms()}
    update_parm_templates = []
    new_parm_templates = []

    for key, value in data.items():
        if value is None:
            continue

        parm_template = get_template_from_value(key, value)

        if key in current_parms:
            if node.evalParm(key) == value:
                continue
            if not update:
                log.debug(f"{key} already exists on {node}")
            else:
                log.debug(f"replacing {key}")
                update_parm_templates.append(parm_template)
            continue

        new_parm_templates.append(parm_template)

    if not new_parm_templates and not update_parm_templates:
        return

    parm_group = node.parmTemplateGroup()

    # Add new parm templates
    if new_parm_templates:
        parm_folder = parm_group.findFolder("Extra")

        # if folder doesn't exist yet, create one and append to it,
        # else append to existing one
        if not parm_folder:
            parm_folder = hou.FolderParmTemplate("folder", "Extra")
            parm_folder.setParmTemplates(new_parm_templates)
            parm_group.append(parm_folder)
        else:
            # Add to parm template folder instance then replace with updated
            # one in parm template group
            for template in new_parm_templates:
                parm_folder.addParmTemplate(template)
            parm_group.replace(parm_folder.name(), parm_folder)

    # Update existing parm templates
    for parm_template in update_parm_templates:
        parm_group.replace(parm_template.name(), parm_template)

        # When replacing a parm with a parm of the same name it preserves its
        # value if before the replacement the parm was not at the default,
        # because it has a value override set. Since we're trying to update the
        # parm by using the new value as `default` we enforce the parm is at
        # default state
        node.parm(parm_template.name()).revertToDefaults()

    node.setParmTemplateGroup(parm_group)


def lsattr(attr, value=None, root="/"):
    """Return nodes that have `attr`
     When `value` is not None it will only return nodes matching that value
     for the given attribute.
     Args:
         attr (str): Name of the attribute (hou.Parm)
         value (object, Optional): The value to compare the attribute too.
            When the default None is provided the value check is skipped.
        root (str): The root path in Houdini to search in.
    Returns:
        list: Matching nodes that have attribute with value.
    """
    if value is None:
        # Use allSubChildren() as allNodes() errors on nodes without
        # permission to enter without a means to continue of querying
        # the rest
        nodes = hou.node(root).allSubChildren()
        return [n for n in nodes if n.parm(attr)]
    return lsattrs({attr: value})


def lsattrs(attrs, root="/"):
    """Return nodes matching `key` and `value`
    Arguments:
        attrs (dict): collection of attribute: value
        root (str): The root path in Houdini to search in.
    Example:
        >> lsattrs({"id": "myId"})
        ["myNode"]
        >> lsattr("id")
        ["myNode", "myOtherNode"]
    Returns:
        list: Matching nodes that have attribute with value.
    """

    matches = set()
    # Use allSubChildren() as allNodes() errors on nodes without
    # permission to enter without a means to continue of querying
    # the rest
    nodes = hou.node(root).allSubChildren()
    for node in nodes:
        for attr in attrs:
            if not node.parm(attr):
                continue
            elif node.evalParm(attr) != attrs[attr]:
                continue
            else:
                matches.add(node)

    return list(matches)


def read(node):
    """Read the container data in to a dict

    Args:
        node(hou.Node): Houdini node

    Returns:
        dict

    """
    # `spareParms` returns a tuple of hou.Parm objects
    data = {}
    if not node:
        return data
    for parameter in node.spareParms():
        value = parameter.eval()
        # test if value is json encoded dict
        if isinstance(value, six.string_types) and \
                value.startswith(JSON_PREFIX):
            try:
                value = json.loads(value[len(JSON_PREFIX):])
            except json.JSONDecodeError:
                # not a json
                pass
        data[parameter.name()] = value

    return data


@contextmanager
def maintained_selection():
    """Maintain selection during context
    Example:
        >>> with maintained_selection():
        ...     # Modify selection
        ...     node.setSelected(on=False, clear_all_selected=True)
        >>> # Selection restored
    """

    previous_selection = hou.selectedNodes()
    try:
        yield
    finally:
        # Clear the selection
        # todo: does hou.clearAllSelected() do the same?
        for node in hou.selectedNodes():
            node.setSelected(on=False)

        if previous_selection:
            for node in previous_selection:
                node.setSelected(on=True)


def reset_framerange(fps=True, frame_range=True):
    """Set frame range and FPS to current folder."""

    project_name = get_current_project_name()
    folder_path = get_current_folder_path()

    folder_entity = ayon_api.get_folder_by_path(project_name, folder_path)
    folder_attributes = folder_entity["attrib"]

    # Set FPS
    if fps:
        fps = get_folder_fps(folder_entity)
        print("Setting scene FPS to {}".format(int(fps)))
        set_scene_fps(fps)

    if frame_range:

        # Set Start and End Frames
        frame_start = folder_attributes.get("frameStart")
        frame_end = folder_attributes.get("frameEnd")

        if frame_start is None or frame_end is None:
            log.warning("No edit information found for '%s'", folder_path)
            return

        handle_start = folder_attributes.get("handleStart", 0)
        handle_end = folder_attributes.get("handleEnd", 0)

        frame_start -= int(handle_start)
        frame_end += int(handle_end)

        # Set frame range and FPS
        hou.playbar.setFrameRange(frame_start, frame_end)
        hou.playbar.setPlaybackRange(frame_start, frame_end)
        hou.setFrame(frame_start)


def get_main_window():
    """Acquire Houdini's main window"""
    if self._parent is None:
        self._parent = hou.ui.mainQtWindow()
    return self._parent


def get_template_from_value(key, value):
    if isinstance(value, float):
        parm = hou.FloatParmTemplate(name=key,
                                     label=key,
                                     num_components=1,
                                     default_value=(value,))
    elif isinstance(value, bool):
        parm = hou.ToggleParmTemplate(name=key,
                                      label=key,
                                      default_value=value)
    elif isinstance(value, int):
        parm = hou.IntParmTemplate(name=key,
                                   label=key,
                                   num_components=1,
                                   default_value=(value,))
    elif isinstance(value, six.string_types):
        parm = hou.StringParmTemplate(name=key,
                                      label=key,
                                      num_components=1,
                                      default_value=(value,))
    elif isinstance(value, (dict, list, tuple)):
        parm = hou.StringParmTemplate(name=key,
                                      label=key,
                                      num_components=1,
                                      default_value=(
                                          JSON_PREFIX + json.dumps(value),))
    else:
        raise TypeError("Unsupported type: %r" % type(value))

    return parm


def get_frame_data(node, log=None):
    """Get the frame data: `frameStartHandle`, `frameEndHandle`
    and `byFrameStep`.

    This function uses Houdini node's `trange`, `t1, `t2` and `t3`
    parameters as the source of truth for the full inclusive frame
    range to render, as such these are considered as the frame
    range including the handles.

    The non-inclusive frame start and frame end without handles
    can be computed by subtracting the handles from the inclusive
    frame range.

    Args:
        node (hou.Node): ROP node to retrieve frame range from,
            the frame range is assumed to be the frame range
            *including* the start and end handles.

    Returns:
        dict: frame data for `frameStartHandle`, `frameEndHandle`
            and `byFrameStep`.

    """

    if log is None:
        log = self.log

    data = {}

    if node.parm("trange") is None:
        log.debug(
            "Node has no 'trange' parameter: {}".format(node.path())
        )
        return data

    if node.evalParm("trange") == 0:
        data["frameStartHandle"] = hou.intFrame()
        data["frameEndHandle"] = hou.intFrame()
        data["byFrameStep"] = 1.0

        log.info(
            "Node '{}' has 'Render current frame' set.\n"
            "Folder Handles are ignored.\n"
            "frameStart and frameEnd are set to the "
            "current frame.".format(node.path())
        )
    else:
        data["frameStartHandle"] = int(node.evalParm("f1"))
        data["frameEndHandle"] = int(node.evalParm("f2"))
        data["byFrameStep"] = node.evalParm("f3")

    return data


def splitext(name, allowed_multidot_extensions):
    # type: (str, list) -> tuple
    """Split file name to name and extension.

    Args:
        name (str): File name to split.
        allowed_multidot_extensions (list of str): List of allowed multidot
            extensions.

    Returns:
        tuple: Name and extension.
    """

    for ext in allowed_multidot_extensions:
        if name.endswith(ext):
            return name[:-len(ext)], ext

    return os.path.splitext(name)


def get_top_referenced_parm(parm):

    processed = set()  # disallow infinite loop
    while True:
        if parm.path() in processed:
            raise RuntimeError("Parameter references result in cycle.")

        processed.add(parm.path())

        ref = parm.getReferencedParm()
        if ref.path() == parm.path():
            # It returns itself when it doesn't reference
            # another parameter
            return ref
        else:
            parm = ref


def evalParmNoFrame(node, parm, pad_character="#"):

    parameter = node.parm(parm)
    assert parameter, "Parameter does not exist: %s.%s" % (node, parm)

    # If the parameter has a parameter reference, then get that
    # parameter instead as otherwise `unexpandedString()` fails.
    parameter = get_top_referenced_parm(parameter)

    # Substitute out the frame numbering with padded characters
    try:
        raw = parameter.unexpandedString()
    except hou.Error as exc:
        print("Failed: %s" % parameter)
        raise RuntimeError(exc)

    def replace(match):
        padding = 1
        n = match.group(2)
        if n and int(n):
            padding = int(n)
        return pad_character * padding

    expression = re.sub(r"(\$F([0-9]*))", replace, raw)

    with hou.ScriptEvalContext(parameter):
        return hou.expandStringAtFrame(expression, 0)


def get_color_management_preferences():
    """Get default OCIO preferences"""
    return {
        "config": hou.Color.ocio_configPath(),
        "display": hou.Color.ocio_defaultDisplay(),
        "view": hou.Color.ocio_defaultView()
    }


def get_obj_node_output(obj_node):
    """Find output node.

    If the node has any output node return the
    output node with the minimum `outputidx`.
    When no output is present return the node
    with the display flag set. If no output node is
    detected then None is returned.

    Arguments:
        node (hou.Node): The node to retrieve a single
            the output node for.

    Returns:
        Optional[hou.Node]: The child output node.

    """

    outputs = obj_node.subnetOutputs()
    if not outputs:
        return

    elif len(outputs) == 1:
        return outputs[0]

    else:
        return min(outputs,
                   key=lambda node: node.evalParm('outputidx'))


def get_output_children(output_node, include_sops=True):
    """Recursively return a list of all output nodes
    contained in this node including this node.

    It works in a similar manner to output_node.allNodes().
    """
    out_list = [output_node]

    if output_node.childTypeCategory() == hou.objNodeTypeCategory():
        for child in output_node.children():
            out_list += get_output_children(child, include_sops=include_sops)

    elif include_sops and \
            output_node.childTypeCategory() == hou.sopNodeTypeCategory():
        out = get_obj_node_output(output_node)
        if out:
            out_list += [out]

    return out_list


def get_resolution_from_folder(folder_entity):
    """Get resolution from the given folder entity.

    Args:
        folder_entity (dict[str, Any]): Folder entity.

    Returns:
        Union[Tuple[int, int], None]: Resolution width and height.

    """
    if not folder_entity or "attrib" not in folder_entity:
        print("Entered folder is not valid. \"{}\"".format(
            str(folder_entity)
        ))
        return None

    folder_attributes = folder_entity["attrib"]
    resolution_width = folder_attributes.get("resolutionWidth")
    resolution_height = folder_attributes.get("resolutionHeight")

    # Make sure both width and height are set
    if resolution_width is None or resolution_height is None:
        print("No resolution information found for '{}'".format(
            folder_entity["path"]
        ))
        return None

    return int(resolution_width), int(resolution_height)


def set_camera_resolution(camera, folder_entity=None):
    """Apply resolution to camera from folder entity of the publish"""

    if not folder_entity:
        folder_entity = get_current_folder_entity()

    resolution = get_resolution_from_folder(folder_entity)

    if resolution:
        print("Setting camera resolution: {} -> {}x{}".format(
            camera.name(), resolution[0], resolution[1]
        ))
        camera.parm("resx").set(resolution[0])
        camera.parm("resy").set(resolution[1])


def get_camera_from_container(container):
    """Get camera from container node. """

    cameras = container.recursiveGlob(
        "*",
        filter=hou.nodeTypeFilter.ObjCamera,
        include_subnets=False
    )

    assert len(cameras) == 1, "Camera instance must have only one camera"
    return cameras[0]


def get_current_context_template_data_with_folder_attrs():
    """

    Output contains 'folderAttributes' key with folder attribute values.

    Returns:
         dict[str, Any]: Template data to fill templates.

    """
    context = get_current_context()
    project_name = context["project_name"]
    folder_path = context["folder_path"]
    task_name = context["task_name"]
    host_name = get_current_host_name()

    project_entity = ayon_api.get_project(project_name)
    anatomy = Anatomy(project_name, project_entity=project_entity)
    folder_entity = ayon_api.get_folder_by_path(project_name, folder_path)
    task_entity = ayon_api.get_task_by_name(
        project_name, folder_entity["id"], task_name
    )

    # get context specific vars
    folder_attributes = folder_entity["attrib"]

    # compute `frameStartHandle` and `frameEndHandle`
    frame_start = folder_attributes.get("frameStart")
    frame_end = folder_attributes.get("frameEnd")
    handle_start = folder_attributes.get("handleStart")
    handle_end = folder_attributes.get("handleEnd")
    if frame_start is not None and handle_start is not None:
        folder_attributes["frameStartHandle"] = frame_start - handle_start

    if frame_end is not None and handle_end is not None:
        folder_attributes["frameEndHandle"] = frame_end + handle_end

    template_data = get_template_data(
        project_entity, folder_entity, task_entity, host_name
    )
    template_data["root"] = anatomy.roots
    template_data["folderAttributes"] = folder_attributes

    return template_data


def set_review_color_space(opengl_node, review_color_space="", log=None):
    """Set ociocolorspace parameter for the given OpenGL node.

    Set `ociocolorspace` parameter of the given OpenGl node
    to to the given review_color_space value.
    If review_color_space is empty, a default colorspace corresponding to
    the display & view of the current Houdini session will be used.

    Args:
        opengl_node (hou.Node): ROP node to set its ociocolorspace parm.
        review_color_space (str): Colorspace value for ociocolorspace parm.
        log (logging.Logger): Logger to log to.
    """

    if log is None:
        log = self.log

    # Set Color Correction parameter to OpenColorIO
    colorcorrect_parm = opengl_node.parm("colorcorrect")
    if colorcorrect_parm.eval() != 2:
        colorcorrect_parm.set(2)
        log.debug(
            "'Color Correction' parm on '{}' has been set to"
            " 'OpenColorIO'".format(opengl_node.path())
        )

    opengl_node.setParms(
        {"ociocolorspace": review_color_space}
    )

    log.debug(
        "'OCIO Colorspace' parm on '{}' has been set to "
        "the view color space '{}'"
        .format(opengl_node, review_color_space)
    )


def get_context_var_changes():
    """get context var changes."""

    houdini_vars_to_update = {}

    project_settings = get_current_project_settings()
    houdini_vars_settings = \
        project_settings["houdini"]["general"]["update_houdini_var_context"]

    if not houdini_vars_settings["enabled"]:
        return houdini_vars_to_update

    houdini_vars = houdini_vars_settings["houdini_vars"]

    # No vars specified - nothing to do
    if not houdini_vars:
        return houdini_vars_to_update

    # Get Template data
    template_data = get_current_context_template_data_with_folder_attrs()

    # Set Houdini Vars
    for item in houdini_vars:
        # For consistency reasons we always force all vars to be uppercase
        # Also remove any leading, and trailing whitespaces.
        var = item["var"].strip().upper()

        # get and resolve template in value
        item_value = StringTemplate.format_template(
            item["value"],
            template_data
        )

        if var == "JOB" and item_value == "":
            # sync $JOB to $HIP if $JOB is empty
            item_value = os.environ["HIP"]

        if item["is_directory"]:
            item_value = item_value.replace("\\", "/")

        current_value = hou.hscript("echo -n `${}`".format(var))[0]

        if current_value != item_value:
            houdini_vars_to_update[var] = (
                current_value, item_value, item["is_directory"]
            )

    return houdini_vars_to_update


def update_houdini_vars_context():
    """Update folder context variables"""

    for var, (_old, new, is_directory) in get_context_var_changes().items():
        if is_directory:
            try:
                os.makedirs(new)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    print(
                        "Failed to create ${} dir. Maybe due to "
                        "insufficient permissions.".format(var)
                    )

        hou.hscript("set {}={}".format(var, new))
        os.environ[var] = new
        print("Updated ${} to {}".format(var, new))


def update_houdini_vars_context_dialog():
    """Show pop-up to update folder context variables"""
    update_vars = get_context_var_changes()
    if not update_vars:
        # Nothing to change
        print("Nothing to change, Houdini vars are already up to date.")
        return

    message = "\n".join(
        "${}: {} -> {}".format(var, old or "None", new or "None")
        for var, (old, new, _is_directory) in update_vars.items()
    )

    # TODO: Use better UI!
    parent = hou.ui.mainQtWindow()
    dialog = SimplePopup(parent=parent)
    dialog.setModal(True)
    dialog.setWindowTitle("Houdini scene has outdated folder variables")
    dialog.set_message(message)
    dialog.set_button_text("Fix")

    # on_show is the Fix button clicked callback
    dialog.on_clicked.connect(update_houdini_vars_context)

    dialog.show()


def publisher_show_and_publish(comment=None):
    """Open publisher window and trigger publishing action.

    Args:
        comment (Optional[str]): Comment to set in publisher window.
    """

    main_window = get_main_window()
    publisher_window = get_tool_by_name(
        tool_name="publisher",
        parent=main_window,
    )
    publisher_window.show_and_publish(comment)


def find_rop_input_dependencies(input_tuple):
    """Self publish from ROP nodes.

    Arguments:
        tuple (hou.RopNode.inputDependencies) which can be a nested tuples
        represents the input dependencies of the ROP node, consisting of ROPs,
        and the frames that need to be be rendered prior to rendering the ROP.

    Returns:
        list of the RopNode.path() that can be found inside
        the input tuple.
    """

    out_list = []
    if isinstance(input_tuple[0], hou.RopNode):
        return input_tuple[0].path()

    if isinstance(input_tuple[0], tuple):
        for item in input_tuple:
            out_list.append(find_rop_input_dependencies(item))

    return out_list


def self_publish():
    """Self publish from ROP nodes.

    Firstly, it gets the node and its dependencies.
    Then, it deactivates all other ROPs
    And finally, it triggers the publishing action.
    """

    result, comment = hou.ui.readInput(
        "Add Publish Comment",
        buttons=("Publish", "Cancel"),
        title="Publish comment",
        close_choice=1
    )

    if result:
        return

    current_node = hou.node(".")
    inputs_paths = find_rop_input_dependencies(
        current_node.inputDependencies()
    )
    inputs_paths.append(current_node.path())

    host = registered_host()
    context = CreateContext(host, reset=True)

    for instance in context.instances:
        node_path = instance.data.get("instance_node")
        instance["active"] = node_path and node_path in inputs_paths

    context.save_changes()

    publisher_show_and_publish(comment)


def add_self_publish_button(node):
    """Adds a self publish button to the rop node."""

    label = os.environ.get("AYON_MENU_LABEL") or "AYON"

    button_parm = hou.ButtonParmTemplate(
        "ayon_self_publish",
        "{} Publish".format(label),
        script_callback="from ayon_houdini.api.lib import "
                        "self_publish; self_publish()",
        script_callback_language=hou.scriptLanguage.Python,
        join_with_next=True
    )

    template = node.parmTemplateGroup()
    template.insertBefore((0,), button_parm)
    node.setParmTemplateGroup(template)


def get_scene_viewer():
    """
    Return an instance of a visible viewport.

    There may be many, some could be closed, any visible are current

    Returns:
        Optional[hou.SceneViewer]: A scene viewer, if any.
    """
    panes = hou.ui.paneTabs()
    panes = [x for x in panes if x.type() == hou.paneTabType.SceneViewer]
    panes = sorted(panes, key=lambda x: x.isCurrentTab())
    if panes:
        return panes[-1]

    return None


def sceneview_snapshot(
        sceneview,
        filepath="$HIP/thumbnails/$HIPNAME.$F4.jpg",
        frame_start=None,
        frame_end=None):
    """Take a snapshot of your scene view.

    It takes snapshot of your scene view for the given frame range.
    So, it's capable of generating snapshots image sequence.
    It works in different Houdini context e.g. Objects, Solaris

    Example:
    	This is how the function can be used::

        	from ayon_houdini.api import lib
	        sceneview = hou.ui.paneTabOfType(hou.paneTabType.SceneViewer)
        	lib.sceneview_snapshot(sceneview)

    Notes:
        .png output will render poorly, so use .jpg.

        How it works:
            Get the current sceneviewer (may be more than one or hidden)
            and screengrab the perspective viewport to a file in the
            publish location to be picked up with the publish.

        Credits:
            https://www.sidefx.com/forum/topic/42808/?page=1#post-354796

    Args:
        sceneview (hou.SceneViewer): The scene view pane from which you want
                                     to take a snapshot.
        filepath (str): thumbnail filepath. it expects `$F4` token
                        when frame_end is bigger than frame_star other wise
                        each frame will override its predecessor.
        frame_start (int): the frame at which snapshot starts
        frame_end (int): the frame at which snapshot ends
    """

    if frame_start is None:
        frame_start = hou.frame()
    if frame_end is None:
        frame_end = frame_start

    if not isinstance(sceneview, hou.SceneViewer):
        log.debug("Wrong Input. {} is not of type hou.SceneViewer."
                  .format(sceneview))
        return
    viewport = sceneview.curViewport()

    flip_settings = sceneview.flipbookSettings().stash()
    flip_settings.frameRange((frame_start, frame_end))
    flip_settings.output(filepath)
    flip_settings.outputToMPlay(False)
    sceneview.flipbook(viewport, flip_settings)
    log.debug("A snapshot of sceneview has been saved to: {}".format(filepath))


def update_content_on_context_change():
    """Update all Creator instances to current asset"""
    host = registered_host()
    context = host.get_current_context()

    folder_path = context["folder_path"]
    task = context["task_name"]

    create_context = CreateContext(host, reset=True)

    for instance in create_context.instances:
        instance_folder_path = instance.get("folderPath")
        if instance_folder_path and instance_folder_path != folder_path:
            instance["folderPath"] = folder_path
        instance_task = instance.get("task")
        if instance_task and instance_task != task:
            instance["task"] = task

    create_context.save_changes()


def prompt_reset_context():
    """Prompt the user what context settings to reset.
    This prompt is used on saving to a different task to allow the scene to
    get matched to the new context.
    """
    # TODO: Cleanup this prototyped mess of imports and odd dialog
    from ayon_core.tools.attribute_defs.dialog import (
        AttributeDefinitionsDialog
    )
    from ayon_core.style import load_stylesheet
    from ayon_core.lib import BoolDef, UILabelDef

    definitions = [
        UILabelDef(
            label=(
                "You are saving your workfile into a different folder or task."
                "\n\n"
                "Would you like to update some settings to the new context?\n"
            )
        ),
        BoolDef(
            "fps",
            label="FPS",
            tooltip="Reset workfile FPS",
            default=True
        ),
        BoolDef(
            "frame_range",
            label="Frame Range",
            tooltip="Reset workfile start and end frame ranges",
            default=True
        ),
        BoolDef(
            "instances",
            label="Publish instances",
            tooltip="Update all publish instance's folder and task to match "
                    "the new folder and task",
            default=True
        ),
    ]

    dialog = AttributeDefinitionsDialog(definitions)
    dialog.setWindowTitle("Saving to different context.")
    dialog.setStyleSheet(load_stylesheet())
    if not dialog.exec_():
        return None

    options = dialog.get_values()
    if options["fps"] or options["frame_range"]:
        reset_framerange(
            fps=options["fps"],
            frame_range=options["frame_range"]
        )

    if options["instances"]:
        update_content_on_context_change()

    dialog.deleteLater()

def launch_workfiles_app():
    """Show workfiles tool on Houdini launch.

    Trigger to show workfiles tool on application launch. Can be executed only
    once all other calls are ignored.

    Workfiles tool show is deferred after application initialization using
    QTimer.
    """
    # Return early if environ doesn't exist or is set to False
    if not env_value_to_bool("AYON_WORKFILE_TOOL_ON_START"):
        return

    # If opening last workfile is enabled and last workfile path exists
    # ignore launching workfile tool
    if env_value_to_bool("AYON_OPEN_LAST_WORKFILE") and \
            env_value_to_bool("AYON_LAST_WORKFILE"):
        log.debug(
            "Last workfile path found so workfile tool won't be launched."
        )
        return

    # Parent tool to main Houdini window - if not found, we force the
    # tool to be on top to avoid it remaining hidden behind e.g. Houdini
    # window.
    # TODO: Check whether there are any cases where Houdini's main window
    #   does not exist yet.
    parent = get_main_window()
    on_top = not parent
    host_tools.show_workfiles(parent=parent, on_top=on_top)
