"""Load a model asset in Blender."""

from pathlib import Path
from pprint import pformat
from typing import Dict, List, Optional

import bpy

from avalon import api
from avalon.blender.pipeline import AVALON_CONTAINERS
from avalon.blender.pipeline import AVALON_CONTAINER_ID
from avalon.blender.pipeline import AVALON_PROPERTY
from openpype.hosts.blender.api import plugin


class BlendModelLoader(plugin.AssetLoader):
    """Load models from a .blend file.

    Because they come from a .blend file we can simply link the collection that
    contains the model. There is no further need to 'containerise' it.
    """

    families = ["model"]
    representations = ["blend"]

    label = "Link Model"
    icon = "code-fork"
    color = "orange"

    def _remove(self, asset_group):
        objects = list(asset_group.children)
        empties = []

        for obj in objects:
            if obj.type == 'MESH':
                for material_slot in list(obj.material_slots):
                    bpy.data.materials.remove(material_slot.material)
                bpy.data.meshes.remove(obj.data)
            elif obj.type == 'EMPTY':
                objects.extend(obj.children)
                empties.append(obj)

        for empty in empties:
            bpy.data.objects.remove(empty)

    def _process(self, libpath, asset_group, group_name):
        relative = bpy.context.preferences.filepaths.use_relative_paths
        with bpy.data.libraries.load(
            libpath, link=True, relative=relative
        ) as (data_from, data_to):
            data_to.objects = data_from.objects

        parent = bpy.context.scene.collection

        empties = [obj for obj in data_to.objects if obj.type == 'EMPTY']

        container = None

        for empty in empties:
            if empty.get(AVALON_PROPERTY):
                container = empty
                break

        assert container, "No asset group found"

        # Children must be linked before parents,
        # otherwise the hierarchy will break
        objects = []
        nodes = list(container.children)

        for obj in nodes:
            obj.parent = asset_group

        for obj in nodes:
            objects.append(obj)
            nodes.extend(list(obj.children))

        objects.reverse()

        for obj in objects:
            parent.objects.link(obj)

        for obj in objects:
            local_obj = plugin.prepare_data(obj, group_name)
            if obj.type != 'EMPTY':
                plugin.prepare_data(local_obj.data, group_name)

                for material_slot in local_obj.material_slots:
                    plugin.prepare_data(material_slot.material, group_name)

            if not obj.get(AVALON_PROPERTY):
                local_obj[AVALON_PROPERTY] = dict()

            avalon_info = local_obj[AVALON_PROPERTY]
            avalon_info.update({"container_name": group_name})

        objects.reverse()

        bpy.ops.object.select_all(action='DESELECT')

        return objects

    def process_asset(
        self, context: dict, name: str, namespace: Optional[str] = None,
        options: Optional[Dict] = None
    ) -> Optional[List]:
        """
        Arguments:
            name: Use pre-defined name
            namespace: Use pre-defined namespace
            context: Full parenthood of representation to load
            options: Additional settings dictionary
        """
        libpath = self.fname
        asset = context["asset"]["name"]
        subset = context["subset"]["name"]

        asset_name = plugin.asset_name(asset, subset)
        unique_number = plugin.get_unique_number(asset, subset)
        group_name = plugin.asset_name(asset, subset, unique_number)
        namespace = namespace or f"{asset}_{unique_number}"

        avalon_container = bpy.data.collections.get(AVALON_CONTAINERS)
        if not avalon_container:
            avalon_container = bpy.data.collections.new(name=AVALON_CONTAINERS)
            bpy.context.scene.collection.children.link(avalon_container)

        asset_group = bpy.data.objects.new(group_name, object_data=None)
        avalon_container.objects.link(asset_group)

        objects = self._process(libpath, asset_group, group_name)

        bpy.context.scene.collection.objects.link(asset_group)

        asset_group[AVALON_PROPERTY] = {
            "schema": "openpype:container-2.0",
            "id": AVALON_CONTAINER_ID,
            "name": name,
            "namespace": namespace or '',
            "loader": str(self.__class__.__name__),
            "representation": str(context["representation"]["_id"]),
            "libpath": libpath,
            "asset_name": asset_name,
            "parent": str(context["representation"]["parent"]),
            "family": context["representation"]["context"]["family"]
        }

        nodes = objects
        self[:] = nodes
        return nodes

    def update(self, container: Dict, representation: Dict):
        """Update the loaded asset.

        This will remove all objects of the current collection, load the new
        ones and add them to the collection.
        If the objects of the collection are used in another collection they
        will not be removed, only unlinked. Normally this should not be the
        case though.
        """
        object_name = container["objectName"]
        asset_group = bpy.data.objects.get(object_name)
        libpath = Path(api.get_representation_path(representation))
        extension = libpath.suffix.lower()

        self.log.info(
            "Container: %s\nRepresentation: %s",
            pformat(container, indent=2),
            pformat(representation, indent=2),
        )

        assert asset_group, (
            f"The asset is not loaded: {container['objectName']}"
        )
        assert libpath, (
            "No existing library file found for {container['objectName']}"
        )
        assert libpath.is_file(), (
            f"The file doesn't exist: {libpath}"
        )
        assert extension in plugin.VALID_EXTENSIONS, (
            f"Unsupported file: {libpath}"
        )

        metadata = asset_group.get(AVALON_PROPERTY)
        group_libpath = metadata["libpath"]

        normalized_group_libpath = (
            str(Path(bpy.path.abspath(group_libpath)).resolve())
        )
        normalized_libpath = (
            str(Path(bpy.path.abspath(str(libpath))).resolve())
        )
        self.log.debug(
            "normalized_group_libpath:\n  %s\nnormalized_libpath:\n  %s",
            normalized_group_libpath,
            normalized_libpath,
        )
        if normalized_group_libpath == normalized_libpath:
            self.log.info("Library already loaded, not updating...")
            return

        self._remove(asset_group)

        self._process(str(libpath), asset_group, object_name)

        metadata["libpath"] = str(libpath)
        metadata["representation"] = str(representation["_id"])

    def remove(self, container: Dict) -> bool:
        """Remove an existing container from a Blender scene.

        Arguments:
            container (openpype:container-1.0): Container to remove,
                from `host.ls()`.

        Returns:
            bool: Whether the container was deleted.
        """
        object_name = container["objectName"]
        asset_group = bpy.data.objects.get(object_name)

        if not asset_group:
            return False

        self._remove(asset_group)

        bpy.data.objects.remove(asset_group)

        return True
