from ayon_core.pipeline import (
    load,
    get_representation_path,
)
from ayon_core.lib import BoolDef, EnumDef
from ayon_core.pipeline.load import LoadError
from ayon_core.hosts.substancepainter.api.pipeline import (
    imprint_container,
    set_container_metadata,
    remove_container_metadata
)
from ayon_core.hosts.substancepainter.api.lib import prompt_new_file_with_mesh

import substance_painter.project


class SubstanceLoadProjectMesh(load.LoaderPlugin):
    """Load mesh for project"""

    product_types = {"*"}
    representations = {"abc", "fbx", "obj", "gltf", "usd", "usda", "usdc"}

    label = "Load mesh"
    order = -10
    icon = "code-fork"
    color = "orange"

    @classmethod
    def get_options(cls, contexts):
        return [
            BoolDef("allow_user_setting",
                    default=True,
                    label="Allow User Setting",
                    tooltip=("Allow user to set up the project"
                             " by their own\n")),
            BoolDef("preserve_strokes",
                    default=True,
                    label="Preserve Strokes",
                    tooltip=("Preserve strokes positions on mesh.\n"
                             "(only relevant when loading into "
                             "existing project)")),
            BoolDef("import_cameras",
                    default=True,
                    label="Import Cameras",
                    tooltip="Import cameras from the mesh file."
            ),
            EnumDef("texture_resolution",
                    items=[128, 256, 512, 1024, 2048, 4096],
                    default=1024,
                    label="Texture Resolution",
                    tooltip="Set texture resolution when creating new project")
        ]

    def load(self, context, name, namespace, options=None):

        # Get user inputs
        allow_user_setting = options.get("allow_user_setting", True)
        import_cameras = options.get("import_cameras", True)
        preserve_strokes = options.get("preserve_strokes", True)
        texture_resolution = options.get("texture_resolution", 1024)
        sp_settings = substance_painter.project.Settings(
            default_texture_resolution=texture_resolution,
            import_cameras=import_cameras
        )
        if not substance_painter.project.is_open():
            # Allow to 'initialize' a new project
            path = self.filepath_from_context(context)
            # TODO: improve the prompt dialog function to not
            # only works for simple polygon scene
            result = prompt_new_file_with_mesh(mesh_filepath=path)
            if not result:
                self.log.info("User cancelled new project prompt."
                              "Creating new project directly from"
                              " Substance Painter API Instead.")
                settings = substance_painter.project.create(
                    mesh_file_path=path, settings=sp_settings
                )

        else:
            # Reload the mesh
            settings = substance_painter.project.MeshReloadingSettings(
                import_cameras=import_cameras,
                preserve_strokes=preserve_strokes
            )

            def on_mesh_reload(status: substance_painter.project.ReloadMeshStatus):  # noqa
                if status == substance_painter.project.ReloadMeshStatus.SUCCESS:  # noqa
                    self.log.info("Reload succeeded")
                else:
                    raise LoadError("Reload of mesh failed")

            path = self.filepath_from_context(context)
            substance_painter.project.reload_mesh(path,
                                                  settings,
                                                  on_mesh_reload)

        # Store container
        container = {}
        project_mesh_object_name = "_ProjectMesh_"
        imprint_container(container,
                          name=project_mesh_object_name,
                          namespace=project_mesh_object_name,
                          context=context,
                          loader=self)

        # We want store some options for updating to keep consistent behavior
        # from the user's original choice. We don't store 'preserve_strokes'
        # as we always preserve strokes on updates.
        container["options"] = {
            "import_cameras": import_cameras,
        }

        set_container_metadata(project_mesh_object_name, container)

    def switch(self, container, context):
        self.update(container, context)

    def update(self, container, context):
        repre_entity = context["representation"]

        path = get_representation_path(repre_entity)

        # Reload the mesh
        container_options = container.get("options", {})
        settings = substance_painter.project.MeshReloadingSettings(
            import_cameras=container_options.get("import_cameras", True),
            preserve_strokes=True
        )

        def on_mesh_reload(status: substance_painter.project.ReloadMeshStatus):
            if status == substance_painter.project.ReloadMeshStatus.SUCCESS:
                self.log.info("Reload succeeded")
            else:
                raise LoadError("Reload of mesh failed")

        substance_painter.project.reload_mesh(path, settings, on_mesh_reload)

        # Update container representation
        object_name = container["objectName"]
        update_data = {"representation": repre_entity["id"]}
        set_container_metadata(object_name, update_data, update=True)

    def remove(self, container):

        # Remove OpenPype related settings about what model was loaded
        # or close the project?
        # TODO: This is likely best 'hidden' away to the user because
        #       this will leave the project's mesh unmanaged.
        remove_container_metadata(container["objectName"])
