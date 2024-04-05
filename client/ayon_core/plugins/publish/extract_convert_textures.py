# -*- coding: utf-8 -*-
"""Convert exrs in representation to tiled exrs usin oiio tools."""
import os

import pyblish.api

from ayon_core.lib import (
    run_subprocess,
    get_oiio_tool_args,
    get_oiio_info_for_input,
    ToolNotFoundError,
)
from ayon_core.pipeline import (
    get_current_project_name,
    get_current_host_name
)
from ayon_core.pipeline.colorspace import (
    get_imageio_config
)
from ayon_core.modules.ingest.lib import textures


class ExtractConvertTextures(pyblish.api.InstancePlugin):
    """Convert texture to .tx using OIIO maketx tool."""

    label = "Extract Texture TX"
    hosts = ["shell"]
    order = pyblish.api.ExtractorOrder
    families = ["textures"]

    def process(self, instance):
        """Plugin entry point."""
        representations = instance.data["representations"]

        in_colorspace = instance.data.get("colorspace")
        render_colorspace = "ACEScg"
        imageio_config = get_imageio_config(
            project_name=get_current_project_name(),
            host_name=get_current_host_name()
        )

        for repre in representations:
            self.log.debug(
                "Processing representation {}".format(repre.get("name")))

            # tags = repre.get("tags", [])
            # if "toTx" not in tags:
            #     self.log.debug(" - missing toTx tag")
            #     continue

            if isinstance(repre["files"], (list, tuple)):
                self.log.warning("We don't support multiple files for the textures family")

            if not isinstance(repre["files"], (list, tuple)):
                texture_files = [repre["files"]]
                self.log.debug("We have a single frame")
            else:
                texture_files = repre["files"]
                self.log.debug("We have a sequence")

            stagingdir = os.path.normpath(repre.get("stagingDir"))

            # TODO: abstract away so it's dynamic based on runtime
            # for now we can simply hard-code the path to the maketx binary
            # as we mount /sw to all of our workers
            # try:
            #     maketx_args = get_oiio_tool_args("maketx")
            # except ToolNotFoundError:
            #     self.log.error("OIIO tool not found.")
            #     raise KnownPublishError("OIIO tool not found")
            maketx_args = ["/sw/arnold/mtoa/2024_5.3.2.1/bin/maketx"]
            self.log.debug("Found 'maketx' binary at %s", maketx_args)

            updated_files = []

            for texture_file in texture_files:
                original_path = os.path.join(stagingdir, texture_file)
                img_info = get_oiio_info_for_input(original_path)

                destination_filename = f"{os.path.splitext(texture_file)[0]}.tx"
                destination_path = os.path.join(
                    stagingdir, destination_filename
                )

                maketx_args.extend([
                    "-v",
                    "-u",  # update mode
                    # unpremultiply before conversion (recommended when alpha present)Fbit
                    "--unpremult",
                    # use oiio-optimized settings for tile-size, planarconfig, metadata
                    "--oiio",
                    # --checknan doesn't influence the output file but aborts the
                    # conversion if it finds any. So we can avoid it for the file hash
                    "--checknan",
                    original_path,
                    "--filter", "lanczos3",
                    "-o", destination_path
                ])

                # promote 8-bit images to EXR half with DWAA compression to avoid quantization errors (#795)
                if "linear" not in in_colorspace and img_info["format"] in textures.BIT_DEPTHS_SRGB:
                    maketx_args.extend(
                        [
                            "--format", "exr",
                            "-d", "half",
                            "--compression", "dwaa"
                        ]
                    )

                if imageio_config:
                    maketx_args.extend(
                        [
                            "--colorconfig", imageio_config["path"],
                            "--colorconvert", in_colorspace, render_colorspace,
                        ]
                    )

                self.log.debug(f"running: {' '.join(maketx_args)}")
                try:
                    run_subprocess(maketx_args, logger=self.log)
                except Exception:
                    self.log.error(
                        "Texture maketx conversion failed", exc_info=True
                    )
                    raise

                # raise error if there is no ouptput
                if not os.path.exists(destination_path):
                    self.log.error(
                        f"File {destination_path} was not converted by oiio tool!"
                    )
                    raise AssertionError("OIIO tool conversion failed")

                # Append updated texture to list so we can override "files" field
                # from the representation
                updated_files.append(destination_filename)

            try:
                repre["tags"].remove("toTx")
            except ValueError:
                # no `toTx` tag present
                pass

            # Update representation file to point to the new tx file
            repre["ext"] = "tx"
            repre["files"] = updated_files
