import os
import pyblish.api

from ayon_core.lib import path_tools
from ayon_core.pipeline import publish
from ayon_core.modules.delivery.scripts import review


class ExtractReviewNuke(publish.Extractor):
    """Generate review media through a Nuke deadline task"""

    order = pyblish.api.ExtractorOrder + 0.02
    label = "Extract Review Nuke"
    families = ["review"]

    def process(self, instance):
        """Submit a job to the farm to generate a mov review media to upload to SG"""

        # Skip review when requested
        if not instance.data.get("review", True):
            return

        if not instance.data.get("farm"):
            self.log.warning(
                "Extract review in Nuke only works when publishing in the farm."
            )
            return

        # Skip review generation if there's already a video representation
        for repre in instance.data["representations"]:
            input_ext = repre["ext"]
            if input_ext.startswith("."):
                input_ext = input_ext[1:]

            if input_ext in review.VIDEO_EXTENSIONS:
                self.log.info(
                    "There's already a video representation with extension '%s', skipping generation of review.",
                        input_ext
                    )
                return

        instance.data["toBeRenderedOn"] = "deadline"

        context = instance.context

        anatomy = context.data["anatomy"]
        staging_dir = anatomy.fill_root(
            instance.data["outputDir"]
        )

        base_path = None
        output_path = None
        src_colorspace = "scene_linear"

        # If there's job dependencies it means there's a prior Deadline task that might be
        # generating files that will be used to generate the review (i.e., transcode frames)
        job_dependencies = instance.data.get("deadlineSubmissionJobs")
        if job_dependencies:
            base_path = instance.data["expectedFiles"][0]
            output_path = "{}_h264.mov".format(base_path.split(".", 1)[0])
            src_colorspace = instance.data.get("colorspace")
        # Otherwise we just iterate from the created representations to generate the review from
        else:
            self.log.info("No Deadline job dependencies, checking instance representations.")
            for repre in self.get_review_representations(instance):
                # Create read path
                basename = repre["files"][0] if isinstance(repre["files"], list) else repre["files"]
                base_path = os.path.join(staging_dir, basename)

                # Create review output path
                output_path = os.path.join(
                    staging_dir,
                    f"{instance.data['productName']}_{repre['name']}_h264.mov"
                )

                # Get source colorspace from representation
                colorspace_data = repre.get("colorspaceData")
                if colorspace_data:
                    self.log.debug(
                        "Setting 'src_colorspace' to `%s`", colorspace_data["colorspace"]
                    )
                    src_colorspace = colorspace_data["colorspace"]

                break

        if not (base_path and output_path):
            self.log.info(
                "Skipping review generation as it couldn't find any representations to generate from."
            )
            return

        read_path = path_tools.replace_frame_number_with_token(base_path, "#", padding=True)

        # Name to use for batch grouping Deadline tasks
        batch_name = instance.data.get("deadlineBatchName") or os.path.basename(
            context.data.get("currentFile")
        )

        # Create dictionary with other useful data required to submit
        # Nuke review job to the farm
        review_data = {
            "comment": instance.data.get("comment", ""),
            "batch_name": batch_name,
        }

        # Grab frame start/end
        if "srcFrameRange" in instance.data:
            self.log.debug("Grabbed frame range from source media")
            frame_start, frame_end = instance.data["srcFrameRange"]
            # Calculate the frame offset from source range to destination range
            review_data["frame_offset"] = instance.data["frameStart"] - frame_start
        else:
            frame_start = instance.data["frameStart"]
            frame_end = instance.data["frameEnd"]

        self.log.debug("Output path: %s", output_path)

        # Add source colorspace if it's set on the representation
        if src_colorspace:
            self.log.debug(
                "src_colorspace set to `%s`", src_colorspace
            )
            review_data["src_colorspace"] = src_colorspace
        else:
            self.log.debug(
                "src_colorspace not set, skipping colorspace info."
            )

        # TODO: Hard-code out_colorspace to `shot_lut` for now but we will want to
        # control when we want it applied or not
        self.log.debug(
            "out_colorspace set to `shot_lut`"
        )
        review_data["out_colorspace"] = "shot_lut"

        # Submit job to the farm
        response = review.generate_review(
            os.getenv("AYON_PROJECT_NAME"),
            os.getenv("SHOW"),
            instance.data["folderPath"],
            instance.data.get("task", os.getenv("AYON_TASK_NAME")),
            read_path,
            output_path,
            frame_start,
            frame_end,
            review_data,
            job_dependencies
        )

        # Adding the review file that will be generated to expected files
        if not instance.data.get("expectedFiles"):
            instance.data["expectedFiles"] = []

        instance.data["expectedFiles"].append(output_path)
        self.log.debug(
            "__ expectedFiles: `{}`".format(instance.data["expectedFiles"])
        )

        if job_dependencies:
            instance.data["deadlineSubmissionJobs"].append(response)
        else:
            instance.data["deadlineSubmissionJobs"] = [response]

    def get_review_representations(self, instance):
        for repre in instance.data["representations"]:
            repre_name = str(repre.get("name"))
            self.log.debug("Looking to see if we should generate review for '%s'", repre_name)

            tags = repre.get("tags") or []

            if repre_name == "exr_fr":
                self.log.debug("Full resolution representation, skipping.")
                continue

            if "review" not in tags:
                self.log.debug((
                    "Repre: {} - Didn't found \"review\" in tags. Skipping"
                ).format(repre_name))
                continue

            if "thumbnail" in tags:
                self.log.debug((
                    "Repre: {} - Found \"thumbnail\" in tags. Skipping"
                ).format(repre_name))
                continue

            if "passing" in tags:
                self.log.debug((
                    "Repre: {} - Found \"passing\" in tags. Skipping"
                ).format(repre_name))
                continue

            input_ext = repre["ext"]
            if input_ext.startswith("."):
                input_ext = input_ext[1:]

            if input_ext not in review.GENERATE_REVIEW_EXTENSIONS:
                self.log.info(
                    "Representation is not an image extension and doesn't need a review generated \"{}\"".format(
                        input_ext
                    )
                )
                continue

            yield repre
