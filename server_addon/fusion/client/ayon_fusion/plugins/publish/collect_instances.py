import pyblish.api


class CollectInstanceData(pyblish.api.InstancePlugin):
    """Collect Fusion saver instances

    This additionally stores the Comp start and end render range in the
    current context's data as "frameStart" and "frameEnd".

    """

    order = pyblish.api.CollectorOrder
    label = "Collect Instances Data"
    hosts = ["fusion"]

    def process(self, instance):
        """Collect all image sequence tools"""

        context = instance.context

        # Include creator attributes directly as instance data
        creator_attributes = instance.data["creator_attributes"]
        instance.data.update(creator_attributes)

        frame_range_source = creator_attributes.get("frame_range_source")
        instance.data["frame_range_source"] = frame_range_source

        # get folder frame ranges to all instances
        # render product type instances `current_folder` render target
        start = context.data["frameStart"]
        end = context.data["frameEnd"]
        handle_start = context.data["handleStart"]
        handle_end = context.data["handleEnd"]
        start_with_handle = start - handle_start
        end_with_handle = end + handle_end

        # conditions for render product type instances
        if frame_range_source == "render_range":
            # set comp render frame ranges
            start = context.data["renderFrameStart"]
            end = context.data["renderFrameEnd"]
            handle_start = 0
            handle_end = 0
            start_with_handle = start
            end_with_handle = end

        if frame_range_source == "comp_range":
            comp_start = context.data["compFrameStart"]
            comp_end = context.data["compFrameEnd"]
            render_start = context.data["renderFrameStart"]
            render_end = context.data["renderFrameEnd"]
            # set comp frame ranges
            start = render_start
            end = render_end
            handle_start = render_start - comp_start
            handle_end = comp_end - render_end
            start_with_handle = comp_start
            end_with_handle = comp_end

        if frame_range_source == "custom_range":
            start = int(instance.data["custom_frameStart"])
            end = int(instance.data["custom_frameEnd"])
            handle_start = int(instance.data["custom_handleStart"])
            handle_end = int(instance.data["custom_handleEnd"])
            start_with_handle = start - handle_start
            end_with_handle = end + handle_end

        frame = instance.data["creator_attributes"].get("frame")
        # explicitly publishing only single frame
        if frame is not None:
            frame = int(frame)

            start = frame
            end = frame
            handle_start = 0
            handle_end = 0
            start_with_handle = frame
            end_with_handle = frame

        # Include start and end render frame in label
        product_name = instance.data["productName"]
        label = (
            "{product_name} ({start}-{end}) [{handle_start}-{handle_end}]"
        ).format(
            product_name=product_name,
            start=int(start),
            end=int(end),
            handle_start=int(handle_start),
            handle_end=int(handle_end)
        )

        instance.data.update({
            "label": label,

            # todo: Allow custom frame range per instance
            "frameStart": start,
            "frameEnd": end,
            "frameStartHandle": start_with_handle,
            "frameEndHandle": end_with_handle,
            "handleStart": handle_start,
            "handleEnd": handle_end,
            "fps": context.data["fps"],
        })

        # Add review family if the instance is marked as 'review'
        # This could be done through a 'review' Creator attribute.
        if instance.data.get("review", False):
            self.log.debug("Adding review family..")
            instance.data["families"].append("review")
