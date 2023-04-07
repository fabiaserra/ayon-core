# -*- coding: utf-8 -*-
import pyblish.api
import hou
from openpype.pipeline import PublishXmlValidationError


class ValidateVDBOutputNode(pyblish.api.InstancePlugin):
    """Validate that the node connected to the output node is of type VDB.

    Regardless of the amount of VDBs create the output will need to have an
    equal amount of VDBs, points, primitives and vertices

    A VDB is an inherited type of Prim, holds the following data:
        - Primitives: 1
        - Points: 1
        - Vertices: 1
        - VDBs: 1

    """

    order = pyblish.api.ValidatorOrder + 0.1
    families = ["vdbcache"]
    hosts = ["houdini"]
    label = "Validate Output Node (VDB)"

    def process(self, instance):
        invalid = self.get_invalid(instance)
        if invalid:
            raise PublishXmlValidationError(
                self,
                "Node connected to the output node is not" " of type VDB!"
            )

    @classmethod
    def get_invalid(cls, instance):

        node = instance.data["output_node"]
        if node is None:
            cls.log.error(
                "SOP path is not correctly set on "
                "ROP node '%s'." % instance.data.get("instance_node")
            )
            return [instance]

        frame = instance.data.get("frameStart", 0)
        geometry = node.geometryAtFrame(frame)
        if geometry is None:
            # No geometry data on this node, maybe the node hasn't cooked?
            cls.log.error(
                "SOP node has no geometry data. "
                "Is it cooked? %s" % node.path()
            )
            return [node]

        prims = geometry.prims()
        nr_of_prims = len(prims)

        # All primitives must be hou.VDB
        invalid_prims = []
        for prim in prims:
            if not isinstance(prim, hou.VDB):
                invalid_prims.append(prim)
        if invalid_prims:
            # TODO Log all invalid primitives in a short readable way, like 0-5
            # This logging can be really slow for many primitives, say 20000+
            # which might be fixed by logging only consecutive ranges
            cls.log.error(
                "Found non-VDB primitives for '{}', "
                "primitive indices: {}".format(
                    node.path(),
                    ", ".join(str(prim.number()) for prim in invalid_prims)
                )
            )
            return [instance]

        nr_of_points = len(geometry.points())
        if nr_of_points != nr_of_prims:
            cls.log.error("The number of primitives and points do not match")
            return [instance]

        for prim in prims:
            if prim.numVertices() != 1:
                cls.log.error("Found primitive with more than 1 vertex!")
                return [instance]
