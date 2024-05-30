import traceback

# activate hiero from pype
from ayon_core.pipeline import install_host
from ayon_core.hosts.hiero import api as phiero
install_host(phiero)

try:
    __import__("ayon_core.hosts.hiero.api")
    __import__("pyblish")

except ImportError as e:
    print(traceback.format_exc())
    print("pyblish: Could not load integration: %s " % e)

else:
    # Setup integration
    phiero.lib.setup()
