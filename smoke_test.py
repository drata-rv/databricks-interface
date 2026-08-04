"""
Zero-dependency smoke test for Databricks serverless compute.

Deliberately has no imports beyond the standard library and no relationship to this
project's own wheel/vendor/dependency setup. If this fails with the same kernel-restart
error extract_devices_job has been hitting, that proves the failure is workspace/platform
-side, not caused by anything in our own code or dependency tree -- see smoke_test_job in
databricks.yml.
"""

import platform
import sys

print("Databricks serverless compute smoke test: kernel started successfully.")
print(f"Python: {sys.version}")
print(f"Platform: {platform.platform()}")
