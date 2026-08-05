"""
Zero-dependency smoke test for Databricks serverless compute.

Deliberately has no imports beyond the standard library and no relationship to this
project's own wheel/vendor/dependency setup. If this fails with the same kernel-restart
error extract_devices_job has been hitting, that proves the failure is workspace/platform
-side, not caused by anything in our own code or dependency tree -- see smoke_test_job in
databricks.yml.
"""

import os
import platform
import sys

print("Databricks serverless compute smoke test: kernel started successfully.")
print(f"Python: {sys.version}")
print(f"Platform: {platform.platform()}")
# db/secrets.py::is_databricks_runtime() assumes this is set on serverless, same as on
# classic clusters -- never actually confirmed against a real serverless run. Print it here
# so the first real deploy settles it instead of leaving it an assumption.
print(f"DATABRICKS_RUNTIME_VERSION: {os.environ.get('DATABRICKS_RUNTIME_VERSION', '(not set)')}")
