# Vendored dependency wheels

Pre-downloaded wheels for every runtime dependency (direct and transitive), targeting the
exact platform Databricks serverless environment `client: "1"` runs on -- Ubuntu 22.04.4 LTS,
Python 3.10.12, linux/x86_64. Referenced directly in `databricks.yml`'s `environments.spec.dependencies`
so the job never needs PyPI reachability from serverless compute, which is not open by default
in this environment.

Committed to git deliberately (unlike `dist/`, which is a regenerated build artifact).

## Refreshing

Whenever `requirements.txt`/`pyproject.toml` dependencies change, regenerate this directory:

```bash
rm -rf vendor/*.whl
pip download -d vendor -r requirements.txt \
  --platform manylinux2014_x86_64 \
  --platform manylinux_2_17_x86_64 \
  --platform manylinux_2_28_x86_64 \
  --python-version 3.10 \
  --implementation cp \
  --abi cp310 \
  --only-binary=:all:
```

`--only-binary=:all:` is required -- without it, pip may fall back to a source tarball (e.g.
`cffi`) for a package with no matching prebuilt wheel, which can't be installed without a C
compiler on Databricks serverless compute.

If Databricks moves the job to a different `client`/`environment_version` with a different
Python version (see `databricks.yml`'s `environment_key`), update `--python-version`/`--abi`
here to match -- check the current mapping at
https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/.
