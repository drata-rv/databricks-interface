# Vendored dependency wheels

Pre-downloaded wheels for every runtime dependency (direct and transitive), targeting the
actual platform Databricks serverless compute runs this job on: linux/x86_64, **Python 3.12**
(confirmed directly from a real deployment error on 2026-08-04 -- see "Python version
correction" below). Referenced directly in `databricks.yml`'s `environments.spec.dependencies`
so the job never needs PyPI reachability from serverless compute, which is not open by default
in this environment.

Committed to git deliberately (unlike `dist/`, which is a regenerated build artifact).

**Python version note:** `pyproject.toml`'s `requires-python = ">=3.8"` governs local
development and the base package only. These vendored wheels are hard-pinned to Python 3.12
(`cp312` ABI) to match the Databricks runtime above -- the Databricks-native job path does
not work on any other Python version regardless of what `requires-python` states, since these
exact wheel files (not a fresh PyPI resolution) are what gets installed on serverless compute.

**Python version correction (2026-08-04):** this was originally built for Python 3.10, based
on Databricks' public docs mapping `client: "1"` -> Python 3.10.12. That mapping was wrong for
this job/workspace -- the actual runtime error ("built for python 3.10 but their env [is] 3.12")
came directly from a real deployed run, overriding the docs-based assumption. Of these 13
wheels, only `charset_normalizer` had a Python-ABI-specific build (`cp310`/`cp312` etc.) rather
than a universal `py3-none-any` one, which is why every other fix attempted before this
(package rename, dropping protobuf, `dynamic_version`) left the real cause completely untouched
and the error never changed -- it was always this one file's ABI tag.

**charset_normalizer switched to the pure-Python wheel (2026-08-04):** rather than keep
rebuilding a `cpXXX`-tagged wheel every time Databricks' actual Python version shifts,
`charset_normalizer` is now vendored as `charset_normalizer-3.4.9-py3-none-any.whl`. Upstream
publishes this pure-Python build as a drop-in fallback for the mypyc-compiled default --
same behavior, no compiled extension, slightly slower. This removes the last ABI-specific
wheel from `vendor/`, so the whole directory is now Python-version-agnostic and this class of
failure can't recur. See the "Refreshing" section below: the general regen command still
pulls the `cpXXX` build for this package, so it needs the separate step called out there.

## Refreshing

Whenever `requirements.txt`/`pyproject.toml` dependencies change, regenerate this directory:

```bash
rm -rf vendor/*.whl
pip download -d vendor -r requirements.txt \
  --platform manylinux2014_x86_64 \
  --platform manylinux_2_17_x86_64 \
  --platform manylinux_2_28_x86_64 \
  --python-version 3.12 \
  --implementation cp \
  --abi cp312 \
  --only-binary=:all:
```

`--only-binary=:all:` is required -- without it, pip may fall back to a source tarball (e.g.
`cffi`) for a package with no matching prebuilt wheel, which can't be installed without a C
compiler on Databricks serverless compute.

The command above will re-fetch a `cpXXX`-tagged `charset_normalizer` wheel (pip prefers the
platform-specific build when one matches `--abi`/`--platform`). Replace it with the pure-Python
build every time:

```bash
rm -f vendor/charset_normalizer-*-cp*.whl
pip download -d vendor --no-deps charset-normalizer==3.4.9 \
  --python-version 3.12 --implementation py --abi none --platform any
```

If Databricks moves the job to a different environment with a different Python version, update
`--python-version`/`--abi` here to match -- confirm the actual running version directly (e.g.
via a deployment error, or printing `sys.version` from a task, as `smoke_test.py` does) rather
than trusting the public `client`/`environment_version` documentation, which was wrong here.

## Deliberately excluded: protobuf only

After the regeneration command above, delete `protobuf-*.whl` from the output before
committing. Databricks' own serverless kernel bootstrap (`dbruntime`) depends on grpc/protobuf
internally, and vendoring a newer protobuf than what's already installed overwrites it -- this
broke the Python kernel outright ("Failed to restart Python... may be due to updating the
version of a core Python package", `ModuleNotFoundError: dbruntime.overlay_magic`) on a real
run 2026-07-24. This is the *only* wheel excluded on purpose, and only because that specific
failure was directly observed -- do not add another package to this list on assumption alone.

## Pinned versions: databricks-sdk==0.72.0, google-auth==2.47.0 (2026-08-04)

**What actually broke, confirmed from a real failed run's diagnostic:** Databricks serverless
compute pre-installs `protobuf==5.29.4` and `google-auth==2.47.0`. `requirements.txt` had
`databricks-sdk>=0.27,<1.0` -- an unpinned range -- so every vendor regen grabbed whatever was
newest on PyPI at the time (`0.123.0`). That version's `protobuf` requirement is
`!=5.29.4,...,>=4.25.8,<7.0` -- it **explicitly excludes** the exact version already installed.
Since `protobuf` itself is deliberately excluded from `vendor/` (see above), pip had no local
wheel and no PyPI reachability to satisfy the now-broken constraint: "Library installation
failed... unsatisfiable dependency constraints."

Separately, that same `databricks-sdk` version's `google-auth~=2.0` pulled the newest matching
release (`2.56.2`), which added an **unconditional** `cryptography>=38.0.3` dependency (no
`extra ==` gate) that the pre-installed `2.47.0` never had. That forced vendoring
`cryptography`/`cffi`/`pycparser` -- all compiled, platform/arch-specific wheels -- which is
almost certainly what then produced the *next* failure ("platform tag mismatch or invalid
wheel"): guessing manylinux x86_64 tags blind, with no way to confirm the real compute
architecture from here.

**The actual fix: stop letting the vendor regen silently pick "whatever's newest."** Checked
`databricks-sdk`'s dependency metadata across its full release history (via each version's
wheel `METADATA`, not by guessing) and found `0.73.0` is exactly where the harsh protobuf
exclusion list was introduced -- `0.72.0` and earlier just require `protobuf<7.0,>=4.21.0`,
which `5.29.4` satisfies cleanly. And `google-auth==2.47.0` (the exact pre-installed version)
requires only `pyasn1-modules` and `rsa` at its core -- every `cryptography` line in its
metadata is gated behind an `extra ==` marker. Pure-Python signing, zero compiled wheels.

`requirements.txt`/`pyproject.toml` now hard-pin `databricks-sdk==0.72.0` and
`google-auth==2.47.0` instead of ranges. `vendor/` is the full dependency closure of that
pinned set minus only `protobuf` -- 13 wheels, every one of them `py3-none-any` /
`py2.py3-none-any`. No `cryptography`, no `cffi`, no `pycparser`, no ABI-specific build
anywhere in this directory. **Do not loosen these two pins back to a range** -- a future
regen would silently drift onto a newer `databricks-sdk`/`google-auth` and reintroduce the
exact protobuf/cryptography conflict this section just walked through. If a newer SDK version
is ever needed, first check its `Requires-Dist: protobuf` in the wheel `METADATA` against
whatever protobuf version is *actually* pre-installed at the time (print it from a real task
run -- don't assume it's still `5.29.4`).
