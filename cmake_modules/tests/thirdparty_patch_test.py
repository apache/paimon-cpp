# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Offline ExternalProject lifecycle tests. Requires patch and lsdiff (patchutils).

No compiler or downloaded packages needed.

python3 cmake_modules/tests/thirdparty_patch_test.py --generator Ninja
python3 cmake_modules/tests/thirdparty_patch_test.py --generator 'Unix Makefiles'
Use --cmake to also test the minimum supported CMake version.
"""

import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest


MODULES = Path(__file__).resolve().parents[1]
PARSER = argparse.ArgumentParser()
PARSER.add_argument("--generator", default="Ninja")
PARSER.add_argument("--cmake", default="cmake")
ARGS, UNITTEST_ARGS = PARSER.parse_known_args()


def patch(before, after, filename="value"):
    return ("diff --git a/{0} b/{0}\n--- a/{0}\n+++ b/{0}\n"
            "@@ -1 +1 @@\n-{1}\n+{2}\n").format(filename, before, after)


class ThirdpartyPatchTest(unittest.TestCase):
    def setUp(self):
        self.create_project()

    def create_project(self):
        self.temp = tempfile.TemporaryDirectory(prefix="paimon patch ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.project = self.root / "project"
        self.project.mkdir()
        self.modules = self.project / "cmake_modules"
        self.modules.mkdir()
        for name in ["ThirdpartyPatch.cmake", "ApplyThirdpartyPatch.cmake"]:
            shutil.copyfile(str(MODULES / name), str(self.modules / name))
        (self.project / "third_party").mkdir()
        # Exercise git parent discovery: source archives live under this repo.
        self.command(["git", "init", "-q", str(self.project)])
        self.build = self.project / "build"
        self.source = self.build / "dep-prefix/src/dep"
        self.install = self.build / "dep-install"
        self.stamp = self.build / "dep-prefix/src/dep-stamp"
        self.package = self.root / "archive-source"
        self.package.mkdir()
        (self.package / "value").write_text("alpha\n")
        (self.package / "other").write_text("unchanged\n")
        self.archive = self.project / "dep.tar.gz"
        self.make_archive()
        self.write_versions()
        (self.project / "one.diff").write_text(patch("alpha", "beta"))
        (self.project / "two.diff").write_text(patch("beta", "gamma"))
        (self.project / "input.rs").write_text("input\n")
        self.write_project()

    def command(self, command, success=True):
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                universal_newlines=True)
        if success:
            self.assertEqual(result.returncode, 0, result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def make_archive(self):
        with tarfile.open(str(self.archive), "w:gz") as archive:
            archive.add(str(self.package), arcname="dep")
        self.sha = hashlib.sha256(self.archive.read_bytes()).hexdigest()

    def write_versions(self, version="1", checksum=None, dependency="1"):
        (self.project / "third_party/versions.txt").write_text(
            "DEP_VERSION={}\nDEP_SHA={}\nOTHER_VERSION={}\n".format(
                version, checksum or self.sha, dependency))

    def write_project(self, patches="one.diff two.diff", extra="", layout=""):
        (self.project / "record.cmake").write_text('''
file(APPEND "${CMAKE_CURRENT_LIST_DIR}/events" "${PHASE}\\n")
if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/fail-${PHASE}")
    message(FATAL_ERROR "injected ${PHASE} failure")
endif()
if(PHASE STREQUAL "build")
    file(READ "${SOURCE}/value" value)
    file(WRITE "${BINARY}/built-value" "${value}")
elseif(PHASE STREQUAL "install")
    file(MAKE_DIRECTORY "${DEST}")
    file(COPY "${BINARY}/built-value" DESTINATION "${DEST}")
endif()
''')
        text = '''cmake_minimum_required(VERSION 3.22)
project(PatchTest NONE)
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()
include("@MODULES@/ThirdpartyPatch.cmake")
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/versions.txt" versions)
foreach(entry IN LISTS versions)
    string(REPLACE "=" ";" fields "${entry}")
    list(GET fields 0 key)
    list(GET fields 1 value)
    set(${key} "${value}")
endforeach()
set(patches @PATCHES@)
list(TRANSFORM patches PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
set(dest "${CMAKE_CURRENT_BINARY_DIR}/dep-install")
set(extra_options "")
@LAYOUT@
paimon_patch_command(dep patch_command)
externalproject_add(dep
    URL "${CMAKE_CURRENT_SOURCE_DIR}/dep.tar.gz"
    URL_HASH "SHA256=${DEP_SHA}"
    ${extra_options}
    PATCH_COMMAND ${patch_command}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND "${CMAKE_COMMAND}" -DPHASE=build "-DSOURCE=<SOURCE_DIR>"
                  "-DBINARY=<BINARY_DIR>" "-DDEST=${dest}"
                  -P "${CMAKE_CURRENT_SOURCE_DIR}/record.cmake"
    INSTALL_COMMAND "${CMAKE_COMMAND}" -DPHASE=install "-DSOURCE=<SOURCE_DIR>"
                    "-DBINARY=<BINARY_DIR>" "-DDEST=${dest}"
                    -P "${CMAKE_CURRENT_SOURCE_DIR}/record.cmake"
    BUILD_BYPRODUCTS "${dest}/built-value")
paimon_register_patched_project(dep
    VERSION "${DEP_VERSION}"
    PATCHES ${patches}
    INPUTS "${CMAKE_CURRENT_SOURCE_DIR}/input.rs"
    INSTALL_DIR "${dest}"
    VERSION_VARIABLES DEP_VERSION DEP_SHA OTHER_VERSION)
@EXTRA@
'''
        for key, value in [("MODULES", str(self.modules)), ("PATCHES", patches),
                           ("EXTRA", extra), ("LAYOUT", layout)]:
            text = text.replace("@{}@".format(key), value)
        (self.project / "CMakeLists.txt").write_text(text)

    def configure(self, success=True, options=()):
        return self.command([ARGS.cmake, "-S", str(self.project), "-B", str(self.build),
                             "-G", ARGS.generator] + list(options), success)

    def compile(self, success=True, target="dep"):
        return self.command([ARGS.cmake, "--build", str(self.build), "--target", target,
                             "--parallel", "4"], success)

    def built(self):
        self.configure()
        self.compile()
        self.assertEqual((self.install / "built-value").read_text(), "gamma\n")

    def assert_cleanup(self, output):
        self.assertIn("Remove these build-owned paths", " ".join(output.split()))
        self.assertIn(str(self.source), output)
        self.assertIn(str(self.install), output)
        self.assertIn(str(self.stamp), output)

    def assert_reuse_rejected(self, targets=("dep",)):
        for target in targets:
            with self.subTest(target=target):
                self.assert_cleanup(self.compile(False, target=target))
        self.assert_cleanup(self.command([ARGS.cmake, "--install", str(self.build)], False))
        self.assertEqual((self.project / "events").read_text(), "build\ninstall\n")
        self.assertEqual((self.install / "built-value").read_text(), "gamma\n")

    def clean_reported_paths(self, output):
        paths = {Path(line.strip()) for line in output.splitlines()
                 if line.strip().startswith(str(self.build) + os.sep)}
        self.assertTrue(paths, output)
        for path in paths:
            self.assertIn(self.build.resolve(), path.resolve().parents)
            if path.is_dir():
                shutil.rmtree(str(path))
            elif path.exists():
                path.unlink()

    def test_ordered_patches_and_repeat_build(self):
        self.built()
        artifact = self.install / "built-value"
        timestamp = artifact.stat().st_mtime_ns
        self.compile()
        self.configure()
        self.compile()
        self.assertEqual((self.project / "events").read_text(), "build\ninstall\n")
        self.assertEqual(artifact.stat().st_mtime_ns, timestamp)

    def test_changed_patch_detected_even_with_preserved_timestamp(self):
        self.built()
        path = self.project / "two.diff"
        stat = path.stat()
        path.write_text(patch("beta", "delta"))
        os.utime(str(path), ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.assert_reuse_rejected()

    def test_multifile_patch_with_brackets_and_header_like_content(self):
        (self.package / "brackets").write_text("unchanged\n")
        (self.package / "other").write_text("-- a/phantom\n")
        self.make_archive()
        self.write_versions()
        (self.project / "one.diff").write_text(
            patch("unchanged", "[;", "brackets") +
            patch("-- a/phantom", "++ b/phantom", "other") + patch("alpha", "gamma"))
        self.write_project(patches="one.diff")
        self.built()
        for name, expected in [("brackets", "[;\n"), ("other", "++ b/phantom\n"),
                               ("value", "gamma\n")]:
            source = self.source / name
            self.assertEqual(source.read_text(), expected)
            for replacement in ["corrupted\n", None]:
                with self.subTest(file=name, replacement=replacement):
                    if replacement is None:
                        source.unlink()
                    else:
                        source.write_text(replacement)
                    try:
                        self.assert_reuse_rejected()
                    finally:
                        source.write_text(expected)
        self.compile()

    def test_missing_lsdiff_reports_required_package(self):
        output = self.configure(False, options=[
            "-DCMAKE_FIND_USE_CMAKE_SYSTEM_PATH=OFF",
            "-DCMAKE_FIND_USE_SYSTEM_ENVIRONMENT_PATH=OFF",
            "-DCMAKE_FIND_USE_CMAKE_ENVIRONMENT_PATH=OFF",
            "-DCMAKE_MAKE_PROGRAM=" + shutil.which(
                "ninja" if ARGS.generator == "Ninja" else "make"),
            "-DPP_PATCH_EXECUTABLE=" + shutil.which("patch"),
        ])
        self.assertIn("lsdiff is required", output)
        self.assertIn("Install patchutils", output)
        self.assertFalse((self.source / "value").exists())

    def test_lsdiff_failure_or_empty_output_does_not_apply_patches(self):
        for exit_code, message in [(1, "Cannot list patch targets"),
                                   (0, "No text patch targets found")]:
            with self.subTest(exit_code=exit_code):
                self.create_project()
                wrapper = self.project / "lsdiff"
                wrapper.write_text("#!/bin/sh\nexit {}\n".format(exit_code))
                wrapper.chmod(0o755)
                self.configure(options=["-DCMAKE_PROGRAM_PATH=" + str(self.project)])
                self.assertIn(message, self.compile(False))
                self.assertEqual((self.source / "value").read_text(), "alpha\n")
                self.assertFalse((self.install / "built-value").exists())

    def test_local_directory_urls_are_rejected(self):
        for prefix in ["", "file://"]:
            with self.subTest(prefix=prefix):
                self.create_project()
                cmake = self.project / "CMakeLists.txt"
                cmake.write_text(cmake.read_text().replace(
                    '${CMAKE_CURRENT_SOURCE_DIR}/dep.tar.gz', prefix + str(self.package)))
                self.write_versions(checksum="0" * 64)
                output = self.configure(False)
                self.assertIn("Local source directories are not supported", output)
                self.assertFalse((self.source / "value").exists())
                self.assertFalse((self.stamp / "dep-download").exists())
                self.assertEqual((self.package / "value").read_text(), "alpha\n")

    def test_local_archive_replaced_by_directory_is_rejected(self):
        self.configure()
        self.archive.unlink()
        shutil.copytree(str(self.package), str(self.archive))
        output = self.compile(False)
        self.assertIn("Local source directories are not supported", output)
        self.assertFalse((self.source / "value").exists())

    def test_local_archive_urls_still_require_matching_checksum(self):
        for prefix in ["", "file://"]:
            for valid in [True, False]:
                with self.subTest(prefix=prefix, valid_checksum=valid):
                    self.create_project()
                    cmake = self.project / "CMakeLists.txt"
                    cmake.write_text(cmake.read_text().replace(
                        '${CMAKE_CURRENT_SOURCE_DIR}/dep.tar.gz', prefix + str(self.archive)))
                    if not valid:
                        self.write_versions(checksum="0" * 64)
                    self.configure()
                    if valid:
                        self.compile()
                        self.assertEqual((self.install / "built-value").read_text(), "gamma\n")
                    else:
                        self.compile(False)
                        self.assertFalse((self.source / "value").exists())
                        self.assertFalse((self.install / "built-value").exists())

    def test_clean_first_stops_before_source_is_overwritten(self):
        self.built()
        output = self.command([ARGS.cmake, "--build", str(self.build), "--target", "dep",
                               "--clean-first"], False)
        self.assertIn("Download completion record is missing", output)
        self.assert_cleanup(output)
        self.assertEqual((self.source / "value").read_text(), "gamma\n")
        self.assertEqual((self.project / "events").read_text(), "build\ninstall\n")
        self.clean_reported_paths(output)
        self.built()

    def test_same_archive_at_new_url_requires_cleanup(self):
        self.built()
        mirror = self.project / "mirror.tar.gz"
        shutil.copyfile(str(self.archive), str(mirror))
        cmake = self.project / "CMakeLists.txt"
        cmake.write_text(cmake.read_text().replace("dep.tar.gz", "mirror.tar.gz"))
        output = self.configure(False)
        self.assert_cleanup(output)
        self.assertEqual((self.source / "value").read_text(), "gamma\n")
        self.assert_reuse_rejected()
        self.clean_reported_paths(output)
        self.built()

    def test_truncated_postimage_is_rejected(self):
        (self.project / "two.diff").write_text(
            patch("beta", "gamma") + patch("unchanged", "updated", "other"))
        self.built()
        record = self.build / "paimon-thirdparty/dep/postimage"
        original = record.read_text()
        for replacement in [original.splitlines()[0] + "\n", ""]:
            with self.subTest(postimage=replacement):
                record.write_text(replacement)
                (self.source / "other").write_text("corrupted\n")
                try:
                    self.assert_reuse_rejected()
                finally:
                    record.write_text(original)
                    (self.source / "other").write_text("updated\n")
        self.compile()

    def test_patch_list_changes_require_cleanup(self):
        self.built()
        (self.project / "three.diff").write_text(patch("gamma", "delta"))
        for patches in ["two.diff one.diff", "", "one.diff two.diff three.diff"]:
            with self.subTest(patches=patches):
                self.write_project(patches=patches)
                self.assert_cleanup(self.configure(False))
                self.assertEqual((self.install / "built-value").read_text(), "gamma\n")

    def test_source_and_build_input_changes_require_cleanup(self):
        cases = {
            "version": lambda: self.write_versions(version="2"),
            "checksum": lambda: self.write_versions(checksum="0" * 64),
            "dependency": lambda: self.write_versions(dependency="2"),
            "additional input": lambda: (self.project / "input.rs").write_text("changed\n"),
            "missing patch": lambda: (self.project / "one.diff").unlink(),
            "build type": lambda: self.write_project(layout='set(CMAKE_BUILD_TYPE Release)'),
        }
        for name, change in cases.items():
            with self.subTest(change=name):
                self.create_project()
                self.built()
                change()
                self.assert_reuse_rejected()

    def test_failed_second_patch_blocks_retry(self):
        (self.project / "two.diff").write_text(patch("wrong", "gamma"))
        self.configure()
        self.assert_cleanup(self.compile(False))
        self.assertEqual((self.source / "value").read_text(), "beta\n")
        self.command([ARGS.cmake, "--install", str(self.build)], False)
        output = self.compile(False)
        self.assertIn("interrupted", output)
        self.assertFalse((self.install / "built-value").exists())

    def test_interrupted_patch_blocks_retry(self):
        real_patch = shutil.which("patch")
        wrapper = self.project / "patch"
        wrapper.write_text('''#!/usr/bin/env python3
import os, signal, subprocess, sys
result = subprocess.call([@PATCH@] + sys.argv[1:])
if result == 0 and '--dry-run' not in sys.argv:
    os.kill(os.getppid(), signal.SIGKILL)
sys.exit(result)
'''.replace("@PATCH@", repr(real_patch)))
        wrapper.chmod(0o755)
        self.configure(options=["-DCMAKE_PROGRAM_PATH=" + str(self.project)])
        self.compile(False)
        self.assertEqual((self.source / "value").read_text(), "beta\n")
        self.assertIn("interrupted", self.compile(False))
        self.command([ARGS.cmake, "--install", str(self.build)], False)

    def test_build_and_install_failures_can_resume(self):
        for phase in ["build", "install"]:
            with self.subTest(phase=phase):
                self.create_project()
                failure = self.project / ("fail-" + phase)
                failure.touch()
                self.configure()
                self.compile(False)
                self.command([ARGS.cmake, "--install", str(self.build)], False)
                self.assertFalse((self.install / "built-value").exists())
                failure.unlink()
                self.compile()
                self.command([ARGS.cmake, "--install", str(self.build)])
                self.assertEqual((self.install / "built-value").read_text(), "gamma\n")

    def test_source_damage_cannot_bypass_guard(self):
        self.write_project(extra="externalproject_add_steptargets(dep build)")
        self.built()
        source = self.source / "value"
        for content in ["alpha\n", None]:
            with self.subTest(content=content):
                if content is None:
                    source.unlink()
                else:
                    source.write_text(content)
                try:
                    self.assert_reuse_rejected(targets=("dep", "dep-build"))
                finally:
                    source.write_text("gamma\n")
        self.compile()

    def test_install_completion_does_not_check_individual_outputs(self):
        self.built()
        (self.install / "built-value").unlink()
        # A regular build tool can rebuild declared byproducts. The patch guard
        # itself accepts a completed identity without inspecting installed files.
        self.configure()
        self.command([ARGS.cmake, "--install", str(self.build)])
        self.assertEqual((self.project / "events").read_text(), "build\ninstall\n")
        self.assertFalse((self.install / "built-value").exists())

    def test_damaged_completion_state_is_not_success(self):
        self.built()
        # Fault injection deliberately knows the state filenames. Verify the
        # public build/install behavior, not the contents of a valid marker.
        state = self.build / "paimon-thirdparty/dep"
        for name, replacement in [("installed", None), ("installed", "outdated identity"),
                                  ("patched", None)]:
            with self.subTest(marker=name, replacement=replacement):
                marker = state / name
                original = marker.read_bytes()
                if replacement is None:
                    marker.unlink()
                else:
                    marker.write_text(replacement)
                try:
                    self.assert_reuse_rejected()
                finally:
                    marker.write_bytes(original)

    def test_legacy_artifacts_are_not_adopted_or_deleted(self):
        for kind in ["source", "install", "stamp"]:
            with self.subTest(kind=kind):
                self.create_project()
                artifact = {"source": self.source / "value",
                            "install": self.install / "obsolete.h",
                            "stamp": self.stamp / "dep-install"}[kind]
                artifact.parent.mkdir(parents=True)
                artifact.write_text("old data\n")
                if kind == "source":
                    (self.source / ".patched").touch()
                self.assert_cleanup(self.configure(False))
                self.assertEqual(artifact.read_text(), "old data\n")

    def test_file_addition_and_deletion(self):
        (self.project / "two.diff").write_text('''diff --git a/other b/other
--- a/other
+++ /dev/null
@@ -1 +0,0 @@
-unchanged
diff --git a/added b/added
--- /dev/null
+++ b/added
@@ -0,0 +1 @@
+created
''')
        self.configure()
        self.compile()
        self.compile()
        self.assertFalse((self.source / "other").exists())
        self.assertEqual((self.source / "added").read_text(), "created\n")
        (self.source / "other").write_text("old\n")
        self.assertIn("Deleted patch target reappeared", self.compile(False))

    def test_layout_migration_reports_old_paths(self):
        layouts = {
            "orc": '''set(dest "${CMAKE_CURRENT_BINARY_DIR}/dep-prefix")
set(extra_options SOURCE_DIR "${dest}/cpp" BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/build/dep"
                  UPDATE_DISCONNECTED TRUE)''',
            "cargo": '''set(extra_options BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/dep-cargo")''',
        }
        for name, layout in layouts.items():
            with self.subTest(layout=name):
                self.create_project()
                self.write_project(layout=layout)
                if name == "orc":
                    self.install = self.build / "dep-prefix"
                    self.source = self.install / "cpp"
                old_binary = self.build / ("build/dep" if name == "orc" else "dep-cargo")
                self.built()
                old_source, old_install = self.source, self.install
                self.write_project()
                self.source = self.build / "dep-prefix/src/dep"
                self.install = self.build / "dep-install"
                # Repeated failed configuration must retain the original advice.
                for _ in range(2):
                    output = self.configure(False)
                    self.assert_cleanup(output)
                    for path in [old_source, old_install, old_binary]:
                        self.assertIn(str(path), output)
                self.assertEqual((old_install / "built-value").read_text(), "gamma\n")
                self.clean_reported_paths(output)
                self.built()

    def test_version_file_is_relative_to_module_in_subproject(self):
        parent = self.root / "parent"
        parent.mkdir()
        (parent / "third_party").mkdir()
        (parent / "third_party/versions.txt").write_text("unrelated parent versions\n")
        (parent / "CMakeLists.txt").write_text('''cmake_minimum_required(VERSION 3.22)
project(Parent NONE)
add_subdirectory("{}" dependency)
'''.format(self.project))
        self.command([ARGS.cmake, "-S", str(parent), "-B", str(self.build),
                      "-G", ARGS.generator])
        self.compile()
        self.install = self.build / "dependency/dep-install"
        self.source = self.build / "dependency/dep-prefix/src/dep"
        self.stamp = self.build / "dependency/dep-prefix/src/dep-stamp"
        self.assertEqual((self.install / "built-value").read_text(), "gamma\n")
        self.write_versions(version="2")
        self.assert_cleanup(self.compile(False))

    def test_vortex_build_uses_standard_directory_and_source_working_directory(self):
        # Exercise the production adapter with Cargo simulated, without requiring
        # a Rust toolchain or building the Vortex workspace in this offline suite.
        toolchain = (MODULES / "ThirdpartyToolchain.cmake").read_text()
        macro = "macro(build_vortex)" + toolchain.split("macro(build_vortex)", 1)[1]
        macro = macro.split("endmacro()", 1)[0] + "endmacro()\n"
        (self.project / "vortex-adapter.cmake").write_text(macro)
        (self.package / "vortex-ffi/cinclude").mkdir(parents=True)
        (self.package / "vortex-ffi/src").mkdir()
        (self.package / "vortex-ffi/cinclude/vortex.h").write_text("header\n")
        (self.package / ".cargo").mkdir()
        (self.package / ".cargo/config.toml").write_text("# workspace config\n")
        self.make_archive()
        (self.project / "third_party/versions.txt").write_text(
            "PAIMON_VORTEX_BUILD_VERSION=1\nPAIMON_VORTEX_BUILD_SHA256_CHECKSUM={}\n".format(self.sha))
        shutil.copyfile(str(self.project / "one.diff"), str(self.project / "vortex.diff"))
        callback_dir = self.project / "crates/vortex_callback_io"
        callback_dir.mkdir(parents=True)
        (callback_dir / "callback_io.rs").write_text("callback\n")
        cargo = self.project / "cargo"
        cargo.write_text('''#!/usr/bin/env python3
import os
from pathlib import Path
assert Path('.cargo/config.toml').is_file()
assert Path('value').read_text() == 'beta\\n'
assert Path('vortex-ffi/src/callback_io.rs').read_text() == 'callback\\n'
target = Path(os.environ['CARGO_TARGET_DIR'])
(target / 'release').mkdir(parents=True, exist_ok=True)
(target / 'release/libvortex_ffi.a').write_text('simulated library\\n')
(target / 'working-directory').write_text(str(Path.cwd()))
''')
        cargo.chmod(0o755)
        (self.project / "CMakeLists.txt").write_text('''cmake_minimum_required(VERSION 3.22)
project(VortexAdapter NONE)
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules/ThirdpartyPatch.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/vortex-adapter.cmake")
set(PAIMON_CARGO_EXECUTABLE "${CMAKE_CURRENT_SOURCE_DIR}/cargo")
set(VORTEX_FLATC "${CMAKE_COMMAND}")
set(PAIMON_VORTEX_BUILD_VERSION 1)
file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/dep.tar.gz" PAIMON_VORTEX_BUILD_SHA256_CHECKSUM)
set(VORTEX_SOURCE_URL "${CMAKE_CURRENT_SOURCE_DIR}/dep.tar.gz")
set(CMAKE_STATIC_LIBRARY_PREFIX lib)
set(CMAKE_STATIC_LIBRARY_SUFFIX .a)
set(CMAKE_INSTALL_LIBDIR lib)
add_library(Threads::Threads INTERFACE IMPORTED)
build_vortex()
''')
        self.configure()
        self.compile(target="vortex_ep")
        binary = self.build / "vortex_ep-prefix/src/vortex_ep-build"
        source = self.build / "vortex_ep-prefix/src/vortex_ep"
        self.assertEqual((binary / "working-directory").read_text(), str(source))
        installed = self.build / "vortex_ep-install/lib/libvortex_ffi.a"
        self.assertEqual(installed.read_text(), "simulated library\n")
        timestamp = installed.stat().st_mtime_ns
        self.compile(target="vortex_ep")
        self.assertEqual(installed.stat().st_mtime_ns, timestamp)
        self.assertFalse((self.build / "vortex_ep-cargo").exists())

    def test_clean_rebuild_after_version_change(self):
        self.built()
        self.write_versions(version="2")
        output = self.configure(False)
        self.assert_cleanup(output)
        self.clean_reported_paths(output)
        self.built()

    def test_empty_patch_list(self):
        self.write_project(patches="")
        self.configure()
        self.compile()
        self.compile()
        self.assertEqual((self.install / "built-value").read_text(), "alpha\n")

    def test_escaping_patch_path(self):
        for name in ["../outside", "file[", "semi;colon", "back\\slash"]:
            with self.subTest(path=name):
                self.create_project()
                (self.project / "one.diff").write_text(patch("alpha", "beta", name))
                self.configure()
                self.assertIn("Unsafe patch path", self.compile(False))


if __name__ == "__main__":
    unittest.main(argv=[__file__] + UNITTEST_ARGS, verbosity=2)
