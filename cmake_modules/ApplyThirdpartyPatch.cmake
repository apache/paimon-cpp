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

function(paimon_patch_error)
    string(CONCAT reason ${ARGV})
    set(cleanup "")
    foreach(path IN LISTS PP_CLEAN)
        string(APPEND cleanup "\n  ${path}")
    endforeach()
    message(FATAL_ERROR "${PP_NAME}: ${reason}\n"
                        "Do not remove only .patched or a patch stamp. Remove these "
                        "build-owned paths, then re-run CMake and build:${cleanup}\n"
                        "Verified source archives may be retained. No files were deleted automatically."
    )
endfunction()

function(paimon_patch_fingerprint output)
    foreach(url IN LISTS PP_URLS)
        if(IS_DIRECTORY "${url}")
            paimon_patch_error("Local source directories are not supported; use a SHA256-verified archive: ${url}"
            )
        endif()
    endforeach()
    # The same ordered list is used here and by the application loop below.
    set(identity
        "paimon-patch-v1\n${PP_NAME}\n${PP_VERSION}\n${PP_ARCHIVE}\n${PP_URLS}\n${PP_SOURCE}\n${PP_BINARY}\n${PP_INSTALL}\n${PP_CONTEXT}\n"
    )
    file(SHA256 "${CMAKE_CURRENT_FUNCTION_LIST_FILE}" runner_hash)
    string(APPEND identity
           "runner=${runner_hash}\npatch-options=-p1,--batch,--forward,--fuzz=0\n")
    set(index 0)
    foreach(input IN LISTS PP_PATCHES PP_INPUTS)
        if(NOT EXISTS "${input}")
            paimon_patch_error("Required patch/build input is missing: ${input}")
        endif()
        file(SHA256 "${input}" hash)
        get_filename_component(name "${input}" NAME)
        string(APPEND identity "${index}:${name}:${hash}\n")
        math(EXPR index "${index} + 1")
    endforeach()
    list(LENGTH PP_PATCHES count)
    string(APPEND identity "patch-count=${count}\n")
    if(PP_VERSIONS_FILE)
        if(NOT EXISTS "${PP_VERSIONS_FILE}")
            paimon_patch_error("Version file is missing: ${PP_VERSIONS_FILE}")
        endif()
        foreach(variable IN LISTS PP_VERSION_VARIABLES)
            file(STRINGS "${PP_VERSIONS_FILE}" entries REGEX "^${variable}=")
            list(LENGTH entries count)
            if(NOT count EQUAL 1)
                paimon_patch_error("Expected one ${variable} entry in ${PP_VERSIONS_FILE}"
                )
            endif()
            string(APPEND identity "${entries}\n")
        endforeach()
    endif()
    string(SHA256 fingerprint "${identity}")
    set(${output}
        "${fingerprint}"
        PARENT_SCOPE)
endfunction()

function(paimon_patch_write name content)
    file(WRITE "${PP_STATE}/${name}.tmp" "${content}")
    file(RENAME "${PP_STATE}/${name}.tmp" "${PP_STATE}/${name}")
endfunction()

function(paimon_patch_check_paths)
    file(REAL_PATH "${PP_ROOT}" root_real)
    foreach(path IN LISTS PP_CLEAN)
        file(REAL_PATH "${path}" path_real)
        cmake_path(IS_PREFIX
                   root_real
                   "${path_real}"
                   NORMALIZE
                   owned)
        if(NOT owned OR path_real STREQUAL root_real)
            # Do not print removal advice for a path redirected outside the build.
            message(FATAL_ERROR "${PP_NAME}: ${path} resolves outside its build-owned location. "
                                "Restore the directory layout; do not delete the external source."
            )
        endif()
    endforeach()
endfunction()

function(paimon_patch_assert_empty directory)
    file(GLOB_RECURSE contents
         LIST_DIRECTORIES FALSE
         "${directory}/*")
    foreach(path IN LISTS contents)
        cmake_path(IS_PREFIX
                   PP_STAMP
                   "${path}"
                   NORMALIZE
                   in_stamp)
        cmake_path(IS_PREFIX
                   PP_TMP
                   "${path}"
                   NORMALIZE
                   in_tmp)
        if(NOT in_stamp AND NOT in_tmp)
            paimon_patch_error("Unrecognized existing source/build/install files in ${directory}"
            )
        endif()
    endforeach()
endfunction()

function(paimon_patch_check_postimage)
    if(NOT EXISTS "${PP_SOURCE}/.paimon-source-id" OR NOT EXISTS "${PP_STATE}/postimage")
        paimon_patch_error("Patched source or its verification record is missing")
    endif()
    file(READ "${PP_SOURCE}/.paimon-source-id" source_id)
    if(NOT source_id STREQUAL PP_EXPECTED)
        paimon_patch_error("Source identity does not match its build state")
    endif()
    file(SHA256 "${PP_STATE}/postimage" postimage_hash)
    file(READ "${PP_STATE}/patched" patched)
    if(NOT patched STREQUAL "${PP_EXPECTED}\n${postimage_hash}\n")
        paimon_patch_error("Patch completion or source verification record is inconsistent"
        )
    endif()
    file(STRINGS "${PP_STATE}/postimage" entries)
    foreach(entry IN LISTS entries)
        if(NOT entry MATCHES "^([^ ]+) (.+)$")
            paimon_patch_error("Invalid source verification record")
        endif()
        set(expected "${CMAKE_MATCH_1}")
        set(relative "${CMAKE_MATCH_2}")
        set(path "${PP_SOURCE}/${relative}")
        if(expected STREQUAL "absent")
            if(EXISTS "${path}" OR IS_SYMLINK "${path}")
                paimon_patch_error("Deleted patch target reappeared: ${path}")
            endif()
        else()
            if(NOT EXISTS "${path}"
               OR IS_DIRECTORY "${path}"
               OR IS_SYMLINK "${path}")
                paimon_patch_error("Patch target is missing or replaced: ${path}")
            endif()
            file(SHA256 "${path}" actual)
            if(NOT actual STREQUAL expected)
                paimon_patch_error("Patched source was modified: ${path}")
            endif()
        endif()
    endforeach()
endfunction()

function(paimon_patch_guard)
    paimon_patch_check_paths()
    paimon_patch_fingerprint(actual)
    if(NOT actual STREQUAL PP_EXPECTED)
        paimon_patch_error("Inputs changed since configuration; re-run CMake. Configured "
                           "identity ${PP_EXPECTED}, current identity ${actual}")
    endif()
    if(EXISTS "${PP_STATE}/identity")
        file(READ "${PP_STATE}/identity" previous)
        if(NOT previous STREQUAL PP_EXPECTED)
            paimon_patch_error("Source version, patches/order, or build inputs changed. "
                               "Previous identity ${previous}, requested identity ${PP_EXPECTED}"
            )
        endif()
    else()
        # Generated EP helper scripts and empty include directories are harmless;
        # an old source, install, compilation cache or success stamp is not.
        paimon_patch_assert_empty("${PP_SOURCE}")
        paimon_patch_assert_empty("${PP_BINARY}")
        paimon_patch_assert_empty("${PP_INSTALL}")
        foreach(step
                download
                update
                update_disconnected
                patch
                patch_disconnected
                configure
                build
                install
                done
                paimon-verify)
            if(EXISTS "${PP_STAMP}/${PP_NAME}-${step}")
                paimon_patch_error("Legacy ExternalProject stamp has no verified identity: ${step}"
                )
            endif()
        endforeach()
        if(EXISTS "${PP_COMPLETE}")
            paimon_patch_error("Legacy ExternalProject completion record has no verified identity"
            )
        endif()
        paimon_patch_write(identity "${PP_EXPECTED}")
    endif()
    if(EXISTS "${PP_STATE}/applying")
        paimon_patch_error("Patch application failed or was interrupted; the source may be partially patched"
        )
    endif()
    if(EXISTS "${PP_STATE}/patched")
        # Clean targets remove ExternalProject stamps but retain the source and
        # our records. Stop before download can overwrite that patched source.
        if(NOT EXISTS "${PP_STAMP}/${PP_NAME}-download")
            paimon_patch_error("Download completion record is missing; refusing to overwrite patched source"
            )
        endif()
        paimon_patch_check_postimage()
        # Re-extraction of the same archive can preserve timestamps of its files.
        # Never reuse the old binary cache after a repeated download/update step.
        foreach(step download update update_disconnected)
            if(EXISTS "${PP_STAMP}/${PP_NAME}-${step}"
               AND "${PP_STAMP}/${PP_NAME}-${step}" IS_NEWER_THAN "${PP_STATE}/patched")
                paimon_patch_error("Source download/update ran after patch completion")
            endif()
        endforeach()
    elseif(EXISTS "${PP_STAMP}/${PP_NAME}-patch"
           OR EXISTS "${PP_STAMP}/${PP_NAME}-patch_disconnected"
           OR EXISTS "${PP_STATE}/installed")
        paimon_patch_error("A build stamp or install exists without verified patch completion"
        )
    endif()
    if(EXISTS "${PP_STATE}/installed")
        file(READ "${PP_STATE}/installed" installed)
        if(NOT installed STREQUAL PP_EXPECTED)
            paimon_patch_error("Installation belongs to a different source/build identity"
            )
        endif()
    elseif(EXISTS "${PP_COMPLETE}")
        paimon_patch_error("ExternalProject completed without a verified installation")
    endif()
endfunction()

function(paimon_patch_apply)
    if(EXISTS "${PP_STATE}/patched")
        return()
    endif()
    if(NOT EXISTS "${PP_STAMP}/${PP_NAME}-download")
        paimon_patch_error("Cannot patch without a completed, checksum-verified download")
    endif()
    if(EXISTS "${PP_SOURCE}/.patched" OR EXISTS "${PP_SOURCE}/.paimon-vortex-patches")
        paimon_patch_error("Legacy patch marker found in downloaded source")
    endif()
    # Reject unsupported patch paths before modifying anything. This mechanism
    # handles textual -p1 diffs, including additions/deletions, not binary patches,
    # renames, mode changes or paths through symlinks.
    set(targets "")
    foreach(patch IN LISTS PP_PATCHES)
        file(READ "${patch}" contents)
        if(contents MATCHES
           "(^|\n)(GIT binary patch|rename from |rename to |old mode |new mode )")
            paimon_patch_error("Unsupported non-text patch operation in ${patch}")
        endif()
        execute_process(COMMAND "${PP_LSDIFF_EXECUTABLE}" -- "${patch}"
                        RESULT_VARIABLE result
                        OUTPUT_VARIABLE paths
                        ERROR_VARIABLE error)
        if(NOT result STREQUAL "0")
            paimon_patch_error("Cannot list patch targets in ${patch}: ${error}")
        endif()
        if(paths STREQUAL "")
            paimon_patch_error("No text patch targets found in ${patch}")
        endif()
        # Only convert the filename output to a list, never the diff body.
        # Reject filenames that cannot be represented by our verification record.
        if(paths MATCHES "[;\\\t]|\\[|\\]")
            paimon_patch_error("Unsafe patch path in ${patch}: ${paths}")
        endif()
        string(REGEX REPLACE "\n+$" "" paths "${paths}")
        string(REPLACE "\n" ";" paths "${paths}")
        foreach(path IN LISTS paths)
            if(NOT path MATCHES "^[ab]/(.+)$")
                paimon_patch_error("Unsupported patch path in ${patch}: ${path}")
            endif()
            set(relative "${CMAKE_MATCH_1}")
            if(relative MATCHES "(^|/)\\.\\.(/|$)" OR IS_ABSOLUTE "${relative}")
                paimon_patch_error("Unsafe patch path in ${patch}: ${relative}")
            endif()
            file(REAL_PATH "${PP_SOURCE}" source_real)
            file(REAL_PATH "${PP_SOURCE}/${relative}" target_real)
            cmake_path(IS_PREFIX
                       source_real
                       "${target_real}"
                       NORMALIZE
                       contained)
            if(NOT contained OR IS_SYMLINK "${PP_SOURCE}/${relative}")
                paimon_patch_error("Patch target escapes its source tree: ${relative}")
            endif()
            list(APPEND targets "${relative}")
        endforeach()
    endforeach()
    paimon_patch_write(applying "${PP_EXPECTED}")
    foreach(patch IN LISTS PP_PATCHES)
        # Check each patch against the result of its predecessors. A failure
        # leaves 'applying' in place; never guess which hunks succeeded.
        foreach(check --dry-run apply)
            set(options
                --batch
                --forward
                --fuzz=0
                -p1
                -i
                "${patch}")
            if(check STREQUAL "--dry-run")
                list(APPEND options --dry-run)
            endif()
            execute_process(COMMAND "${PP_PATCH_EXECUTABLE}" ${options}
                            WORKING_DIRECTORY "${PP_SOURCE}"
                            RESULT_VARIABLE result
                            OUTPUT_VARIABLE log
                            ERROR_VARIABLE error)
            if(NOT result EQUAL 0)
                paimon_patch_error("Cannot apply ${patch} (${check}): ${log}${error}")
            endif()
        endforeach()
    endforeach()
    list(REMOVE_DUPLICATES targets)
    set(postimage "")
    foreach(relative IN LISTS targets)
        if(EXISTS "${PP_SOURCE}/${relative}")
            file(SHA256 "${PP_SOURCE}/${relative}" hash)
        else()
            set(hash absent)
        endif()
        string(APPEND postimage "${hash} ${relative}\n")
    endforeach()
    paimon_patch_write(postimage "${postimage}")
    file(WRITE "${PP_SOURCE}/.paimon-source-id" "${PP_EXPECTED}")
    string(SHA256 postimage_hash "${postimage}")
    paimon_patch_write(patched "${PP_EXPECTED}\n${postimage_hash}\n")
    # A crash between these operations is conservatively treated as interrupted.
    file(REMOVE "${PP_STATE}/applying")
endfunction()

if(CMAKE_SCRIPT_MODE_FILE STREQUAL CMAKE_CURRENT_LIST_FILE)
    cmake_minimum_required(VERSION 3.22)
    if(NOT PAIMON_PATCH_CONFIG)
        message(FATAL_ERROR "PAIMON_PATCH_CONFIG is required")
    endif()
    include("${PAIMON_PATCH_CONFIG}")
    file(LOCK "${PP_STATE}/lock"
         GUARD PROCESS
         TIMEOUT 0
         RESULT_VARIABLE locked)
    if(NOT locked STREQUAL "0")
        message(FATAL_ERROR "${PP_NAME}: another process is validating or patching this source. Retry after it finishes."
        )
    endif()
    paimon_patch_guard()
    if(PAIMON_PATCH_MODE STREQUAL "apply")
        paimon_patch_apply()
    elseif(PAIMON_PATCH_MODE STREQUAL "complete" OR PAIMON_PATCH_MODE STREQUAL "consume")
        if(NOT EXISTS "${PP_STATE}/patched")
            paimon_patch_error("Cannot use outputs before patch completion")
        endif()
        if(PAIMON_PATCH_MODE STREQUAL "complete")
            # This step runs only after ExternalProject's install command succeeds.
            # Record that success for this identity, without sampling output files.
            paimon_patch_write(installed "${PP_EXPECTED}")
        elseif(NOT EXISTS "${PP_STATE}/installed")
            paimon_patch_error("Build and installation have not completed")
        endif()
    elseif(PAIMON_PATCH_MODE)
        message(FATAL_ERROR "Unknown patch mode: ${PAIMON_PATCH_MODE}")
    endif()
endif()
