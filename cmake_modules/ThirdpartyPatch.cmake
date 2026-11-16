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

include_guard(GLOBAL)
include(ExternalProject)

# Call before ExternalProject_Add, then register the project after creating it.
function(paimon_patch_command name output)
    set(${output}
        "${CMAKE_COMMAND}"
        "-DPAIMON_PATCH_MODE=apply"
        "-DPAIMON_PATCH_CONFIG=${CMAKE_CURRENT_BINARY_DIR}/paimon-thirdparty/${name}/request.cmake"
        -P
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/ApplyThirdpartyPatch.cmake"
        PARENT_SCOPE)
endfunction()

# Read the previous generated request in a separate scope. Directory migrations
# must report old caches as well as the newly configured paths on failure.
function(paimon_patch_previous_cleanup config output)
    set(PP_CLEAN "")
    if(EXISTS "${config}")
        include("${config}")
    endif()
    set(${output}
        "${PP_CLEAN}"
        PARENT_SCOPE)
endfunction()

# Only SHA256-verified archives in build-owned directories are supported. PATCHES
# is ordered; an empty list still registers source/build identity validation.
function(paimon_register_patched_project name)
    cmake_parse_arguments(P
                          ""
                          "VERSION;INSTALL_DIR"
                          "PATCHES;INPUTS;VERSION_VARIABLES"
                          ${ARGN})
    if(P_UNPARSED_ARGUMENTS
       OR NOT P_VERSION
       OR NOT P_INSTALL_DIR)
        message(FATAL_ERROR "Invalid patch registration for ${name}")
    endif()
    if(CMAKE_CONFIGURATION_TYPES)
        message(FATAL_ERROR "${name}: patch state validation requires a single-configuration generator"
        )
    endif()
    externalproject_get_property(${name}
                                 SOURCE_DIR
                                 BINARY_DIR
                                 STAMP_DIR
                                 TMP_DIR)
    get_property(archive_hash
                 TARGET ${name}
                 PROPERTY _EP_URL_HASH)
    if(NOT archive_hash MATCHES "^SHA256=[0-9a-fA-F]+$")
        message(FATAL_ERROR "${name}: patched dependencies require a SHA256-verified archive"
        )
    endif()
    string(LENGTH "${archive_hash}" hash_length)
    if(NOT hash_length EQUAL 71)
        message(FATAL_ERROR "${name}: invalid archive SHA256")
    endif()

    # Validate ownership even though this module never deletes directories.
    # In particular, do not generate cleanup advice for a user checkout.
    set(PP_ROOT "${CMAKE_BINARY_DIR}")
    set(PP_NAME "${name}")
    set(PP_SOURCE "${SOURCE_DIR}")
    set(PP_BINARY "${BINARY_DIR}")
    set(PP_STAMP "${STAMP_DIR}")
    set(PP_TMP "${TMP_DIR}")
    set(PP_INSTALL "${P_INSTALL_DIR}")
    set(PP_STATE "${CMAKE_CURRENT_BINARY_DIR}/paimon-thirdparty/${name}")
    set(PP_COMPLETE "${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/${name}-complete")
    paimon_patch_previous_cleanup("${PP_STATE}/request.cmake" previous_cleanup)
    set(PP_CLEAN
        "${SOURCE_DIR};${BINARY_DIR};${STAMP_DIR};${TMP_DIR};${P_INSTALL_DIR};${PP_COMPLETE};${PP_STATE};${previous_cleanup}"
    )
    list(FILTER
         PP_CLEAN
         EXCLUDE
         REGEX
         "^$")
    list(REMOVE_DUPLICATES PP_CLEAN)
    file(REAL_PATH "${PP_ROOT}" root_real)
    foreach(path IN LISTS PP_CLEAN)
        file(REAL_PATH "${path}" path_real)
        cmake_path(IS_PREFIX
                   root_real
                   "${path_real}"
                   NORMALIZE
                   owned)
        if(NOT owned OR path_real STREQUAL root_real)
            message(FATAL_ERROR "${name}: ${path} is not a build-owned directory. "
                                "Local source overrides are not supported by the patch mechanism."
            )
        endif()
    endforeach()

    set(PP_VERSION "${P_VERSION}")
    string(TOLOWER "${archive_hash}" PP_ARCHIVE)
    set(PP_PATCHES "${P_PATCHES}")
    set(PP_INPUTS "${P_INPUTS}")
    # Resolve relative to this module, including when Paimon is a subproject.
    get_filename_component(PP_VERSIONS_FILE
                           "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../third_party/versions.txt"
                           ABSOLUTE)
    set(PP_VERSION_VARIABLES "${P_VERSION_VARIABLES}")
    set(PP_CONTEXT
        "${CMAKE_GENERATOR}|${CMAKE_BUILD_TYPE}|${CMAKE_C_COMPILER}|${CMAKE_CXX_COMPILER}"
    )
    foreach(property CMAKE_ARGS CONFIGURE_COMMAND BUILD_COMMAND INSTALL_COMMAND)
        get_property(value
                     TARGET ${name}
                     PROPERTY _EP_${property})
        string(APPEND PP_CONTEXT "\n${property}=${value}")
    endforeach()
    set(PP_RUNNER "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/ApplyThirdpartyPatch.cmake")
    find_program(PP_PATCH_EXECUTABLE patch REQUIRED)

    set_property(DIRECTORY
                 APPEND
                 PROPERTY CMAKE_CONFIGURE_DEPENDS
                          ${PP_PATCHES}
                          ${PP_INPUTS}
                          ${PP_VERSIONS_FILE}
                          "${PP_RUNNER}")
    # Including the runner only defines helpers; script mode executes them.
    include("${PP_RUNNER}")
    paimon_patch_fingerprint(PP_EXPECTED)
    file(MAKE_DIRECTORY "${PP_STATE}")
    set(request "")
    foreach(variable
            ROOT
            NAME
            SOURCE
            BINARY
            STAMP
            TMP
            INSTALL
            STATE
            COMPLETE
            CLEAN
            VERSION
            ARCHIVE
            PATCHES
            INPUTS
            VERSIONS_FILE
            VERSION_VARIABLES
            CONTEXT
            PATCH_EXECUTABLE
            EXPECTED)
        # Bracket arguments preserve spaces, semicolons, dollar signs and backslashes.
        set(delimiter "=")
        while("${PP_${variable}}" MATCHES "]${delimiter}]")
            string(APPEND delimiter "=")
        endwhile()
        string(APPEND request
               "set(PP_${variable} [${delimiter}[${PP_${variable}}]${delimiter}])\n")
    endforeach()
    # Do not update timestamps on ordinary reconfiguration.
    set(config "${PP_STATE}/request.cmake")
    file(WRITE "${config}.tmp" "${request}")
    configure_file("${config}.tmp" "${config}" COPYONLY)
    file(REMOVE "${config}.tmp")
    set(command "${CMAKE_COMMAND}" "-DPAIMON_PATCH_CONFIG=${config}" -P "${PP_RUNNER}")
    execute_process(COMMAND ${command} RESULT_VARIABLE result)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "${name}: third-party patch state validation failed")
    endif()

    add_custom_target(${name}-patch-guard
                      COMMAND ${command}
                      VERBATIM)
    # A target dependency orders validation without making the download stamp
    # perpetually out of date, unlike an ALWAYS step in the EP step chain.
    externalproject_add_stepdependencies(${name} download ${name}-patch-guard)
    externalproject_add_step(${name} paimon-verify
                             COMMAND "${CMAKE_COMMAND}" "-DPAIMON_PATCH_MODE=complete"
                                     "-DPAIMON_PATCH_CONFIG=${config}" -P "${PP_RUNNER}"
                             DEPENDEES install) # codespell:ignore dependees
    # cmake --install does not build first. Refuse to export stale artifacts.
    install(CODE "execute_process(COMMAND \"${CMAKE_COMMAND}\" \"-DPAIMON_PATCH_MODE=consume\" \"-DPAIMON_PATCH_CONFIG=${config}\" -P \"${PP_RUNNER}\" RESULT_VARIABLE patch_result)
if(NOT patch_result EQUAL 0)
    message(FATAL_ERROR \"${name}: build the dependency successfully before installing Paimon\")
endif()")
endfunction()
