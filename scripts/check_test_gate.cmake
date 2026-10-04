# scripts/check_test_gate.cmake
#
# Verifies that the unit-test gate (cmake/McoTesting.cmake) fails closed.
#
#   cmake -P scripts/check_test_gate.cmake
#
# The standalone fixture in test/gate-check is configured once per scenario and the
# observed outcome is asserted against what the gate promises:
#
#   missing-dep   Google Test unavailable, BUILD_TESTING=ON -> configure MUST die on the
#                 "Google Test was not found" message
#   zero-cases    Google Test available, nothing registered -> configure MUST die on the
#                 "no test case was registered" message
#   registered    Google Test available, 1 case registered  -> configure MUST finish and
#                 ctest MUST see exactly 1 case
#   build-only    BUILD_TESTING=OFF                         -> configure MUST finish, print
#                 the build-only marker and register 0 cases
#
# A failed scenario has to die with *its own* message: the assertions look for the
# wording inside the "CMake Error" block and also require that a failing configure
# never reaches "Configuring done". Without those two, a gate that merely failed
# later (or downgraded its error to a warning) would still look like a pass.
#
# Written as a CMake script instead of PowerShell so it behaves identically on Linux,
# macOS and Windows, needs nothing but CMake, and can be re-run by anyone. CI runs it
# in the test-gate job of .github/workflows/ci.yml.

string(RANDOM LENGTH 6 _mco_gate_suffix)
set(_mco_gate_tmp "$ENV{TMPDIR}")
if(NOT _mco_gate_tmp)
    set(_mco_gate_tmp "$ENV{TEMP}")
endif()
if(NOT _mco_gate_tmp)
    set(_mco_gate_tmp "/tmp")
endif()
set(_mco_gate_work "${_mco_gate_tmp}/mco-test-gate-${_mco_gate_suffix}")
file(MAKE_DIRECTORY "${_mco_gate_work}")

get_filename_component(_mco_gate_repo "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
set(_mco_gate_fixture "${_mco_gate_repo}/test/gate-check")
if(NOT IS_DIRECTORY "${_mco_gate_fixture}")
    message(FATAL_ERROR "MCO-GATE-HARNESS: fixture directory not found: ${_mco_gate_fixture}")
endif()

find_program(_mco_gate_cmake NAMES cmake REQUIRED)
find_program(_mco_gate_ctest NAMES ctest REQUIRED)

set(_mco_gate_failures "")

# mco_gate_check(<scenario> <BUILD_TESTING> <PASS|FAIL> <expected regex> <expected ctest cases or "-">)
function(mco_gate_check _scenario _build_testing _expect _marker _expected_cases)
    set(_build_dir "${_mco_gate_work}/${_scenario}")
    file(MAKE_DIRECTORY "${_build_dir}")

    execute_process(
        COMMAND ${_mco_gate_cmake} -S "${_mco_gate_fixture}" -B "${_build_dir}"
                "-DMCO_GATE_SCENARIO=${_scenario}" "-DBUILD_TESTING=${_build_testing}"
        OUTPUT_VARIABLE _out ERROR_VARIABLE _err RESULT_VARIABLE _rc)
    string(APPEND _out "${_err}")

    set(_problems "")
    if(_expect STREQUAL "FAIL")
        if(_rc EQUAL 0)
            string(APPEND _problems "configure exited 0 although the gate must reject it;")
        endif()
        if(_out MATCHES "Configuring done")
            string(APPEND _problems "configure completed although it must have been rejected;")
        endif()
        # The rejection has to come from this specific check, as a fatal error.
        if(NOT _out MATCHES "CMake Error[^\n]*\n[ ]+${_marker}")
            string(APPEND _problems "no FATAL_ERROR carrying '${_marker}' was raised;")
        endif()
    else()
        if(NOT _rc EQUAL 0)
            string(APPEND _problems "configure failed with exit=${_rc} although it must succeed;")
        endif()
        if(NOT _out MATCHES "Configuring done")
            string(APPEND _problems "configure did not complete;")
        endif()
        if(NOT _out MATCHES "${_marker}")
            string(APPEND _problems "configure log is missing '${_marker}';")
        endif()
    endif()

    set(_seen "-")
    if(NOT _expected_cases STREQUAL "-")
        execute_process(
            COMMAND ${_mco_gate_ctest} --test-dir "${_build_dir}" -N
            OUTPUT_VARIABLE _list ERROR_VARIABLE _list_err RESULT_VARIABLE _list_rc)
        string(APPEND _list "${_list_err}")
        if(_list MATCHES "Total Tests: +([0-9]+)")
            set(_seen "${CMAKE_MATCH_1}")
        else()
            string(APPEND _problems "'ctest -N' reported no test count (exit=${_list_rc});")
        endif()
        if(NOT _seen STREQUAL _expected_cases)
            string(APPEND _problems "ctest saw '${_seen}' case(s), expected '${_expected_cases}';")
        endif()
    endif()

    if(_problems)
        message(STATUS "[gate] ${_scenario}: FAILED (${_problems})")
        message(STATUS "---- configure output ----\n${_out}\n------------------------")
        string(APPEND _mco_gate_failures " ${_scenario}")
    else()
        message(STATUS "[gate] ${_scenario}: OK (ctest cases: ${_seen})")
    endif()
    set(_mco_gate_failures "${_mco_gate_failures}" PARENT_SCOPE)
endfunction()

mco_gate_check(missing-dep ON FAIL
        "MCO-BUILD-GATE: BUILD_TESTING=ON but Google Test" "-")
mco_gate_check(zero-cases ON FAIL
        "MCO-BUILD-GATE: BUILD_TESTING=ON but no test case was registered" "-")
mco_gate_check(registered ON PASS
        "BUILD_TESTING=ON -> 1 ctest case\\(s\\) registered: gate_probe" "1")
mco_gate_check(build-only OFF PASS
        "BUILD_TESTING=OFF -> driver binaries only" "0")

if(_mco_gate_failures)
    message(STATUS
        "MCO-GATE-HARNESS: configure directories kept for inspection under ${_mco_gate_work}")
    message(FATAL_ERROR
        "MCO-GATE-HARNESS: the unit-test gate did not behave as specified in:${_mco_gate_failures}")
endif()

# Only a passing run cleans up; a failing one leaves the configure logs behind.
file(REMOVE_RECURSE "${_mco_gate_work}")

message(STATUS "MCO-GATE-HARNESS: all 4 scenarios behave as specified")
