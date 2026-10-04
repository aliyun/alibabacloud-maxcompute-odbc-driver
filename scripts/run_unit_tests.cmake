# scripts/run_unit_tests.cmake
#
# Executes the registered ctest cases and refuses to report success for a build that
# contains (almost) no tests.
#
#   cmake -DBUILD_DIR=build -DMIN_TESTS=5 -P scripts/run_unit_tests.cmake
#
#   BUILD_DIR       CMake build directory to test (default: ./build)
#   MIN_TESTS       minimum number of registered cases (default: 5, keep it in sync
#                   with MCO_EXPECTED_UNIT_TESTS in test/CMakeLists.txt)
#
# The case list is printed first, the count is checked, and only then is ctest
# executed with --output-on-failure. A quiet "no tests found" run can therefore never
# turn into a green pipeline step. Used by the build jobs in .github/workflows/ci.yml.

if(NOT DEFINED BUILD_DIR)
    set(BUILD_DIR "${CMAKE_CURRENT_LIST_DIR}/../build")
endif()
get_filename_component(BUILD_DIR "${BUILD_DIR}" ABSOLUTE)
if(NOT IS_DIRECTORY "${BUILD_DIR}")
    message(FATAL_ERROR "MCO-UNIT-TESTS: build directory not found: ${BUILD_DIR}")
endif()

if(NOT DEFINED MIN_TESTS)
    set(MIN_TESTS 5)
endif()
if(MIN_TESTS LESS 1)
    message(FATAL_ERROR "MCO-UNIT-TESTS: MIN_TESTS must be >= 1 (got ${MIN_TESTS})")
endif()

find_program(MCO_CTEST NAMES ctest REQUIRED)

message(STATUS "MCO-UNIT-TESTS: registered test cases in ${BUILD_DIR}")
execute_process(
    COMMAND ${MCO_CTEST} --test-dir "${BUILD_DIR}" -N
    OUTPUT_VARIABLE _list ERROR_VARIABLE _list_err
    OUTPUT_STRIP_TRAILING_WHITESPACE RESULT_VARIABLE _list_rc)
message(STATUS "${_list}\n${_list_err}")

if(NOT _list MATCHES "Total Tests: +([0-9]+)")
    message(FATAL_ERROR
        "MCO-UNIT-TESTS: 'ctest -N' (exit=${_list_rc}) did not report a test count for "
        "${BUILD_DIR}. A build directory without ctest registration is not a pass; "
        "configure with -DBUILD_TESTING=ON (see cmake/McoTesting.cmake).")
endif()

set(_count "${CMAKE_MATCH_1}")
if(_count LESS MIN_TESTS)
    message(FATAL_ERROR
        "MCO-UNIT-TESTS: only ${_count} test case(s) registered in ${BUILD_DIR}, "
        "expected at least ${MIN_TESTS}. A CI run that executes no test is not a pass "
        "(MCO_EXPECTED_UNIT_TESTS in test/CMakeLists.txt is the reference list).")
endif()
message(STATUS "MCO-UNIT-TESTS: ${_count} case(s) registered, minimum is ${MIN_TESTS}")

execute_process(
    COMMAND ${MCO_CTEST} --test-dir "${BUILD_DIR}" --output-on-failure --timeout 120
    RESULT_VARIABLE _ctest_rc)
if(NOT _ctest_rc EQUAL 0)
    message(FATAL_ERROR "MCO-UNIT-TESTS: ctest failed with exit code ${_ctest_rc}")
endif()

message(STATUS "MCO-UNIT-TESTS: all ${_count} test case(s) passed")
