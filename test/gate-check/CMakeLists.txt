# test/gate-check/CMakeLists.txt
#
# Standalone CMake project that proves the unit-test gate (cmake/McoTesting.cmake)
# fails closed. It compiles no product source and needs no vcpkg dependency, so a
# scenario costs one configure and runs in seconds on any machine that has CMake.
#
#   cmake -S test/gate-check -B <dir> -DMCO_GATE_SCENARIO=missing-dep  # must FAIL: Google Test unavailable
#   cmake -S test/gate-check -B <dir> -DMCO_GATE_SCENARIO=zero-cases   # must FAIL: dependency fine, 0 cases
#   cmake -S test/gate-check -B <dir> -DMCO_GATE_SCENARIO=registered -DBUILD_TESTING=ON   # must PASS
#   cmake -S test/gate-check -B <dir> -DMCO_GATE_SCENARIO=build-only -DBUILD_TESTING=OFF  # must PASS
#
# scripts/check_test_gate.cmake drives the four scenarios with those expectations
# and is what CI runs on Linux and Windows (see the test-gate job in
# .github/workflows/ci.yml).
cmake_minimum_required(VERSION 3.20)
# NONE: the fixture compiles nothing, it only exercises the gate logic.
project(mco_test_gate_check NONE)

set(MCO_GATE_SCENARIO
    ""
    CACHE STRING "Scenario to exercise: missing-dep | zero-cases | registered | build-only")

if(NOT MCO_GATE_SCENARIO MATCHES "^(missing-dep|zero-cases|registered|build-only)$")
    message(FATAL_ERROR
        "MCO-GATE-CHECK: MCO_GATE_SCENARIO must be one of missing-dep, zero-cases, "
        "registered, build-only (got '${MCO_GATE_SCENARIO}')")
endif()

# Keep the scenario and BUILD_TESTING in sync, otherwise a harness typo would
# "pass" for the wrong reason.
if(MCO_GATE_SCENARIO STREQUAL "build-only")
    if(BUILD_TESTING)
        message(FATAL_ERROR "MCO-GATE-CHECK: scenario 'build-only' needs -DBUILD_TESTING=OFF")
    endif()
elseif(NOT BUILD_TESTING)
    message(FATAL_ERROR
        "MCO-GATE-CHECK: scenario '${MCO_GATE_SCENARIO}' needs -DBUILD_TESTING=ON")
endif()

if(MCO_GATE_SCENARIO STREQUAL "missing-dep")
    # Hide Google Test even from a host that has it installed: this is the
    # situation the old QUIET find_package used to shrug off.
    set(CMAKE_DISABLE_FIND_PACKAGE_GTest TRUE)
elseif(MCO_GATE_SCENARIO MATCHES "^(zero-cases|registered)$")
    # Represent "the test dependency is available" without depending on the host.
    # Nothing here links against it; the gate only requires the imported target.
    add_library(GTest::gtest INTERFACE IMPORTED GLOBAL)
endif()

include(${CMAKE_CURRENT_LIST_DIR}/../../cmake/McoTesting.cmake)

mco_testing_begin()

if(MCO_BUILD_TESTS AND MCO_GATE_SCENARIO STREQUAL "registered")
    add_test(NAME gate_probe COMMAND ${CMAKE_COMMAND} -E echo "MCO-GATE-CHECK probe")
endif()

mco_testing_end()
