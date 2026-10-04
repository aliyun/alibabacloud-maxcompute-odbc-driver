# cmake/McoTesting.cmake
#
# Unit-test gate for the MaxCompute ODBC driver.
#
# The driver used to discover Google Test with `find_package(GTest QUIET)` and
# silently skip the whole suite when it was absent, so a build that compiled and
# ran zero tests looked exactly like a build that ran all of them. This module
# makes the intent explicit through the standard BUILD_TESTING option:
#
#   BUILD_TESTING=ON  (default -- the documented vcpkg build, where `gtest` is a
#                     declared dependency in vcpkg.json)
#                     The unit-test suite is part of the build contract:
#                       * Google Test must be available, otherwise configure fails;
#                       * at least one test case must be registered, otherwise
#                         configure fails.
#   BUILD_TESTING=OFF (pure packaging / build-only mode)
#                     No test target is created, testing is not enabled, and the
#                     configure log states explicitly that tests were neither
#                     built nor run, so the produced binaries cannot be mistaken
#                     for a tested build.
#
# Callers that already provide the `GTest::gtest` imported target (a superbuild,
# or the gate harness in test/gate-check) keep it; everyone else gets Google Test
# located for them.
#
# These are macros rather than functions on purpose: enable_testing() is
# directory-scoped and has no effect when it is called from inside a function.

option(BUILD_TESTING "Build and require the C++ unit-test suite" ON)

# mco_testing_begin()
#
# Resolves the test dependency for a testing build and enables ctest
# registration. Sets MCO_BUILD_TESTS to ON or OFF for the calling directory.
macro(mco_testing_begin)
    if(BUILD_TESTING)
        if(NOT TARGET GTest::gtest)
            find_package(GTest QUIET)
            if(NOT TARGET GTest::gtest)
                message(FATAL_ERROR
                    "MCO-BUILD-GATE: BUILD_TESTING=ON but Google Test was not found. "
                    "Refusing to produce a 'successful' build that contains no tests.\n"
                    "  * Documented vcpkg build: 'gtest' is declared in vcpkg.json. Configure through "
                    "scripts/build.sh / scripts/build.bat, or pass the vcpkg toolchain file\n"
                    "      -DCMAKE_TOOLCHAIN_FILE=<VCPKG_ROOT>/scripts/buildsystems/vcpkg.cmake\n"
                    "  * System Google Test: install it and put its GTestConfig.cmake on CMAKE_PREFIX_PATH.\n"
                    "  * Packaging on purpose (no tests wanted): re-configure with -DBUILD_TESTING=OFF.")
            endif()
        endif()

        enable_testing()
        set(MCO_BUILD_TESTS ON)
        message(STATUS
            "MCO-BUILD-GATE: BUILD_TESTING=ON and Google Test is available -> "
            "the unit-test suite is required for this build to succeed")
    else()
        set(MCO_BUILD_TESTS OFF)
        message(STATUS
            "MCO-BUILD-GATE: BUILD_TESTING=OFF -> driver binaries only; "
            "unit tests are NOT built, NOT registered and NOT run")
    endif()
endmacro()

# mco_testing_end()
#
# Second half of the gate: a testing build that registered no test case is an
# error, not a skipped suite. Call it at the end of the directory that adds the
# tests.
macro(mco_testing_end)
    if(BUILD_TESTING)
        get_property(_mco_registered_tests DIRECTORY PROPERTY TESTS)
        list(LENGTH _mco_registered_tests _mco_registered_count)
        if(_mco_registered_count LESS 1)
            message(FATAL_ERROR
                "MCO-BUILD-GATE: BUILD_TESTING=ON but no test case was registered in "
                "${CMAKE_CURRENT_SOURCE_DIR}. A testing build that runs zero tests is a "
                "build error; check the add_test() calls, or configure with "
                "-DBUILD_TESTING=OFF if tests are not wanted.")
        endif()

        list(JOIN _mco_registered_tests ", " _mco_registered_list)
        message(STATUS
            "MCO-BUILD-GATE: BUILD_TESTING=ON -> ${_mco_registered_count} ctest case(s) registered: "
            "${_mco_registered_list}")
        unset(_mco_registered_tests)
        unset(_mco_registered_count)
        unset(_mco_registered_list)
    endif()
endmacro()
