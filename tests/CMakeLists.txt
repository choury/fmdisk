add_executable(locker_test locker_test.cpp)
target_link_libraries(locker_test pthread)

add_executable(fmtest fm_test.cpp mock_backend.cpp)
target_link_libraries(fmtest fmdisk pthread)
set_property(TARGET fmtest PROPERTY CXX_STANDARD 20)
set_property(TARGET fmtest PROPERTY CXX_STANDARD_REQUIRED ON)
target_compile_definitions(fmtest PRIVATE FMDISK_TEST_SCRIPT_DIR="${CMAKE_CURRENT_SOURCE_DIR}/scripts")

# Discover and register fmtest scripts
# This allows `make test` or `ctest` to run all test scenarios.
file(GLOB FMTEST_SCRIPTS "${CMAKE_CURRENT_SOURCE_DIR}/scripts/*.fmtest")

foreach(FMTEST_SCRIPT ${FMTEST_SCRIPTS})
    get_filename_component(TEST_NAME ${FMTEST_SCRIPT} NAME_WE)
    add_test(
        NAME ${TEST_NAME}
        COMMAND fmtest ${FMTEST_SCRIPT}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )
endforeach()

# Run locker_test after scripted fmtest scenarios to keep longer run time at the end.
add_test(
    NAME locker_test
    COMMAND $<TARGET_FILE:locker_test>
)
