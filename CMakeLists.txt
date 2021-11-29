cmake_minimum_required(VERSION 3.20)
project(pluck VERSION 0.1 LANGUAGES C)

set(CMAKE_C_STANDARD 99)

execute_process(COMMAND /usr/bin/git show -s --format="%H"
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        OUTPUT_VARIABLE COMMIT_HASH)
configure_file(${PROJECT_SOURCE_DIR}/commit.h.in ${PROJECT_SOURCE_DIR}/commit.h)
add_executable(pluck pluck.c fb_struct.h commit.h)
target_link_libraries(pluck pthread)
