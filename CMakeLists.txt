cmake_minimum_required(VERSION 3.20)
project(pluck C)

set(CMAKE_C_STANDARD 99)

exec_program(/usr/bin/git ${PROJECT_SOURCE_DIR}
        ARGS show -s --format='\#define COMMIT_HASH \"%H\"' >commit.h)
add_executable(pluck pluck.c fb_struct.h commit.h)
