cmake_minimum_required(VERSION 3.20)
project(pluck C)

set(CMAKE_C_STANDARD 99)

add_executable(pluck pluck.c fb_struct.h)
