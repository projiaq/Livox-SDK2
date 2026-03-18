cmake_minimum_required(VERSION 3.0)

set(DEMO_NAME point_cloud_recorder)
add_executable(${DEMO_NAME} main.cpp)

target_include_directories(${DEMO_NAME}
        PRIVATE
        ${PROJECT_SOURCE_DIR}/../sdk_core)

target_link_libraries(${DEMO_NAME}
        PUBLIC
        livox_lidar_sdk_static)
