cmake_minimum_required(VERSION 3.21)

project(moqtopus VERSION 0.0.2 LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

find_package(msquic CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

option(MOQTOPUS_BUILD_EXAMPLES "Build Moqtopus examples" ${PROJECT_IS_TOP_LEVEL})
option(MOQTOPUS_BUILD_TOOLS "Build Moqtopus tools" ${PROJECT_IS_TOP_LEVEL})

file(GLOB_RECURSE MOQ_ALL_SRC CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

add_library(moq_subscriber ${MOQ_ALL_SRC})
add_library(moqtopus::moq_subscriber ALIAS moq_subscriber)
target_compile_features(moq_subscriber PUBLIC cxx_std_17)
target_include_directories(moq_subscriber
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
  PRIVATE src
)
target_link_libraries(moq_subscriber PRIVATE msquic)
target_link_libraries(moq_subscriber PRIVATE spdlog::spdlog)
target_compile_definitions(moq_subscriber PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)

if(MOQTOPUS_BUILD_TOOLS)
  # moq-interop-runner compatible test client
  add_executable(moqtopus_interop_client tools/moqtopus_interop_client.cpp)
  target_link_libraries(moqtopus_interop_client PRIVATE moq_subscriber spdlog::spdlog)
endif()

if(MOQTOPUS_BUILD_EXAMPLES)
  add_executable(subscriber example/subscriber.cpp)
  target_link_libraries(subscriber PRIVATE moq_subscriber spdlog::spdlog)

  add_executable(publisher example/publisher.cpp)
  target_link_libraries(publisher PRIVATE moq_subscriber spdlog::spdlog)
endif()

install(
  TARGETS moq_subscriber
  EXPORT moqtopus-targets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

set(MOQTOPUS_INSTALL_CMAKEDIR "${CMAKE_INSTALL_DATAROOTDIR}/moqtopus")
configure_package_config_file(
  cmake/moqtopus-config.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/moqtopus-config.cmake"
  INSTALL_DESTINATION "${MOQTOPUS_INSTALL_CMAKEDIR}"
)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/moqtopus-config-version.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion
)

install(
  EXPORT moqtopus-targets
  FILE moqtopus-targets.cmake
  NAMESPACE moqtopus::
  DESTINATION "${MOQTOPUS_INSTALL_CMAKEDIR}"
)
install(
  FILES
    "${CMAKE_CURRENT_BINARY_DIR}/moqtopus-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/moqtopus-config-version.cmake"
  DESTINATION "${MOQTOPUS_INSTALL_CMAKEDIR}"
)
