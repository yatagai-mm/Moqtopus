vcpkg_from_github(
  OUT_SOURCE_PATH SOURCE_PATH
  REPO yatagai-mm/Moqtopus
  REF b511971eb096df932b699577f9fc4ec403b538f7
  SHA512 01b2e53e1cc7de170b2beeefcde52d8016ef5e79d3f1c6026d085bde9c5425a7e86ab4e9465086814567fb47fe5f9812cce6fc8ed56eba47fe74975c59d9516f
  HEAD_REF main
)

vcpkg_cmake_configure(
  SOURCE_PATH "${SOURCE_PATH}"
  OPTIONS
    -DMOQTOPUS_BUILD_EXAMPLES=OFF
    -DMOQTOPUS_BUILD_TOOLS=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(
  PACKAGE_NAME moqtopus
  CONFIG_PATH share/moqtopus
)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
