vcpkg_from_github(
  OUT_SOURCE_PATH SOURCE_PATH
  REPO yatagai-mm/Moqtopus
  REF b27587a03286935c7c5cc57efabb146fc70b1ca3
  SHA512 70c26a2199b5701bbcbc3aba570b3c1ee8149decf8867d5ab7a1d4a6a6b6e006b2c262a5688105301ec9f8be1bc86d582ce834f5c2a0c6269174a075b35e5af1
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
