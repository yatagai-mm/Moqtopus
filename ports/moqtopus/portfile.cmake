vcpkg_from_github(
  OUT_SOURCE_PATH SOURCE_PATH
  REPO yatagai-mm/Moqtopus
  REF 65869019e1cb36b0acbd96760360bccbdb5c8a78
  SHA512 80d18acf2e7f746be666f80e093455cb0ade3048c9551c1c6b08b24d9af35080555575a77e36fdd8569834bd10e99274ad5a61274701e75fcf0471f0768ee9a0
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
