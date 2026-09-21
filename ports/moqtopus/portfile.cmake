vcpkg_from_github(
  OUT_SOURCE_PATH SOURCE_PATH
  REPO yatagai-mm/Moqtopus
  REF 95b163c1f9c1401cfd27396a87de9236164b35e9
  SHA512 6250f6cedac6619ac31c4860e2230fe2a941d2f42d2b7989fb8ba4071feef59610d1306ab69cc0c86eb5ac1f5e5c703042fde31b94f967ce1867fde8f570bb9f
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
