vcpkg_from_github(
  OUT_SOURCE_PATH SOURCE_PATH
  REPO yatagai-mm/Moqtopus
  REF 5822f983abbbae58afa723ccaf99c338601bda7c
  SHA512 e35049077c4bc669353703c39f9972a98a77d12115e74bef9fa07cc90fe83ee258484a8873721e32dd9caa7682528b16f1b15133ce9eb65e982dbca8b8ea93c1
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
