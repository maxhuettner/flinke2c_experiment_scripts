#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "NebulaStream::nes-grpc" for configuration "Release"
set_property(TARGET NebulaStream::nes-grpc APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-grpc PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-grpc.so"
  IMPORTED_SONAME_RELEASE "libnes-grpc.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-grpc )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-grpc "${_IMPORT_PREFIX}/lib/libnes-grpc.so" )

# Import target "NebulaStream::nes-common" for configuration "Release"
set_property(TARGET NebulaStream::nes-common APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-common PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-common.so"
  IMPORTED_SONAME_RELEASE "libnes-common.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-common )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-common "${_IMPORT_PREFIX}/lib/libnes-common.so" )

# Import target "NebulaStream::nes-data-types" for configuration "Release"
set_property(TARGET NebulaStream::nes-data-types APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-data-types PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-data-types.so"
  IMPORTED_SONAME_RELEASE "libnes-data-types.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-data-types )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-data-types "${_IMPORT_PREFIX}/lib/libnes-data-types.so" )

# Import target "NebulaStream::nes-compiler" for configuration "Release"
set_property(TARGET NebulaStream::nes-compiler APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-compiler PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_RELEASE "NebulaStream::nes-common"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-compiler.so"
  IMPORTED_SONAME_RELEASE "libnes-compiler.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-compiler )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-compiler "${_IMPORT_PREFIX}/lib/libnes-compiler.so" )

# Import target "NebulaStream::nes-runtime" for configuration "Release"
set_property(TARGET NebulaStream::nes-runtime APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-runtime PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_RELEASE "NebulaStream::nes-compiler"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-runtime.so"
  IMPORTED_SONAME_RELEASE "libnes-runtime.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-runtime )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-runtime "${_IMPORT_PREFIX}/lib/libnes-runtime.so" )

# Import target "NebulaStream::nes-catalogs" for configuration "Release"
set_property(TARGET NebulaStream::nes-catalogs APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-catalogs PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-catalogs.so"
  IMPORTED_SONAME_RELEASE "libnes-catalogs.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-catalogs )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-catalogs "${_IMPORT_PREFIX}/lib/libnes-catalogs.so" )

# Import target "NebulaStream::nes-configurations" for configuration "Release"
set_property(TARGET NebulaStream::nes-configurations APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-configurations PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-configurations.so"
  IMPORTED_SONAME_RELEASE "libnes-configurations.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-configurations )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-configurations "${_IMPORT_PREFIX}/lib/libnes-configurations.so" )

# Import target "NebulaStream::nes-operators" for configuration "Release"
set_property(TARGET NebulaStream::nes-operators APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-operators PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-operators.so"
  IMPORTED_SONAME_RELEASE "libnes-operators.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-operators )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-operators "${_IMPORT_PREFIX}/lib/libnes-operators.so" )

# Import target "NebulaStream::nes-optimizer" for configuration "Release"
set_property(TARGET NebulaStream::nes-optimizer APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-optimizer PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-optimizer.so"
  IMPORTED_SONAME_RELEASE "libnes-optimizer.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-optimizer )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-optimizer "${_IMPORT_PREFIX}/lib/libnes-optimizer.so" )

# Import target "NebulaStream::nes-worker" for configuration "Release"
set_property(TARGET NebulaStream::nes-worker APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-worker PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-worker.so"
  IMPORTED_SONAME_RELEASE "libnes-worker.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-worker )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-worker "${_IMPORT_PREFIX}/lib/libnes-worker.so" )

# Import target "NebulaStream::nes-coordinator" for configuration "Release"
set_property(TARGET NebulaStream::nes-coordinator APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-coordinator PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-coordinator.so"
  IMPORTED_SONAME_RELEASE "libnes-coordinator.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-coordinator )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-coordinator "${_IMPORT_PREFIX}/lib/libnes-coordinator.so" )

# Import target "NebulaStream::nes-client" for configuration "Release"
set_property(TARGET NebulaStream::nes-client APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-client PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_RELEASE "NebulaStream::nes-operators;NebulaStream::nes-grpc;NebulaStream::nes-common"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-client.so"
  IMPORTED_SONAME_RELEASE "libnes-client.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-client )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-client "${_IMPORT_PREFIX}/lib/libnes-client.so" )

# Import target "NebulaStream::nes-execution" for configuration "Release"
set_property(TARGET NebulaStream::nes-execution APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-execution PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_RELEASE "NebulaStream::nes-compiler"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-execution.so"
  IMPORTED_SONAME_RELEASE "libnes-execution.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-execution )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-execution "${_IMPORT_PREFIX}/lib/libnes-execution.so" )

# Import target "NebulaStream::nes-nautilus" for configuration "Release"
set_property(TARGET NebulaStream::nes-nautilus APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-nautilus PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_RELEASE "mlir_float16_utils;mlir_c_runner_utils;mlir_runner_utils;mlir_async_runtime"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-nautilus.so"
  IMPORTED_SONAME_RELEASE "libnes-nautilus.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-nautilus )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-nautilus "${_IMPORT_PREFIX}/lib/libnes-nautilus.so" )

# Import target "NebulaStream::nes-expressions" for configuration "Release"
set_property(TARGET NebulaStream::nes-expressions APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-expressions PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-expressions.so"
  IMPORTED_SONAME_RELEASE "libnes-expressions.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-expressions )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-expressions "${_IMPORT_PREFIX}/lib/libnes-expressions.so" )

# Import target "NebulaStream::nes-statistics" for configuration "Release"
set_property(TARGET NebulaStream::nes-statistics APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-statistics PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-statistics.so"
  IMPORTED_SONAME_RELEASE "libnes-statistics.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-statistics )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-statistics "${_IMPORT_PREFIX}/lib/libnes-statistics.so" )

# Import target "NebulaStream::nes-window-types" for configuration "Release"
set_property(TARGET NebulaStream::nes-window-types APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-window-types PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnes-window-types.so"
  IMPORTED_SONAME_RELEASE "libnes-window-types.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-window-types )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-window-types "${_IMPORT_PREFIX}/lib/libnes-window-types.so" )

# Import target "NebulaStream::nes-arrow" for configuration "Release"
set_property(TARGET NebulaStream::nes-arrow APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NebulaStream::nes-arrow PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/bin/nes-plugins/libnes-arrow.so"
  IMPORTED_SONAME_RELEASE "libnes-arrow.so"
  )

list(APPEND _cmake_import_check_targets NebulaStream::nes-arrow )
list(APPEND _cmake_import_check_files_for_NebulaStream::nes-arrow "${_IMPORT_PREFIX}/bin/nes-plugins/libnes-arrow.so" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
