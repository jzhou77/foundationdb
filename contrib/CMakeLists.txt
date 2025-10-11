add_library(rapidjson INTERFACE)
target_include_directories(rapidjson INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/rapidjson)

add_subdirectory(cpu_features)
add_subdirectory(crc32)
add_subdirectory(stacktrace)
add_subdirectory(folly_memcpy)

if(USE_AVX)
  message(STATUS "AVX enabled. ${CMAKE_HOST_SYSTEM_PROCESSOR} Configuring for folly_memcpy...")

  # IMPORTANT: Your target uses an Assembly file (.S).
  # The 'target_compile_features' command is mainly for C/C++. For assembly,
  # it's more reliable to add the flag directly with target_compile_options.
  # This is the correct, target-specific way to add "-mavx".
  if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^x86")
    target_compile_options(folly_memcpy PRIVATE -mavx)
  endif()
endif()

add_subdirectory(rapidxml)
add_subdirectory(sqlite)
add_subdirectory(SimpleOpt)
add_subdirectory(md5)
add_subdirectory(libb64)
if(NOT WIN32)
  add_subdirectory(linenoise)
  add_subdirectory(debug_determinism)
  add_subdirectory(monitoring)
  add_subdirectory(TraceLogHelper)
  add_subdirectory(TestHarness)
endif()
