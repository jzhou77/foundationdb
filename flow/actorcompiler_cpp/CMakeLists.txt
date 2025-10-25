# Actor Compiler C++ Implementation
# Transpiler that converts .actor.h and .actor.cpp files to standard C++

cmake_minimum_required(VERSION 3.13)

set(ACTORCOMPILER_CPP_SRCS
  main.cpp
  ParseTree.cpp
  Token.cpp
  Tokenizer.cpp
  ActorParser.cpp
  ActorCompiler.cpp
)

set(ACTORCOMPILER_CPP_HEADERS
  Error.h
  ParseTree.h
  Token.h
  TokenRange.h
  Tokenizer.h
  ActorParser.h
  ActorCompiler.h
  Function.h
  Context.h
)

add_executable(actorcompiler_cpp ${ACTORCOMPILER_CPP_SRCS} ${ACTORCOMPILER_CPP_HEADERS})

# Require C++17 for std::filesystem, std::optional, string_view
target_compile_features(actorcompiler_cpp PRIVATE cxx_std_17)

target_include_directories(actorcompiler_cpp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Find OpenSSL for SHA256 hashing
find_package(OpenSSL REQUIRED)
target_link_libraries(actorcompiler_cpp PRIVATE OpenSSL::Crypto)

# Platform-specific settings
if(WIN32)
  # Windows: filesystem is in std
elseif(APPLE)
  # macOS: May need explicit filesystem linking on older systems
  target_compile_options(actorcompiler_cpp PRIVATE -Wall -Wextra)
else()
  # Linux: May need to link stdc++fs on GCC < 9
  target_compile_options(actorcompiler_cpp PRIVATE -Wall -Wextra)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
    target_link_libraries(actorcompiler_cpp PRIVATE stdc++fs)
  endif()
endif()

# Install to bin directory
install(TARGETS actorcompiler_cpp DESTINATION bin COMPONENT clients)

# --- Tests ---
add_executable(actorcompiler_cpp_tokenizer_smoke
  tests/tokenizer_smoke.cpp
  Tokenizer.cpp
  Token.cpp
)
target_include_directories(actorcompiler_cpp_tokenizer_smoke PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(actorcompiler_cpp_tokenizer_smoke PRIVATE cxx_std_17)
add_test(NAME ActorCompilerTokenizerSmoke COMMAND actorcompiler_cpp_tokenizer_smoke)

add_executable(actorcompiler_cpp_parser_smoke
  tests/parser_smoke.cpp
  Tokenizer.cpp
  Token.cpp
  ParseTree.cpp
  ActorParser.cpp
  ActorCompiler.cpp
)
target_include_directories(actorcompiler_cpp_parser_smoke PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(actorcompiler_cpp_parser_smoke PRIVATE cxx_std_17)
target_link_libraries(actorcompiler_cpp_parser_smoke PRIVATE OpenSSL::Crypto)
add_test(NAME ActorCompilerParserSmoke COMMAND actorcompiler_cpp_parser_smoke)
