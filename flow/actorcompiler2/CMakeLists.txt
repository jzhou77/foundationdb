set(ACTOR_COMPILER_SRCS
    ActorParser.cpp
    ActorParser.h
    ActorCompiler.cpp
    ActorCompiler.h
    ParseTree.h
    Program.cpp)

add_executable(actorcompiler2 ${ACTOR_COMPILER_SRCS})
target_link_libraries(actorcompiler2 PUBLIC fmt::fmt)
target_link_libraries(actorcompiler2 PUBLIC OpenSSL::SSL)
target_link_libraries(actorcompiler2 PUBLIC boost_target)
