set(LCCV_SRCS lccv.actor.cpp)

add_flow_target(EXECUTABLE NAME lccv SRCS "${LCCV_SRCS}")

target_link_libraries(lccv PUBLIC fdbclient)
