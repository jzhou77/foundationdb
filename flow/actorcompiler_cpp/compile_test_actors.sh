#!/bin/bash
# compile_test_actors.sh - Compile test actors using actorcompiler_cpp
# This script generates C++ from .actor.cpp files

set -e

# Find the actor compiler binary
ACTORCOMPILER=""
if [ -f "/root/build_output/flow/actorcompiler_cpp/actorcompiler_cpp" ]; then
    ACTORCOMPILER="/root/build_output/flow/actorcompiler_cpp/actorcompiler_cpp"
elif [ -f "./build/actorcompiler_cpp" ]; then
    ACTORCOMPILER="./build/actorcompiler_cpp"
elif [ -f "./actorcompiler_cpp" ]; then
    ACTORCOMPILER="./actorcompiler_cpp"
else
    echo "Error: actorcompiler_cpp not found"
    echo "Please build the project first"
    exit 1
fi

echo "Using actor compiler: $ACTORCOMPILER"
echo ""

# Directory containing test actors
TEST_DIR="tests/runtime_test_actors"
OUTPUT_DIR="$TEST_DIR/generated"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Compile each test actor
for actor_file in "$TEST_DIR"/*.actor.cpp; do
    if [ -f "$actor_file" ]; then
        basename=$(basename "$actor_file" .actor.cpp)
        output_file="$OUTPUT_DIR/${basename}.cpp"
        
        echo "Compiling: $actor_file -> $output_file"
        "$ACTORCOMPILER" "$actor_file" "$output_file"
        
        if [ $? -eq 0 ]; then
            echo "✓ Success: $basename"
        else
            echo "✗ Failed: $basename"
            exit 1
        fi
        echo ""
    fi
done

echo "All test actors compiled successfully!"
echo "Generated files are in: $OUTPUT_DIR"
