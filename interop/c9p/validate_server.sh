#!/usr/bin/env bash

# Error: Fid mishandling
echo
echo "Reading the root dir: '/'"
9 9p -a 127.0.0.1:9090 ls /

echo
echo "Stat a dir: '/interjections'"
9 9p -a 127.0.0.1:9090 stat /interjections

echo
echo "Reading a synthetic dir: '/interjections'"
echo "Skipping -- offset on interop-dirreader isn't working per spec"
#9 9p -a 127.0.0.1:9090 read /interjections # This one doesn't respect offset and just spews forever
#9 9p -a 127.0.0.1:9090 ls /interjections # This one hangs can causes infinite allocation on the server

echo
echo "Reading a file: '/interjections/hello'"
9 9p -a 127.0.0.1:9090 read /interjections/hello

echo
echo "Writing to a file: '/interjections/hello'"
echo "hello" | 9 9p -a 127.0.0.1:9090 write /interjections/hello

echo
echo "Reading a file after write: '/interjections/hello'"
9 9p -a 127.0.0.1:9090 read /interjections/hello
# Reset the server text...
echo "Hello World" | 9 9p -a 127.0.0.1:9090 write /interjections/hello

echo
echo
echo "done!"

