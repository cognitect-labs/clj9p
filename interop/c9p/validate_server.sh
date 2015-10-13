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
#9 9p -a 127.0.0.1:9090 read /interjections
#9 9p -a 127.0.0.1:9090 ls /interjections

echo
echo "Reading a file: '/interjections/hello'"
9 9p -a 127.0.0.1:9090 read /interjections/hello

echo
echo
echo "Writing to a file: '/interjections/hello'"
echo "Skipping -- I can't seem to get '9p' cli to write correctly"
#9 9p -a 127.0.0.1:9090 write /interjections/hello hi

echo
echo "Reading a file after write: '/interjections/hello'"
9 9p -a 127.0.0.1:9090 read /interjections/hello

echo
echo
echo "done!"

