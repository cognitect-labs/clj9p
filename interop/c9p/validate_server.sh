#!/usr/bin/env bash

echo
echo "Reading the root dir: '/'"
9 9p -a 127.0.0.1:9090 ls / # CLJ9P is writing the embedded stat bytes wrong

echo
echo "Stat a dir: '/interjections'"
9 9p -a 127.0.0.1:9090 stat /interjections

echo
echo "Stat a file: '/interjections/hello'"
9 9p -a 127.0.0.1:9090 stat /interjections/hello

echo
echo "Stat a directory: '/interjections'"
9 9p -a 127.0.0.1:9090 stat /interjections

echo
echo "ls a file: '/interjections/hello'"
9 9p -a 127.0.0.1:9090 ls /interjections/hello

echo
echo "ls a synthetic dir: '/interjections'"
9 9p -a 127.0.0.1:9090 ls /interjections # CLJ9P is writing the embedded stat bytes wrong

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

