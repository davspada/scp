#!/bin/sh

# Clean, then build jar
./clean.sh

echo "Packaging project..."
sbt package

echo "Build finished. The JAR is in target/scala-2.12/"