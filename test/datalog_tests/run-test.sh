#!/bin/bash
# Run one datalog test

set -e

function usage {
    echo "Usage: run-test.sh testname [debug|release]"
    echo "Run one Datalog test"
    echo "The following environment variables control this script:"
    echo "- DDLOGFLAGS controls the ddlog compilation process"
    echo "- RUSTFLAGS controls Rust compiler flags"
    echo "- CARGOFLAGS controls cargo (Rust package system) compilation flags"
    echo "- CLIFLAGS controls arguments to the generated _cli executable"
    exit 1
}

if [ $# == 0 ]; then
    usage
fi

testname=$1
base=$(basename ${testname} .dl)
shift

build="release"
if [ $# == 1 ]; then
    build=$1
    shift
else
    usage
fi

CARGOFLAGS="--features=flatbuf"
if [ "x${build}" == "xrelease" ]; then
    CARGOFLAGS="--release ${CARGOFLAGS}"
elif [ "x${build}" == "xdebug" ]; then
    CARGOFLAGS="${CARGOFLAGS}"
else
    usage
fi

CLASSPATH=$(pwd)/${base}_ddlog/flatbuf/java:$(pwd)/../../java/ddlogapi.jar:$CLASSPATH
# Run DDlog compiler
ddlog -i ${base}.dl -j -L../../lib ${DDLOGFLAGS}
# Compile produced Rust files
cd ${base}_ddlog
cargo build ${CARGOFLAGS}
cd ..

# Build the Java library with the DDlog API
make -C ../../java
# Build the generated Java classes for serialization (generated by flatbuf)
(cd ${base}_ddlog/flatbuf/java && javac $(ls ddlog/__${base}/*.java) && javac $(ls ddlog/${base}/*.java))

if [ -f ${base}.dat ]; then
    # Run script with input data
    ${base}_ddlog/target/${build}/${base}_cli --no-print ${CLIFLAGS} < ${base}.dat > ${base}.dump
    # Compare outputs
    if [ -f ${base}.dump.expected.gz ]; then
        zdiff -q ${base}.dump ${base}.dump.expected.gz
    elif [ -f ${base}.dump.expected ]; then
        diff -q ${base}.dump ${base}.dump.expected
    fi
fi
# Remote outputs
rm -rf ${base}.dump ${base}.dump.gz
# Additional cleanup possible
# rm -rf ${base}_ddlog
