#!/usr/bin/env bash
BIN=`dirname "$0"`
BASE=$BIN/../core
java -Xmx6g -cp "$BASE/target/classes:$BASE/target/*" \
edu.stanford.futuredata.macrobase.cli.CliRunner "$@"
