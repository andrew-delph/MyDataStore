#!/usr/bin/env bash


exec bazel run --execution_log_json_file=tools-events.json --tool_tag=gopackagesdriver -- @io_bazel_rules_go//go/tools/gopackagesdriver "${@}"

