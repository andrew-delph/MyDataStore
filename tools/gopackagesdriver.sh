#!/usr/bin/env bash


exec bazel run --tool_tag=gopackagesdriver -- @io_bazel_rules_go//go/tools/gopackagesdriver "${@}"

