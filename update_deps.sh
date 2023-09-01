#!/bin/sh
bazel run //:gazelle-update-repos
bazel run //:gazelle
