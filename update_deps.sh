#!/bin/sh

# Exit on any error
set -e

bazel run //:gazelle
bazel run //:gazelle-update-repos
