#!/bin/sh

bazel run //:gazelle
bazel run //:gazelle-update-repos
