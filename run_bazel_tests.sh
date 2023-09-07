#!/bin/bash

# Run Bazel tests excluding those under the //store package
bazel test $(bazel query 'kind(go_test, //... except //store:*)' --output=label)
