bazel run //:gazelle -- update-repos -from_file="store/go.mod" -to_macro=repositories.bzl%go_repositories -prune
