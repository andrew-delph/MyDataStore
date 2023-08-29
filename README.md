# UPDATE GO DEPS

bazel run //:gazelle -- update-repos -from_file="store/go.mod" -to_macro=repositories.bzl%go_repositories -prune

# DOCKER-COMPOSE DEV

tmuxinator start -p ./.tmuxinator/tmuxinator-dc.yaml

# KUBERNETES DEV

tmuxinator start -p ./.tmuxinator/tmuxinator-k8.yaml

# TEST

ibazel test --test_output=errors //store:go_default_test

ibazel run --test_output=errors //store:go_default_test --test_arg=-test.run=TestStore

# EDITOR SETUP

https://github.com/bazelbuild/rules_go/wiki/Editor-setup
