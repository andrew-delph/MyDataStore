# UPDATE GO DEPS

./dev deps

# DOCKER-COMPOSE DEV

./dev dc

# KUBERNETES DEV

./dev k8-init

./dev k8

# E2E

./dev e2e

# TEST

./dev test

ibazel run --test_output=errors //store:go_default_test --test_arg=-test.run=TestStore
// ibazel run --test_output=errors //store:go_default_test --test_arg=-test.run=TestSpeed --verbose_failures --test_strategy=standalone

# EDITOR SETUP

https://github.com/bazelbuild/rules_go/wiki/Editor-setup
