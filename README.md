# UPDATE GO DEPS

./dev deps

# DOCKER-COMPOSE DEV

./dev dc

# KUBERNETES DEV

minikube start && ./deploy_k8.sh

./dev k8

# E2E

./dev e2e

# TEST

ibazel test --test_output=errors //store:go_default_test

ibazel run --test_output=errors //store:go_default_test --test_arg=-test.run=TestStore

# EDITOR SETUP

https://github.com/bazelbuild/rules_go/wiki/Editor-setup
