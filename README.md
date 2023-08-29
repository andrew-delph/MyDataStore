# UPDATE GO DEPS

bazel run //:gazelle -- update-repos -from_file="store/go.mod" -to_macro=repositories.bzl%go_repositories -prune

# LOCAL DEC

tmuxinator local

# K8 DEV

kubectl create configmap nginx-config --from-file=nginx.conf

kubectl apply -f ./resources/

ibazel -run_command_after_success='./deploy_k8.sh' run //store:image_push

while true; do kubectl logs -f deployment/store; done

# TEST

ibazel test --test_output=errors //store:go_default_test

ibazel run --test_output=errors //store:go_default_test --test_arg=-test.run=TestStore

# EDITOR SETUP

https://github.com/bazelbuild/rules_go/wiki/Editor-setup
