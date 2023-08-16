# UPDATE GO DEPS

bazel run //:gazelle -- update-repos -from_file="store/go.mod" -to_macro=repositories.bzl%go_repositories -prune

# DOCKER-COMPOSE DEV

ibazel -run_command_after_success='docker-compose up --force-recreate -d store' run //store:store_image

while true; do docker-compose logs -f --tail=1 store; done

# K8 DEV

kubectl create configmap nginx-config --from-file=nginx.conf

kubectl apply -f ./resources/

ibazel -run_command_after_success='kubectl rollout restart deployment store' run //store:image_push

while true; do kubectl logs -f deployment/store; done

# TEST

ibazel test --test_output=errors //store:go_default_test

ibazel run --test_output=errors //store:go_default_test --test_arg=-run=TestStore

# EDITOR SETUP

https://github.com/bazelbuild/rules_go/wiki/Editor-setup
