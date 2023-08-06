# UPDATE GO DEPS

bazel run //:gazelle -- update-repos -from_file="store/go.mod" -to_macro=repositories.bzl%go_repositories -prune

# DOCKER-COMPOSE DEV

ibazel -run_command_after_success='docker-compose up --force-recreate -d store' run //store:store_image

while true; do docker-compose logs --no-log-prefix -f store; done

# K8 DEV

kubectl create configmap nginx-config --from-file=nginx.conf

kubectl apply -f ./resources/

ibazel -run_command_after_success='kubectl rollout restart deployment store' run //store:image_push

# TEST

ibazel run //store:go_default_test
