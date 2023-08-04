# UPDATE GO DEPS

bazel run //:gazelle -- update-repos -from_file="store/go.mod" -to_macro=repositories.bzl%go_repositories -prune

# RUN BUILD SERVER

ibazel -run_command_after_success='docker-compose up --force-recreate -d store' run store_image
