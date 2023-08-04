bazel run //:gazelle -- update-repos -from_file="store/go.mod" -to_macro=repositories.bzl%go_repositories -prune

ibazel -run_command_after_success='docker-compose up --force-recreate -d store' run store_image
