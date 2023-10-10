#!/bin/bash

# default dashboards: https://github.com/prometheus-community/helm-charts/tree/8f91606e01b85b9aa013ed472c5acb836c4e7ac8/charts/kube-prometheus-stack/templates/grafana/dashboards-1.14

# delete existing dashboards
kubectl delete configmap --selector custom_dashboard='1' -n monitoring

# Set the directory path
dir_path="./dashboards"

# Loop through all files in the directory
for file_path in $dir_path/*; do
  file_name=$(basename "${file_path%.*}")

  # # Create the ConfigMap using the file contents and name as the ConfigMap name
  kubectl create configmap "$file_name" --from-file="$file_path" -n monitoring

  # # Add labels to the ConfigMap
  kubectl label configmap "$file_name" grafana_dashboard='1' custom_dashboard='1' -n monitoring

done