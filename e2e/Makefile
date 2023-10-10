
build:
	docker build -t andrewdelph/k6-tests:latest .
	docker push andrewdelph/k6-tests:latest

init:
	@echo "init"
	curl https://raw.githubusercontent.com/grafana/k6-operator/main/bundle.yaml | kubectl apply -f -
	make monitoring

setup:
	kubectl create namespace k6 || echo "namespace already exists"
	kubectl  delete configmap test-scripts --ignore-not-found -n k6
	kubectl  delete configmap prometheus-config --ignore-not-found -n k6
	kubectl  create configmap test-scripts  \
		--from-file=./test.js -n k6

run:
	make setup
	./run-kube.sh resources/k6-output-grafana-cloud.yaml

logs:
	stern -n k6 .*

watch:
	watch kubectl get pods -n k6
	
delete:
	kubectl  delete  -f resources/k6-output-grafana-cloud.yaml -n k6 || true
	helm uninstall loki --namespace=monitoring --wait || true

monitoring:
	helm repo add grafana https://grafana.github.io/helm-charts
	kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml --wait
	kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
	helm upgrade --install loki --namespace=monitoring grafana/loki-stack -f loki-stack.values.yaml
	bash ./create_dashboards.sh
