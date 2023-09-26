
build:
	docker build -t andrewdelph/k6-tests:latest .
	docker push andrewdelph/k6-tests:latest

init:
	@echo "init"
	curl https://raw.githubusercontent.com/grafana/k6-operator/main/bundle.yaml | kubectl apply -f -

setup:
	kubectl --context minikube create namespace k6 || echo "namespace already exists"
	kubectl --context minikube delete configmap test-scripts --ignore-not-found -n k6
	kubectl --context minikube delete configmap prometheus-config --ignore-not-found -n k6
	kubectl --context minikube create configmap test-scripts  \
		--from-file=./test.js -n k6

run:
	make setup
	./run-kube.sh resources/k6-output-grafana-cloud.yaml

logs:
	stern -n k6 .*

prod:
	make setup
	./run-kube.sh resources/k6-output-grafana-cloud.yaml --external

watch:
	watch -n 15 kubectl --context minikube get pods -n k6

delete:
	kubectl --context minikube delete  -f resources/k6-output-grafana-cloud.yaml -n k6
	# cd dependencies/k6-operator && make delete

