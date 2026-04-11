#!/usr/bin/env bash
# Start a minikube cluster and deploy the Provisa Helm chart.
# Usage: ./start-minikube.sh [--reset] [--observability] [--hostname NAME]
#   --reset            Delete and recreate the minikube cluster
#   --observability    Skip prompt, install observability stack
#   --hostname NAME    Expose via nginx ingress at NAME instead of port-forward

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESET=false
OBSERVABILITY_FLAG=false
HOSTNAME=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset) RESET=true; shift ;;
    --observability) OBSERVABILITY_FLAG=true; shift ;;
    --hostname)
      if [[ -z "${2:-}" ]]; then echo "--hostname requires a value"; exit 1; fi
      HOSTNAME="$2"; shift 2 ;;
    --hostname=*) HOSTNAME="${1#--hostname=}"; shift ;;
    *) echo "Unknown option: $1"
       echo "Usage: $0 [--reset] [--observability] [--hostname NAME]"
       exit 1 ;;
  esac
done

# ── Prerequisites ────────────────────────────────────────────────────────────
for cmd in minikube helm kubectl docker python3; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' not found. Please install it and retry."
    exit 1
  fi
done

# ── Observability prompt ─────────────────────────────────────────────────────
INSTALL_OBSERVABILITY=false
if [ "$OBSERVABILITY_FLAG" = true ]; then
  INSTALL_OBSERVABILITY=true
else
  read -r -p "Install observability stack (OTel Collector, Tempo, Prometheus, Grafana)? [y/N] " obs_ans
  case "$obs_ans" in
    [Yy]*) INSTALL_OBSERVABILITY=true ;;
    *) INSTALL_OBSERVABILITY=false ;;
  esac
fi

# ── MongoDB prompt ───────────────────────────────────────────────────────────
read -r -p "Install MongoDB? [y/N] " mongo_ans
case "$mongo_ans" in
  [Yy]*) INSTALL_MONGODB=true ;;
  *) INSTALL_MONGODB=false ;;
esac

# ── Minikube cluster ─────────────────────────────────────────────────────────
if [ "$RESET" = true ]; then
  echo "Deleting existing minikube cluster..."
  minikube delete --profile provisa 2>/dev/null || true
fi

if ! minikube status --profile provisa &>/dev/null; then
  echo "Starting minikube cluster (profile: provisa)..."
  minikube start \
    --profile provisa \
    --cpus 4 \
    --memory 8192 \
    --disk-size 40g \
    --driver docker
else
  echo "Minikube cluster 'provisa' already running."
fi

kubectl config use-context provisa

# ── Build provisa image inside minikube ──────────────────────────────────────
echo "Building provisa Docker image inside minikube..."
eval "$(minikube docker-env --profile provisa)"

# Dockerfile uses --no-index --find-links /wheels; build wheels if missing or stale
WHEELS_DIR="$SCRIPT_DIR/wheels"
if [ ! -d "$WHEELS_DIR" ] || [ -z "$(ls -A "$WHEELS_DIR" 2>/dev/null)" ]; then
  echo "Downloading wheels for offline image build..."
  mkdir -p "$WHEELS_DIR"
  python3 -m pip download \
    --dest "$WHEELS_DIR" \
    --requirement <(python3 -c "
import tomllib, sys
with open('$SCRIPT_DIR/pyproject.toml','rb') as f:
    d = tomllib.load(f)
for dep in d.get('project',{}).get('dependencies',[]):
    print(dep)
") \
    --quiet
fi

docker build \
  --tag provisa:latest \
  --file "$SCRIPT_DIR/Dockerfile" \
  "$SCRIPT_DIR"

# Restore host docker env
eval "$(minikube docker-env --profile provisa --unset)"

# ── Helm deploy ──────────────────────────────────────────────────────────────
HELM_RELEASE=provisa
HELM_CHART="$SCRIPT_DIR/helm/provisa"
HELM_NAMESPACE=provisa

kubectl get namespace "$HELM_NAMESPACE" &>/dev/null || \
  kubectl create namespace "$HELM_NAMESPACE"

# Dev overrides: reduced resources, single replicas, no autoscaling, no ingress
HELM_ARGS=(
  --namespace "$HELM_NAMESPACE"
  --set provisa.replicaCount=1
  --set provisa.image.pullPolicy=Never
  --set trino.coordinator.resources.requests.cpu=250m
  --set trino.coordinator.resources.requests.memory=1Gi
  --set trino.coordinator.resources.limits.cpu=1
  --set trino.coordinator.resources.limits.memory=2Gi
  --set trino.worker.replicaCount=0
  --set trino.worker.autoscaling.enabled=false
  --set provisa.hpa.enabled=false
  --set ingress.enabled=false
  --set mongodb.enabled="$INSTALL_MONGODB"
  --set observability.enabled="$INSTALL_OBSERVABILITY"
  # Use ClusterIP for Flight in minikube — LoadBalancer requires minikube tunnel or a cloud provider.
  # Port-forward below provides local access.
  --set provisa.flightService.type=ClusterIP
)

if helm status "$HELM_RELEASE" --namespace "$HELM_NAMESPACE" &>/dev/null; then
  echo "Upgrading Helm release '$HELM_RELEASE'..."
  helm upgrade "$HELM_RELEASE" "$HELM_CHART" "${HELM_ARGS[@]}"
else
  echo "Installing Helm release '$HELM_RELEASE'..."
  helm install "$HELM_RELEASE" "$HELM_CHART" "${HELM_ARGS[@]}"
fi

# ── Wait for rollout ─────────────────────────────────────────────────────────
echo "Waiting for provisa deployment to be ready..."
kubectl rollout status deployment/"$HELM_RELEASE"-provisa \
  --namespace "$HELM_NAMESPACE" \
  --timeout=300s

echo "Waiting for trino coordinator to be ready..."
kubectl rollout status deployment/"$HELM_RELEASE"-trino-coordinator \
  --namespace "$HELM_NAMESPACE" \
  --timeout=300s

# ── Expose services ──────────────────────────────────────────────────────────
echo ""

if [[ -n "$HOSTNAME" ]]; then
  # ── Ingress mode ────────────────────────────────────────────────────────────
  echo "Enabling minikube ingress addon..."
  minikube addons enable ingress --profile provisa

  # Patch Helm release to enable ingress with the given hostname
  helm upgrade "$HELM_RELEASE" "$HELM_CHART" "${HELM_ARGS[@]}" \
    --set ingress.enabled=true \
    --set ingress.host="$HOSTNAME"

  MINIKUBE_IP="$(minikube ip --profile provisa)"

  kubectl port-forward \
    --namespace "$HELM_NAMESPACE" \
    svc/"$HELM_RELEASE"-provisa-flight 8815:8815 &
  PF_FLIGHT=$!

  echo ""
  echo "Provisa running in minikube (profile: provisa):"
  echo "  API:          http://$HOSTNAME"
  echo "  Arrow Flight: localhost:8815  (port-forward; for cross-device access on a real cluster, Flight uses a LoadBalancer service)"
  [ "$INSTALL_OBSERVABILITY" = true ] && echo "  Grafana:      http://$HOSTNAME/grafana"
  echo ""
  echo "Add this entry to /etc/hosts if $HOSTNAME does not resolve via DNS:"
  echo "  $MINIKUBE_IP  $HOSTNAME"

  cleanup() {
    echo "Stopping port-forwards..."
    kill $PF_FLIGHT 2>/dev/null || true
    echo "Minikube cluster 'provisa' is still running. Use 'minikube stop --profile provisa' to stop it."
  }
  trap cleanup EXIT INT TERM
  wait $PF_FLIGHT

else
  # ── Port-forward mode ────────────────────────────────────────────────────────
  echo "Starting port-forwards (background)..."

  # Exposed services:
  #   8000  — Provisa HTTP API
  #   8815  — Provisa Arrow Flight (gRPC)
  #   9001  — MinIO console (browse redirect results)
  #   8080  — Trino UI
  kubectl port-forward --namespace "$HELM_NAMESPACE" svc/"$HELM_RELEASE"-provisa        8000:8000 &
  PF_API=$!
  kubectl port-forward --namespace "$HELM_NAMESPACE" svc/"$HELM_RELEASE"-provisa-flight 8815:8815 &
  PF_FLIGHT=$!
  kubectl port-forward --namespace "$HELM_NAMESPACE" svc/"$HELM_RELEASE"-minio    9001:9001 &
  PF_MINIO=$!
  kubectl port-forward --namespace "$HELM_NAMESPACE" svc/"$HELM_RELEASE"-trino-coordinator 8080:8080 &
  PF_TRINO=$!

  PF_GRAFANA=""
  if [ "$INSTALL_OBSERVABILITY" = true ]; then
    kubectl port-forward --namespace "$HELM_NAMESPACE" svc/"$HELM_RELEASE"-grafana 3100:3000 &
    PF_GRAFANA=$!
  fi

  echo ""
  echo "Provisa running in minikube (profile: provisa):"
  echo "  API:           http://localhost:8000"
  echo "  Arrow Flight:  localhost:8815"
  echo "  Trino UI:      http://localhost:8080"
  echo "  MinIO console: http://localhost:9001  (user: minioadmin / minioadmin)"
  [ "$INSTALL_OBSERVABILITY" = true ] && echo "  Grafana:       http://localhost:3100"
  echo ""
  echo "Press Ctrl+C to stop port-forwards."
  echo ""

  cleanup() {
    echo "Stopping port-forwards..."
    kill $PF_API $PF_FLIGHT $PF_MINIO $PF_TRINO 2>/dev/null || true
    [[ -n "$PF_GRAFANA" ]] && kill "$PF_GRAFANA" 2>/dev/null || true
    echo "Minikube cluster 'provisa' is still running. Use 'minikube stop --profile provisa' to stop it."
  }
  trap cleanup EXIT INT TERM

  wait $PF_API $PF_FLIGHT $PF_MINIO $PF_TRINO
fi
