#!/usr/bin/env bash
# Deploy Provisa to any K8s cluster via Helm.
#
# Usage:
#   ./start-k8s.sh --registry REGISTRY [OPTIONS]
#
# Required:
#   --registry REGISTRY    Image registry prefix, e.g. docker.io/myorg or 123456.dkr.ecr.us-east-1.amazonaws.com
#
# Options:
#   --tag TAG              Image tag (default: git short SHA)
#   --context CONTEXT      kubectl context to use (default: current context)
#   --namespace NS         K8s namespace (default: provisa)
#   --hostname HOST        Ingress hostname, e.g. provisa.example.com (required for HTTP access)
#   --tls-secret SECRET    TLS secret name for ingress HTTPS (must already exist in the namespace)
#   --pg-password PASS     PostgreSQL password (default: prompts interactively)
#   --observability        Install OTel Collector, Tempo, Prometheus, Grafana
#   --mongodb              Install MongoDB
#   --skip-build           Skip image build and push (use existing registry image)
#   --upgrade              Upgrade an existing release instead of fresh install
#   --dry-run              Print the helm command without executing

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Defaults ──────────────────────────────────────────────────────────────────
REGISTRY=""
TAG="$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo "latest")"
KUBE_CONTEXT=""
NAMESPACE="provisa"
HOSTNAME=""
TLS_SECRET=""
PG_PASSWORD=""
INSTALL_OBSERVABILITY=false
INSTALL_MONGODB=false
SKIP_BUILD=false
UPGRADE=false
DRY_RUN=false

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)       REGISTRY="$2";       shift 2 ;;
    --registry=*)     REGISTRY="${1#*=}";  shift ;;
    --tag)            TAG="$2";            shift 2 ;;
    --tag=*)          TAG="${1#*=}";       shift ;;
    --context)        KUBE_CONTEXT="$2";   shift 2 ;;
    --context=*)      KUBE_CONTEXT="${1#*=}"; shift ;;
    --namespace)      NAMESPACE="$2";      shift 2 ;;
    --namespace=*)    NAMESPACE="${1#*=}"; shift ;;
    --hostname)       HOSTNAME="$2";       shift 2 ;;
    --hostname=*)     HOSTNAME="${1#*=}";  shift ;;
    --tls-secret)     TLS_SECRET="$2";     shift 2 ;;
    --tls-secret=*)   TLS_SECRET="${1#*=}"; shift ;;
    --pg-password)    PG_PASSWORD="$2";    shift 2 ;;
    --pg-password=*)  PG_PASSWORD="${1#*=}"; shift ;;
    --observability)  INSTALL_OBSERVABILITY=true; shift ;;
    --mongodb)        INSTALL_MONGODB=true; shift ;;
    --skip-build)     SKIP_BUILD=true;     shift ;;
    --upgrade)        UPGRADE=true;        shift ;;
    --dry-run)        DRY_RUN=true;        shift ;;
    *)
      echo "Unknown option: $1"
      echo "Run '$0 --help' for usage."
      exit 1 ;;
  esac
done

# ── Validate ──────────────────────────────────────────────────────────────────
if [[ -z "$REGISTRY" ]]; then
  echo "ERROR: --registry is required."
  echo "  Example: $0 --registry docker.io/myorg --hostname provisa.example.com"
  exit 1
fi

if [[ -z "$HOSTNAME" ]]; then
  echo "WARNING: --hostname not set. The HTTP API will not be reachable via ingress."
  echo "         Arrow Flight (gRPC) will still be reachable via the LoadBalancer service."
fi

# ── Prerequisites ─────────────────────────────────────────────────────────────
for cmd in kubectl helm docker python3; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' not found. Please install it and retry."
    exit 1
  fi
done

# ── kubectl context ───────────────────────────────────────────────────────────
if [[ -n "$KUBE_CONTEXT" ]]; then
  echo "Switching kubectl context to '$KUBE_CONTEXT'..."
  kubectl config use-context "$KUBE_CONTEXT"
fi
ACTIVE_CONTEXT="$(kubectl config current-context)"
echo "Deploying to cluster context: $ACTIVE_CONTEXT"

# ── Interactive prompts for anything not set via flags ────────────────────────
if [[ -z "$PG_PASSWORD" ]]; then
  read -r -s -p "PostgreSQL password (leave blank to use chart default 'provisa'): " PG_PASSWORD
  echo ""
fi

if [[ "$INSTALL_MONGODB" = false ]]; then
  read -r -p "Install MongoDB? [y/N] " mongo_ans
  [[ "$mongo_ans" =~ ^[Yy] ]] && INSTALL_MONGODB=true
fi

if [[ "$INSTALL_OBSERVABILITY" = false ]]; then
  read -r -p "Install observability stack (OTel Collector, Tempo, Prometheus, Grafana)? [y/N] " obs_ans
  [[ "$obs_ans" =~ ^[Yy] ]] && INSTALL_OBSERVABILITY=true
fi

# ── Build and push image ──────────────────────────────────────────────────────
IMAGE="${REGISTRY}/provisa:${TAG}"

if [[ "$SKIP_BUILD" = false ]]; then
  echo "Building provisa image..."

  WHEELS_DIR="$SCRIPT_DIR/wheels"
  if [ ! -d "$WHEELS_DIR" ] || [ -z "$(ls -A "$WHEELS_DIR" 2>/dev/null)" ]; then
    echo "Downloading wheels for offline image build..."
    mkdir -p "$WHEELS_DIR"
    python3 -m pip download \
      --dest "$WHEELS_DIR" \
      --requirement <(python3 -c "
import tomllib
with open('$SCRIPT_DIR/pyproject.toml','rb') as f:
    d = tomllib.load(f)
for dep in d.get('project',{}).get('dependencies',[]):
    print(dep)
") \
      --quiet
  fi

  docker build \
    --tag "$IMAGE" \
    --file "$SCRIPT_DIR/Dockerfile" \
    "$SCRIPT_DIR"

  echo "Pushing $IMAGE..."
  docker push "$IMAGE"
else
  echo "Skipping image build — using $IMAGE"
fi

# ── Namespace ─────────────────────────────────────────────────────────────────
kubectl get namespace "$NAMESPACE" &>/dev/null || \
  kubectl create namespace "$NAMESPACE"

# ── Helm args ─────────────────────────────────────────────────────────────────
HELM_RELEASE=provisa
HELM_CHART="$SCRIPT_DIR/helm/provisa"

HELM_ARGS=(
  --namespace "$NAMESPACE"
  --set provisa.image.repository="${REGISTRY}/provisa"
  --set provisa.image.tag="$TAG"
  --set provisa.image.pullPolicy=Always
  --set mongodb.enabled="$INSTALL_MONGODB"
  --set observability.enabled="$INSTALL_OBSERVABILITY"
)

# PostgreSQL password
if [[ -n "$PG_PASSWORD" ]]; then
  HELM_ARGS+=(--set postgresql.password="$PG_PASSWORD")
fi

# Ingress
if [[ -n "$HOSTNAME" ]]; then
  HELM_ARGS+=(
    --set ingress.enabled=true
    --set ingress.host="$HOSTNAME"
  )
  if [[ -n "$TLS_SECRET" ]]; then
    HELM_ARGS+=(
      --set ingress.tls.enabled=true
      --set ingress.tls.secretName="$TLS_SECRET"
    )
  fi
fi

# ── Deploy ────────────────────────────────────────────────────────────────────
if [[ "$DRY_RUN" = true ]]; then
  echo ""
  echo "Dry run — would execute:"
  if helm status "$HELM_RELEASE" --namespace "$NAMESPACE" &>/dev/null || [[ "$UPGRADE" = true ]]; then
    echo "  helm upgrade $HELM_RELEASE $HELM_CHART \\"
  else
    echo "  helm install $HELM_RELEASE $HELM_CHART \\"
  fi
  for arg in "${HELM_ARGS[@]}"; do
    echo "    $arg \\"
  done
  exit 0
fi

if helm status "$HELM_RELEASE" --namespace "$NAMESPACE" &>/dev/null || [[ "$UPGRADE" = true ]]; then
  echo "Upgrading Helm release '$HELM_RELEASE'..."
  helm upgrade "$HELM_RELEASE" "$HELM_CHART" "${HELM_ARGS[@]}"
else
  echo "Installing Helm release '$HELM_RELEASE'..."
  helm install "$HELM_RELEASE" "$HELM_CHART" "${HELM_ARGS[@]}"
fi

# ── Wait for rollout ──────────────────────────────────────────────────────────
echo "Waiting for provisa deployment..."
kubectl rollout status deployment/"$HELM_RELEASE"-provisa \
  --namespace "$NAMESPACE" --timeout=300s

echo "Waiting for trino coordinator..."
kubectl rollout status deployment/"$HELM_RELEASE"-trino-coordinator \
  --namespace "$NAMESPACE" --timeout=300s

# ── Print endpoints ───────────────────────────────────────────────────────────
echo ""
echo "Provisa deployed to context '$ACTIVE_CONTEXT' / namespace '$NAMESPACE':"

if [[ -n "$HOSTNAME" ]]; then
  if [[ -n "$TLS_SECRET" ]]; then
    echo "  API:    https://$HOSTNAME"
  else
    echo "  API:    http://$HOSTNAME"
  fi
fi

# Wait up to 90s for the LoadBalancer external IP, then print it
echo -n "  Arrow Flight (LoadBalancer): waiting for external IP"
FLIGHT_IP=""
for i in $(seq 1 18); do
  FLIGHT_IP="$(kubectl get svc "$HELM_RELEASE"-provisa-flight \
    --namespace "$NAMESPACE" \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
  # Some clouds use hostname instead of IP
  [[ -z "$FLIGHT_IP" ]] && FLIGHT_IP="$(kubectl get svc "$HELM_RELEASE"-provisa-flight \
    --namespace "$NAMESPACE" \
    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
  if [[ -n "$FLIGHT_IP" ]]; then
    echo ""
    echo "  Arrow Flight: $FLIGHT_IP:8815"
    break
  fi
  echo -n "."
  sleep 5
done
if [[ -z "$FLIGHT_IP" ]]; then
  echo ""
  echo "  Arrow Flight: external IP pending — check: kubectl get svc $HELM_RELEASE-provisa-flight -n $NAMESPACE"
fi

if [[ "$INSTALL_OBSERVABILITY" = true ]]; then
  echo "  Grafana: kubectl port-forward -n $NAMESPACE svc/$HELM_RELEASE-grafana 3100:3000"
fi

echo ""
echo "To uninstall: helm uninstall $HELM_RELEASE --namespace $NAMESPACE"
