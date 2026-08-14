# Copyright (c) 2026 Kenneth Stott
#
# The Provisa federation-engine image (REQ-1447).
#
# Compose hands stock trinodb/trino the plugins and the OpenTelemetry agent as
# bind mounts from the host checkout. A GKE node has no checkout to mount, so
# everything a coordinator needs at JVM start has to be IN the image. Baking the
# agent in is also what ends the jvm.config divergence: isolated_provisioner had
# to ship a jvm.config with no -javaagent line, because the agent existed only
# where compose mounted it, and a missing agent jar aborts the JVM before Trino
# logs a word. One image, one jvm.config, both lanes.
#
#   docker build -f docker/trino-engine.Dockerfile \
#     --build-arg PROVISA_VERSION=v0.1.0 \
#     -t us-central1-docker.pkg.dev/$PROJECT/provisa/trino-engine:v0.1.0 .

ARG TRINO_VERSION=481
FROM trinodb/trino:${TRINO_VERSION}

ARG PROVISA_VERSION
ARG GITHUB_REPO=kenstott/provisa
ARG OTEL_AGENT_VERSION=v2.11.0

USER root

# The connectors Provisa adds to Trino. They ship as a release asset rather than
# living in the tree, which is the same source packaging/linux/first-launch.sh
# pulls from — the image and the desktop install therefore carry identical
# plugin builds instead of two independently-assembled sets.
RUN set -eux; \
    microdnf install -y tar gzip; \
    curl -fSL --retry 5 --retry-all-errors \
      "https://github.com/${GITHUB_REPO}/releases/download/${PROVISA_VERSION}/provisa-trino-plugins-${PROVISA_VERSION}.tar.gz" \
      -o /tmp/plugins.tar.gz; \
    tar -xzf /tmp/plugins.tar.gz -C /usr/lib/trino/plugin; \
    rm /tmp/plugins.tar.gz; \
    microdnf clean all

# Pinned, not "latest": an engine image that resolves a different agent build on
# every rebuild produces spans whose attributes drift without any change here.
RUN set -eux; \
    mkdir -p /etc/trino/otel; \
    curl -fSL --retry 5 --retry-all-errors \
      "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar" \
      -o /etc/trino/otel/opentelemetry-javaagent.jar

RUN chown -R trino:trino /usr/lib/trino/plugin /etc/trino/otel

USER trino
