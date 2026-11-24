# The Maven release is pinned, as scripts/build_trino_functions.sh pins its own.
FROM maven:3.9.16-eclipse-temurin-17 AS build
RUN git clone https://github.com/Raiffeisen-DGTL/zaychik-trino-proxy.git /src && \
    cd /src && git checkout 17fce978076c5136b5d3ef1c1291527e8ec5915e
WORKDIR /src

# Fix: decodePeerIdentity crashes when password is empty (ArrayIndexOutOfBoundsException)
# The token is base64("user:password") — when no password, split produces length 1
COPY fix-decode-peer-identity.patch /tmp/
RUN git apply /tmp/fix-decode-peer-identity.patch

# REQ-2194: a Flight SQL statement runs through the Trino client protocol (StatementClient), not
# JDBC, so Provisa's reads arrive as Arrow with what JDBC drops (docs/arch/fetch-paths.md §4):
#  - TIME/TIMESTAMP in microseconds (half-even past six digits, as the Trino Python client rounds);
#  - every field's Trino type ("trino.type"), type signature and encoding in its metadata; a type
#    Arrow lacks (map, row, json, uuid, ipaddress, intervals, time with time zone, an array of one
#    of them) is Utf8 holding the protocol's text/JSON instead of failing as an unmapped JDBC type;
#  - an array of a type Arrow has is an Arrow list of it, nested as deep as the array;
#  - source, catalog, schema, session properties and time zone from the x-provisa-trino-context
#    call header;
#  - Trino's error name/type/code and query id in the Flight error's trailers.
# The statement runs at GetFlightInfo up to its columns; DoGet streams the rows.
COPY trino-statement-protocol.patch /tmp/
RUN git apply /tmp/trino-statement-protocol.patch

# Maven's HTTP transport retries a request three times and only on 429 and 503. Maven Central
# answering one POM with 502 Bad Gateway therefore failed the whole image build on the first try.
# A gateway error is retried too, five times, with the transport's own backoff.
ENV MAVEN_ARGS="-Daether.connector.http.retryHandler.count=5 -Daether.connector.http.retryHandler.serviceUnavailable=429,502,503,504"

# The dependency plugin is named by its coordinates and version, not by its prefix: a prefix
# (`dependency:`) is resolved by reading the plugin groups' maven-metadata.xml from Maven Central,
# and a failed read of it is reported only as "No plugin found for prefix 'dependency'" (run
# 44379779451, kafka lane, after 752 s of retries). CI builds this image once per change to this
# directory and every lane loads it (.github/actions/zaychik-image).
ARG DEPENDENCY_PLUGIN=org.apache.maven.plugins:maven-dependency-plugin:3.11.0
# Separate dependency download so it's cached independently of source changes.
# Run 45456550808: Central answered "Could not find artifact maven-dependency-plugin:jar:3.11.0", a
# 404 for a jar it serves, and `-q` hid what the resolver tried. The transport retry above never
# retries a 404, so the whole fetch is attempted three times. `-B -e` logs every download and the
# resolver's stack trace; `-U` makes a later attempt ask Central again instead of reading the
# not-found the resolver cached in the local repository. After the third failure the build stops
# naming the plugin.
ARG GO_OFFLINE_ATTEMPTS=3
RUN for attempt in $(seq 1 "${GO_OFFLINE_ATTEMPTS}"); do \
        mvn -B -e -U ${DEPENDENCY_PLUGIN}:go-offline && exit 0; \
        echo "zaychik: go-offline attempt ${attempt}/${GO_OFFLINE_ATTEMPTS} failed" >&2; \
        sleep 20; \
    done; \
    echo "zaychik: ${DEPENDENCY_PLUGIN}:go-offline failed after ${GO_OFFLINE_ATTEMPTS} attempts" >&2; \
    exit 1
RUN mvn clean package -DskipTests && mvn ${DEPENDENCY_PLUGIN}:copy-dependencies

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/*.jar /app/zaychik.jar
COPY --from=build /src/target/dependency/ /app/lib/
ENTRYPOINT ["java", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-cp", "/app/zaychik.jar:/app/lib/*", \
    "ru.raiffeisen.trino.arrow.flight.sql.server.FlightSQLTrinoProxy"]
