FROM maven:3.9-eclipse-temurin-17 AS build
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
#    Arrow lacks (map, row, array, json, uuid, ipaddress, intervals, time with time zone) is Utf8
#    holding the protocol's text/JSON instead of failing as an unmapped JDBC type;
#  - source, catalog, schema and session properties from the x-provisa-trino-context call header;
#  - Trino's error name/type/code and query id in the Flight error's trailers.
# The statement runs at GetFlightInfo up to its columns; DoGet streams the rows.
COPY trino-statement-protocol.patch /tmp/
RUN git apply /tmp/trino-statement-protocol.patch

# Maven's HTTP transport retries a request three times and only on 429 and 503. Maven Central
# answering one POM with 502 Bad Gateway therefore failed the whole image build on the first try.
# A gateway error is retried too, five times, with the transport's own backoff.
ENV MAVEN_ARGS="-Daether.connector.http.retryHandler.count=5 -Daether.connector.http.retryHandler.serviceUnavailable=429,502,503,504"

# Separate dependency download so it's cached independently of source changes
RUN mvn dependency:go-offline -q
RUN mvn clean package -DskipTests && mvn dependency:copy-dependencies

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/*.jar /app/zaychik.jar
COPY --from=build /src/target/dependency/ /app/lib/
ENTRYPOINT ["java", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-cp", "/app/zaychik.jar:/app/lib/*", \
    "ru.raiffeisen.trino.arrow.flight.sql.server.FlightSQLTrinoProxy"]
