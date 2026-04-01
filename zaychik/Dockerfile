FROM maven:3.9-eclipse-temurin-17 AS build
RUN git clone https://github.com/Raiffeisen-DGTL/zaychik-trino-proxy.git /src
WORKDIR /src

# Fix: decodePeerIdentity crashes when password is empty (ArrayIndexOutOfBoundsException)
# The token is base64("user:password") — when no password, split produces length 1
COPY fix-decode-peer-identity.patch /tmp/
RUN git apply /tmp/fix-decode-peer-identity.patch || \
    sed -i 's/return new ImmutablePair<>(parts\[0\], parts\[1\]);/String password = parts.length > 1 ? parts[1] : ""; return new ImmutablePair<>(parts[0], password);/' \
    src/main/java/ru/raiffeisen/trino/arrow/flight/sql/producer/FlightSQLProducerHelper.java

RUN mvn clean package -DskipTests && mvn dependency:copy-dependencies

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/*.jar /app/zaychik.jar
COPY --from=build /src/target/dependency/ /app/lib/
ENTRYPOINT ["java", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "-cp", "/app/zaychik.jar:/app/lib/*", \
    "ru.raiffeisen.trino.arrow.flight.sql.server.FlightSQLTrinoProxy"]
