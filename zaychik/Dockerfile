FROM maven:3.9-eclipse-temurin-17 AS build
RUN git clone https://github.com/Raiffeisen-DGTL/zaychik-trino-proxy.git /src
WORKDIR /src
RUN mvn clean package -DskipTests && mvn dependency:copy-dependencies

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/*.jar /app/zaychik.jar
COPY --from=build /src/target/dependency/ /app/lib/
ENTRYPOINT ["java", "-cp", "/app/zaychik.jar:/app/lib/*", "ru.raiffeisen.trino.arrow.flight.sql.server.FlightSQLTrinoProxy"]
