FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

FROM eclipse-temurin:17-jre
COPY --from=build /src/target/ingest-engine.jar /app/ingest-engine.jar
COPY config.example.yaml /app/config.yaml
EXPOSE 8080 9090 9100
ENTRYPOINT ["java", "-jar", "/app/ingest-engine.jar", "/app/config.yaml"]
