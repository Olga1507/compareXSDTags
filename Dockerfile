FROM openjdk:17.0.2
WORKDIR /opt/srv
COPY ./target/compareXSDTags-0.0.1-SNAPSHOT.jar /opt/srv

EXPOSE 8080
CMD ["java", "-jar", "/opt/srv/compareXSDTags-0.0.1-SNAPSHOT.jar"]