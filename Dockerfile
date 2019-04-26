FROM openjdk:8-jre-alpine
WORKDIR /work
RUN chmod 777 /work
COPY ./target/mannequin-0.1-SNAPSHOT-fat.jar /work/application.jar
EXPOSE 8080 5432
ENV JAVA_OPTS "$JAVA_OPTS -Djava.net.preferIPv4Stack=true -Xmx64m"
ENTRYPOINT java ${JAVA_OPTS} -cp /work/application -jar /work/application.jar

