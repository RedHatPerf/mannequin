FROM registry.access.redhat.com/ubi8/ubi:latest
ADD target/mannequin-1.0-SNAPSHOT-runner /mannequin
ADD target/mannequin-1.0-SNAPSHOT-runner.debug /tmp/mannequin
#RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
RUN chown 1001 /mannequin
USER 1001
EXPOSE 8080/tcp 5432/tcp
ENTRYPOINT [ "/mannequin" ]

