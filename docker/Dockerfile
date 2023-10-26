FROM registry.service.consul:4443/apache/spark:3.4.1

USER root

ARG YARNAPP_USER="yarnapp"
ARG YARNAPP_GROUP="hadoop"
RUN groupadd -g 1234 $YARNAPP_GROUP && \
    useradd -m --uid 1235  --gid 1234 $YARNAPP_USER


RUN apt-get update && \
    apt-get install -y wget git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget -P /opt/spark/jars https://repo.hops.works/dev/gibson/spark-connect_2.12-3.4.1.jar

RUN pip install 'git+https://github.com/gibchikafa/spark-connect@main'
COPY spark-connect-server /usr/local/sbin
RUN chmod +x /usr/local/sbin/spark-connect-server

RUN chown ${YARNAPP_USER}:${YARNAPP_GROUP} /opt/spark/work-dir

USER ${YARNAPP_USER}

ARG SPARK_VERSION=3.4.1

ENTRYPOINT [ "tini" ,"--"]
CMD [ "spark-connect-server", "start" ]