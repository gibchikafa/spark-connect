FROM apache/spark:3.5.0

USER root

ARG YARNAPP_USER="yarnapp"
ARG YARNAPP_GROUP="hadoop"
RUN groupadd -g 1234 $YARNAPP_GROUP && \
    useradd -m --uid 1235  --gid 1234 $YARNAPP_USER

RUN apt-get update && \
    apt-get install -y wget git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install 'git+https://github.com/gibchikafa/spark-connect@main'
COPY spark-connect-server /usr/local/sbin
RUN chmod +x /usr/local/sbin/spark-connect-server

RUN chown ${YARNAPP_USER}:${YARNAPP_GROUP} /opt/spark/work-dir

USER ${YARNAPP_USER}

ARG SPARK_VERSION=3.5.0

ENTRYPOINT [ "tini" ,"--"]
CMD [ "spark-connect-server", "start" ]
