#!/usr/bin/env bash
SPARK_VERSION=3.5.0
SCALA_VERSION=2.12

/opt/spark/sbin/start-connect-server.sh \
--conf spark.connect.grpc.binding.port=${SPARK_CONNECT_SERVER_PORT}

tail -f /opt/spark/logs/*
