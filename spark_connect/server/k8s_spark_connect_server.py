import json
import os
from collections import UserDict
from typing import Dict, Any

from kubernetes import config
from kubernetes.client import ApiException

from spark_connect.constants import SPARK_CONFIG_DEFAULT_DIR, SPARK_CONFIG_FILE_NAME, \
    SPARK_CONNECT_SERVER_SERVICE_SUFFIX, SPARK_CONNECT_SERVER_HEADLESS_SERVICE_SUFFIX, \
    SPARK_CONNECT_SERVER_SPARK_CONFIG_MAP_SUFFIX, SPARK_CONNECT_SERVER_DEPLOYMENT_SUFFIX, \
    SPARK_CONNECT_SERVER_PORT_INTERNAL, SPARK_UI_SERVER_PORT_INTERNAL, SPARK_CONNECT_SERVER_CPU, \
    SPARK_CONNECT_SERVER_MEMORY
from spark_connect.exceptions import SparkConnectServerCreateError
from spark_connect.log import logger

config.load_incluster_config()
from kubernetes import client

configuration = client.Configuration().get_default_copy()
configuration.verify_ssl = False  # Disable SSL verification
config.load_incluster_config()
client.Configuration.set_default(configuration)
k8s_api = client.CoreV1Api()
aps_api = client.AppsV1Api()


class SparkConnectServer:
    def __init__(
            self,
            namespace: str,
            image: str,
            service_account: str,
            kube_project_user: str,
            spark_config: UserDict,
    ):
        self.namespace = namespace
        self.image = image
        self.service_account = service_account
        self.kube_project_user = kube_project_user
        self.deployment_name = kube_project_user + SPARK_CONNECT_SERVER_DEPLOYMENT_SUFFIX
        self.labels = {"app": self.deployment_name}
        self.service_name = self.kube_project_user + SPARK_CONNECT_SERVER_SERVICE_SUFFIX
        self.headless_service_name = self.kube_project_user + SPARK_CONNECT_SERVER_HEADLESS_SERVICE_SUFFIX
        self.spark_config_configmap_name = self.kube_project_user + SPARK_CONNECT_SERVER_SPARK_CONFIG_MAP_SUFFIX
        self.spark_connect_server_port = SPARK_CONNECT_SERVER_PORT_INTERNAL
        self.spark_connect_server_node_port = None
        self.spark_ui_port = SPARK_UI_SERVER_PORT_INTERNAL
        self.spark_ui_node_port = None
        self.spark_configuration = dict(spark_config)
        self.__prepare_spark_config()

    def start(self):

        try:
            if not self.service_exist(self.service_name):
                self.__create_service()
                logger.info(f"Successfully created service for spark-connect server: {self.service_name}")
            self.get_server_port()
            if not self.service_exist(self.headless_service_name):
                self.__create_headless_service()
                logger.info(f"Successfully created service for spark-connect-server: {self.headless_service_name}")

            final_config = {"session_configs": {"conf": self.spark_configuration}}
            final_config["session_configs"] = json.dumps(final_config["session_configs"])
            self.__create_or_replace_config_map(self.spark_config_configmap_name, final_config)

        except client.rest.ApiException as e:
            raise SparkConnectServerCreateError(f"Error launching spark-connect server: {e}")

        if self.deployment_exist():
            # should we delete the deployment??
            self.__remove_connect_server()

        deployment_manifest = client.V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(
                name=self.deployment_name,
                namespace=self.namespace,
                labels=self.labels
            ),
            spec=client.V1DeploymentSpec(
                replicas=1,
                selector=client.V1LabelSelector(
                    match_labels=self.labels
                ),
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels=self.labels
                    ),
                    spec=client.V1PodSpec(
                        service_account_name=self.service_account,
                        containers=[
                            client.V1Container(
                                name="spark-connect-server",
                                image=self.image,
                                ports=[client.V1ContainerPort(container_port=int(self.spark_connect_server_port))],
                                image_pull_policy="IfNotPresent",
                                env=[
                                    client.V1EnvVar(name="SPARK_CONNECT_SERVER_MODE", value="k8s"),
                                    client.V1EnvVar(
                                        name="SPARK_CONNECT_DRIVER_HOST",
                                        value_from=client.V1EnvVarSource(
                                            field_ref=client.V1ObjectFieldSelector(field_path="status.podIP")
                                        ),
                                    ),
                                    client.V1EnvVar(
                                        name="SPARK_CONNECT_DRIVER_POD_NAME",
                                        value_from=client.V1EnvVarSource(
                                            field_ref=client.V1ObjectFieldSelector(field_path="metadata.name")
                                        ),
                                    ),
                                    client.V1EnvVar(
                                        name="SPARK_CONNECT_SERVER_PORT",
                                        value=str(self.spark_connect_server_port),
                                    ),
                                    client.V1EnvVar(
                                        name="SPARK_CONNECT_KUBERNETES_CONFIG_DIR",
                                        value=SPARK_CONFIG_DEFAULT_DIR,
                                    ),
                                    client.V1EnvVar(
                                        name="SPARK_CONNECT_KUBERNETES_CONFIG_FILE",
                                        value=SPARK_CONFIG_FILE_NAME,
                                    ),
                                    client.V1EnvVar(
                                        name="SPARK_CONNECT_KUBERNETES_SERVICE_ACCOUNT_NAME",
                                        value=self.service_account
                                    )
                                ],
                                volume_mounts=[
                                    client.V1VolumeMount(
                                        name="spark-config-volume",
                                        mount_path=SPARK_CONFIG_DEFAULT_DIR,
                                        read_only=True
                                    )
                                ],
                                resources=self.__get_resource_requests()
                            )
                        ],
                        volumes=[
                            client.V1Volume(
                                name="spark-config-volume",
                                config_map=client.V1ConfigMapVolumeSource(name=self.spark_config_configmap_name)
                            )
                        ]
                    )
                )
            )
        )

        try:
            aps_api.create_namespaced_deployment(self.namespace, deployment_manifest, pretty=True)
            logger.info(f"Connect server deployment: {self.deployment_name} created successfully.")
        except client.rest.ApiException as e:
            raise SparkConnectServerCreateError(f"Error launching spark-connect server: {e}")

        return self

    def __create_service(self):
        service_manifest = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(
                name=self.service_name,
                namespace=self.namespace
            ),
            spec=client.V1ServiceSpec(
                ports=[
                    client.V1ServicePort(
                        name="connect-server",
                        protocol="TCP",
                        port=self.spark_connect_server_port,
                        node_port=0,  # will be set automatically
                        target_port=self.spark_connect_server_port,
                    ),
                    client.V1ServicePort(
                        name="spark-ui",
                        protocol="TCP",
                        port=self.spark_ui_port,
                        node_port=0,
                        target_port=self.spark_ui_port,
                    )
                ],
                selector=self.labels,
                type="NodePort"
            )
        )
        k8s_api.create_namespaced_service(self.namespace, service_manifest)

    def __create_headless_service(self):
        service_manifest = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(
                name=self.headless_service_name,
                namespace=self.namespace
            ),
            spec=client.V1ServiceSpec(
                cluster_ip="None",  # Setting the cluster IP to "None" makes it headless
                selector=self.labels
            )
        )
        k8s_api.create_namespaced_service(self.namespace, service_manifest)

    def __create_or_replace_config_map(self, config_map_name, data: Any):
        if self.config_map_exist(self.spark_config_configmap_name):
            try:
                k8s_api.delete_namespaced_config_map(config_map_name, self.namespace)
            except ApiException as e:
                logger.error(f"Failed to delete existing config map {self.spark_config_configmap_name}. {e}")

        config_map = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=client.V1ObjectMeta(name=config_map_name, namespace=self.namespace),
            data=data
        )
        try:
            k8s_api.create_namespaced_config_map(self.namespace, body=config_map)
        except ApiException as e:
            logger.error(f"Failed to create config map {self.spark_config_configmap_name}. {e}")
            raise
        logger.info(f"Successfully created spark config map for"
                    f" spark-connect-server: {self.spark_config_configmap_name}")

    def get_server_port(self):
        try:
            service = k8s_api.read_namespaced_service(self.service_name, self.namespace)
            for port in service.spec.ports:
                if port.name == "connect-server":
                    self.spark_connect_server_port = port.target_port
                    self.spark_connect_server_node_port = port.node_port
                    break
            return {"node_port": self.spark_connect_server_node_port, "port": self.spark_connect_server_port}
        except ApiException as e:
            logger.error(f"Failed to get service for spark connect server: {e}")
            raise SparkConnectServerCreateError(f"Failed to get service,{self.service_name}, "
                                                f"for spark connect server: {e}")

    def config_map_exist(self, config_map_name) -> bool:
        try:
            k8s_api.read_namespaced_config_map(config_map_name, self.namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise ApiException(f"Error reading config map {config_map_name}. {e}")

    def service_exist(self, service_name: str) -> bool:
        try:
            k8s_api.read_namespaced_service(service_name, self.namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise ApiException(f"Error reading service {service_name}. {e}")

    def deployment_exist(self):
        try:
            aps_api.read_namespaced_deployment(self.deployment_name, self.namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise ApiException(f"Error reading deployment {self.deployment_name}. {e}")

    def __get_resource_requests(self):
        resources = client.V1ResourceRequirements(
            requests={"cpu": self.spark_configuration["spark.driver.cores"] + SPARK_CONNECT_SERVER_CPU,
                      "memory": self.__get_memory_request()},
        )
        return resources


    def __prepare_spark_config(self):
        # for testing since we are using non-hopsworks image and these might break launching the executor
        configs_to_remove = ["spark.executorEnv.HADOOP_HOME", "spark.executorEnv.HADOOP_HDFS_HOME",
                             "spark.executor.extraJavaOptions", "spark.executor.extraClassPath",
                             "spark.driver.extraClassPath", "spark.driver.extraJavaOptions",
                             "spark.executorEnv.SPARK_CONF_DIR", "spark.executorEnv.LIBHDFS_OPTS",
                             "spark.executorEnv.SPARK_CONF_DIR", "spark.executor.resource.gpu.vendor",
                             "spark.master", "spark.remote"]
        for key in configs_to_remove:
            try:
                self.spark_configuration.pop(key)
            except KeyError as e:
                pass
        # remove executor envs
        exec_env_configs = []
        for key in self.spark_configuration:
            if "spark.executorEnv" in key:
                exec_env_configs.append(key)
        for key in exec_env_configs:
            try:
                self.spark_configuration.pop(key)
            except KeyError as e:
                pass
        # make sure it is indeed a spark 3.5.0 image
        self.spark_configuration["spark.kubernetes.container.image"] = "registry.service.consul:4443/apache/spark:3.4.1"

    def __remove_connect_server(self):
        try:
            aps_api.delete_namespaced_deployment(self.deployment_name, self.namespace)
        except client.rest.ApiException as e:
            raise SparkConnectServerCreateError(f"Error launching spark-connect server. Could not delete the "
                                                f"existing deployment {self.deployment_name}: {e}")
        wait_timeout = 30
        trials = 1
        while True:
            if not self.deployment_exist():
                return
            else:
                if trials == wait_timeout:
                    raise SparkConnectServerCreateError("Timeout out to remove existing spark connect server")
            trials = trials + 1

    def __get_memory_request(self):
        memory = self.spark_configuration["spark.driver.memory"].upper().replace("M", "")
        return str(float(memory) + float(SPARK_CONNECT_SERVER_MEMORY)) + "m"




