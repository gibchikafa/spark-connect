from kubernetes import config
from kubernetes.client import ApiException

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
            deployment_name: str,
            image: str,
            service_account: str,
            kube_project_user: str
    ):
        self.namespace = namespace
        self.deployment_name = deployment_name
        self.image = image
        self.service_account = service_account
        self.kube_project_user = kube_project_user
        self.labels = {"app": self.deployment_name}
        self.service_name = self.kube_project_user + "--spark-connect-svc"
        self.headless_service_name = self.kube_project_user + "--spark-connect-headless-svc"
        self.spark_connect_server_port = None
        self.spark_connect_server_node_port = None
        self.spark_ui_port = None
        self.spark_ui_node_port = None

    def launch_spark_connect_server_on_k8s(self):

        try:
            if not self.service_exist(self.service_name):
                self.__create_service()
                logger.info(f"Successfully created service for spark-connect server: {self.service_name}")
            self.get_server_port()
            if not self.service_exist(self.headless_service_name):
                self.__create_headless_service()
                logger.info(f"Successfully created service for spark-connect-server: {self.headless_service_name}")
        except client.rest.ApiException as e:
            raise SparkConnectServerCreateError(f"Error launching spark-connect server: {e}")
        logger.info(f"Spark connect server port is : {self.spark_connect_server_port}")
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
                                ]
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
        # Define the Service specification
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
                        port=0,
                        node_port=0,  # will be set automatically
                        target_port=0,
                    ),
                    client.V1ServicePort(
                        name="spark-ui",
                        protocol="TCP",
                        port=0,
                        node_port=0,
                        target_port=0,
                    )
                ],
                selector=self.labels,
                type="NodePort"
            )
        )
        k8s_api.create_namespaced_service(self.namespace, service_manifest)

    def __create_headless_service(self):
        # Define the headless Service specification
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
            raise SparkConnectServerCreateError(f"Failed to get service,{self.service_name}, for spark connect server: {e}")

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



