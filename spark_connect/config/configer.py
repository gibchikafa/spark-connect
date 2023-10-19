from __future__ import annotations

import os
import json
from collections import UserDict
from functools import wraps
from typing import Any, Dict, Iterable, Literal, Optional, Tuple, Union

from kubernetes.config.incluster_config import SERVICE_TOKEN_FILENAME

from spark_connect.config.k8s import INCLUSTER, get_k8s_config
from spark_connect.exceptions import AlreadyConfiguredError, UnconfigurableError
from spark_connect.log import logger
from spark_connect.utils import get_host_ip, get_spark_version
from spark_connect.server.k8s_spark_connect_server import SparkConnectServer

ConfigEnvMapper = Dict[str, Tuple[Union[Iterable, str], Optional[str]]]
SPARK_VERSION = get_spark_version() or "3.4.1"  # Assume 3.4.1 if not found

class Config(UserDict):
    def __init__(self, dict=None, /, **kwargs):
        self.master_configured: bool = False
        super().__init__(dict, **kwargs)

    def __setitem__(self, key: Any, item: Any) -> None:
        will_config_master = key in ["spark.master", "spark.remote"]
        if self.master_configured and will_config_master:
            raise AlreadyConfiguredError("Spark master/remote already configured.")
        if will_config_master:
            self.master_configured = True

        return super().__setitem__(key, item)

    def pretty_format(self) -> str:
        lines = ["{"]
        for k, v in self.items():
            if ("key" in k.lower() or "secret" in k.lower()) and "file" not in k.lower():
                v = "******"
            lines.append(f'  "{k}":"{v}"')
        lines += ["}"]
        return "\n".join(lines)

    def __repr__(self) -> str:
        return self.pretty_format()


class SparkEnvConfiger:
    _basic = {
        "spark.app.name": ("SPARK_CONNECT_APP_NAME", "spark-connect"),
        "spark.submit.deployMode": ("SPARK_CONNECT_DEPLOY_MODE", "client"),
        "spark.scheduler.mode": ("SPARK_CONNECT_SCHEDULER_MODE", "FAIR"),
        "spark.ui.port": ("SPARK_CONNECT_UI_PORT", None),
    }
    _s3 = {
        "spark.hadoop.fs.s3a.access.key": (
            ["S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"],
            None,
        ),
        "spark.hadoop.fs.s3a.secret.key": (
            ["S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"],
            None,
        ),
        "spark.hadoop.fs.s3a.endpoint": ("S3_ENTRY_POINT", None),
        "spark.hadoop.fs.s3a.endpoint.region": (
            ["S3_ENTRY_POINT_REGION", "AWS_DEFAULT_REGION"],
            None,
        ),
        "spark.hadoop.fs.s3a.path.style.access": ("S3_PATH_STYLE_ACCESS", None),
        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": (
            "S3_MAGIC_COMMITTER",
            None,
        ),
    }
    _local = {
        "spark.master": ("SPARK_CONNECT_MASTER", "local[*]"),
        "spark.driver.memory": ("SPARK_CONNECT_LOCAL_MEMORY", "512m"),
    }

    _connect_client = {
        "spark.remote": ("SPARK_CONNECT_REMOTE", "sc://localhost:15002"),
    }

    _connect_server = {
        "spark.connect.grpc.binding.port": ("SPARK_CONNECT_SERVER_PORT", None),
        "spark.connect.grpc.arrow.maxBatchSize": (
            "SPARK_CONNECT_CONNECT_GRPC_ARROW_MAXBS",
            None,
        ),
        "spark.connect.grpc.maxInboundMessageSize": (
            "SPARK_CONNECT_CONNECT_GRPC_MAXIM",
            None,
        ),
    }
    _k8s = {
        # Authenticate will auto config by k8s config file
        # May convert by k8s config file(if exsits)
        "spark.master": ("SPARK_CONNECT_MASTER", "k8s://https://kubernetes.default.svc"),
        "spark.kubernetes.namespace": ("SPARK_CONNECT_KUBERNETES_NAMESPACE", None),
        "spark.kubernetes.container.image": (
            "SPARK_CONNECT_KUBERNETES_IMAGE",
            "registry.service.consul:4443/apache/spark:3.5.0",
        ),
        "spark.kubernetes.container.image.pullSecrets": (
            "SPARK_CONNECT_KUBERNETES_IMAGE_PULL_SECRETS",
            None,
        ),
        "spark.kubernetes.container.image.pullPolicy": (
            "SPARK_CONNECT_KUBERNETES_IMAGE_PULL_POLICY",
            "IfNotPresent",
        ),
        "spark.executor.instances": ("SPARK_EXECUTOR_NUMS", "2"),
        "spark.kubernetes.hadoop.configMapName": ("SPARK_CONNECT_KUBERNETES_HADOOP_CONFIG_MAP_NAME", None),
        # Expend list from "a,b" -> [a,b], then map them to {label.a: true, label.b: true}
        "spark.kubernetes.executor.label.list": (
            "SPARK_CONNECT_KUBERNETES_EXECUTOR_LABEL_LIST",
            "spark-connect-executor",
        ),
        "spark.kubernetes.executor.annotation.list": (
            "SPARK_CONNECT_KUBERNETES_EXECUTOR_ANNOTATION_LIST",
            "spark-connect-executor",
        ),
        # INCLUSTER, work with k8s filedRef.fieldPath, see example
        "spark.driver.host": ("SPARK_CONNECT_DRIVER_HOST", None),
        "spark.driver.bindAddress": ("SPARK_CONNECT_DRIVER_BINDADDRESS", "0.0.0.0"),
        "spark.kubernetes.driver.pod.name": ("SPARK_CONNECT_DRIVER_POD_NAME", None),
        "spark.kubernetes.authenticate.driver.serviceAccountName": ("SPARK_CONNECT_DRIVER_SERVICE_ACCOUNT_NAME", "spark-connect"),
        # Config for executor
        "spark.kubernetes.executor.cores": (
            "SPARK_CONNECT_KUBERNETES_EXECUTOR_REQUEST_CORES",
            None,
        ),
        "spark.kubernetes.executor.limit.cores": (
            "SPARK_CONNECT_KUBERNETES_EXECUTOR_LIMIT_CORES",
            None,
        ),
        "spark.executor.memory": ("SPARK_CONNECT_EXECUTOR_REQUEST_MEMORY", "512m"),
        "spark.executor.memoryOverhead": ("SPARK_CONNECT_EXECUTOR_LIMIT_MEMORY", None),
        # GPU
        "spark.executor.resource.gpu.vendor": ("SPARK_CONNECT_KUBERNETES_GPU_VENDOR", "nvidia.com"),
        "spark.executor.resource.gpu.amount": ("SPARK_CONNECT_KUBERNETES_GPU_AMOUNT", None),

    }

    default_config_mapper = {
        **_basic,
        **_s3,
    }

    def __init__(self) -> None:
        self._config: Config
        self.initialize()

    def initialize(self) -> None:
        self._config: Config = Config(self._config_from_env(self.default_config_mapper))
        logger.debug(f"Initialized config: {self._config}")

    @property
    def master_configured(self) -> bool:
        return self._config.master_configured

    def get_all(self) -> Dict[str, str]:
        return {k: v for k, v in self._config.items() if v is not None}


    def _config_from_env(self, mapper: ConfigEnvMapper) -> Dict[str, Any]:
        config = dict()
        for k, (envs, default) in mapper.items():
            if isinstance(envs, str):
                envs = [envs]
            for env in envs:
                v = os.getenv(env)
                if v:
                    break
            v = v or default

            if v:
                config[k] = v

        return config

    def _config_from_json(self) -> Dict[str, Any]:
        HOME_PATH = os.environ.get("SPARKMAGIC_CONF_DIR", "~/.sparkmagic")
        CONFIG_FILE = os.environ.get("SPARKMAGIC_CONF_FILE", "config.json")
        json_file_path  = os.path.join(HOME_PATH, CONFIG_FILE)
        config = dict()
        with open(json_file_path, 'r') as json_file:
            config = json.load(json_file)
        conf = config["session_configs"]["conf"]
        return conf

    def clear(self) -> SparkEnvConfiger:
        logger.info("Reinitialize config and stop SparkSession")
        if self._spark:
            self._spark.stop()
            self._spark = None
        self.initialize()
        return self

    def _merge_config(self, c: Dict[str, Any]) -> None:
        logger.debug(f"Merge config: {c}")
        self._config.update(**c)
        logger.debug(f"Current config: {self._config}")

    def config(self, c: Dict[str, Any]) -> SparkEnvConfiger:
        self._merge_config(c)
        return self

    def config_s3(self, custom_config: Optional[Dict[str, Any]] = None) -> SparkEnvConfiger:
        if not custom_config:
            custom_config = dict()
        return self.config(
            {
                **self._config_from_env(self._s3),
                **custom_config,
            }
        )

    def config_local(self, custom_config: Optional[Dict[str, Any]] = None) -> SparkEnvConfiger:
        logger.info(f"Config master: local mode")
        if not custom_config:
            custom_config = dict()
        return self.config(
            {
                **self._config_from_env(self._local),
                **custom_config,
            }
        )

    def config_k8s(
        self,
        custom_config: Optional[Dict[str, Any]] = None,
        *,
        k8s_config_path: Optional[str] = None,
    ) -> SparkEnvConfiger:
        logger.info(f"Config master: k8s mode")
        if not custom_config:
            custom_config = dict()
        custom_config = {**self._config_from_json(), **custom_config}
        env_config = self._config_from_env(self._k8s)
        if INCLUSTER:
            env_config.setdefault(
                "spark.kubernetes.authenticate.oauthTokenFile", SERVICE_TOKEN_FILENAME
            )
        else:
            env_config.setdefault("spark.driver.host", get_host_ip())

        to_remove_keys = []
        extracted_config = dict()
        for k, v in env_config.items():
            if not k.endswith(".list"):
                continue
            prefix = ".".join(k.split(".")[:-1])
            for item in v.split(","):
                extracted_config[f"{prefix}.{item}"] = "true"
            to_remove_keys.append(k)
        for k in to_remove_keys:
            env_config.pop(k)
        env_config.update(**extracted_config)

        try:
            url, _, ca, key_file, cert_file = get_k8s_config(k8s_config_path)
        except Exception as e:
            logger.exception(e)
            raise UnconfigurableError("Fail to load k8s config")

        k8s_config = {
            "spark.master": f"k8s://{url}",
            "spark.kubernetes.authenticate.caCertFile": ca,
            "spark.kubernetes.authenticate.clientKeyFile": key_file,
            "spark.kubernetes.authenticate.clientCertFile": cert_file,
        }

        logger.debug(f"Env configuration is {env_config}")
        logger.debug(f"K8s configuration is {k8s_config}")
        return self.config(
            {
                **env_config,
                **k8s_config,
                **custom_config,
            }
        )

    def config_connect_client(
        self, custom_config: Optional[Dict[str, Any]] = None
    ) -> SparkEnvConfiger:
        if not custom_config:
            custom_config = dict()
        custom_config = {**self._config_from_json(), **custom_config}

        config = self.config(
            {
                **self._config_from_env(self._connect_client),
                **custom_config,
            }
        )
        deployment_name = "g1--meb1000-spark-connect-server-deployment"
        SparkConnectServer("g1",
                           deployment_name,
                           "registry.service.consul:4443/spark-connect-test:latest",
                           "spark-launcher",
                           "g1--meb1000").launch_spark_connect_server_on_k8s()
        return config

    def config_connect_server(
        self,
        mode: Optional[str] = None,
        custom_config: Optional[Dict[str, Any]] = None,
        *,
        k8s_config_path: Optional[str] = None,
    ) -> SparkEnvConfiger:
        if not custom_config:
            custom_config = dict()

        custom_config = custom_config.update(self._config_from_json())
        if mode:
            logger.info(f"Config master to connect-server via {mode} mode")
            if mode == "local":
                self.config_local(custom_config)
            elif mode == "k8s":
                self.config_k8s(custom_config, k8s_config_path=k8s_config_path)
            else:
                raise UnconfigurableError(f"Unknown mode: {mode}")
        if k8s_config_path and mode != "k8s":
            logger.warning(f"k8s_config_path has no effort for mode: {mode}")
        logger.info(f"Config connect server")
        return self.config(self._config_from_env(self._connect_server))
