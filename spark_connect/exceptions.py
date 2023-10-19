class UnconfigurableError(Exception):
    ...


class DaemonError(Exception):
    ...


class AlreadyConfiguredError(UnconfigurableError):
    ...


class SparkConnectServerCreateError(Exception):
    ...
