__version__ = "0.1.0"

import findspark  # noqa

try:  # noqa
    findspark.init()  # noqa
except Exception:  # noqa
    pass  # noqa
