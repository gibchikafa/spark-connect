__version__ = "0.1.5"

import findspark  # noqa

try:  # noqa
    findspark.init()  # noqa
except Exception:  # noqa
    pass  # noqa
