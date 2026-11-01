# -*- coding: utf-8 -*-

# Ensure can import paho package
try:
    import paho

except ImportError:
    # This part is only required to run the examples from within the examples
    # directory when the module itself is not installed.
    import inspect
    import os
    import sys

    cmd_subfolder = os.path.realpath(
        os.path.abspath(
            os.path.join(
                os.path.split(
                    inspect.getfile(inspect.currentframe())
                )[0],
                "..",
                "src"
            )
        )
    )
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)

    import paho
