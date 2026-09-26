"""Native Nutmeg extension metadata and a small Spark Connect client.

The host reads manifest() before importing any native FFI object.
"""
TYPE_URL = "type.googleapis.com/nutmeg.v1.NutmegApi"


class Extension:
    def manifest(self):
        return {
            "name": "nutmeg",
            "version": "0.1.0",
            "api_version": 1,
            "datafusion_version": "55.1.0",
            "arrow_version": "59.3.0",
            "placement": "driver",
            "relation_types": [{
                "type_url": TYPE_URL,
                "accepts_bare": True,
                "min_inputs": 0,
                "max_inputs": 2,
            }],
        }

    def bind(self, session_id):
        from ._native import BoundExtension
        # Each bind allocates fresh state; a recycled session ID inherits nothing.
        return BoundExtension()


def extension():
    return Extension()


def __getattr__(name):
    if name == "Nutmeg":
        from .client import Nutmeg
        return Nutmeg
    raise AttributeError(name)
