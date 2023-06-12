import os
from mkite_core.external import load_config

from .base import EngineError, EngineRoles


def get_engine_class(module: str, role: EngineRoles):
    """Obtains the engine class given by `module` and `role`.
    For example, obtaining the class for a LocalProducer
    would requiring passing the following arguments:

        module="mkite_engines.local", role="producer"

    Arguments:
        module (str): namespace of the engine module
        role (str): whether the role of the engine is producer or consumer.

    Returns:
        engine: class of type EngineRole
    """
    engine = module.split(".")[-1]
    clsname = engine.capitalize() + role.value.capitalize()

    _module = __import__(module, globals(), locals(), [clsname], 0)
    if not hasattr(_module, clsname):
        raise EngineError(f"{clsname} does not exist in module {module}")

    _cls = getattr(_module, clsname)
    return _cls


def instantiate_from_dict(settings: dict, role: EngineRoles, **kwargs):
    _module_key: str = "_module"
    _settings = {
        **settings,
        **kwargs,
    }
    if _module_key not in settings:
        raise EngineError(
            "Engine settings do not specify which engine to use."
            + f"Please specify a `{_module_key}` for the engine."
        )

    module = _settings.pop(_module_key)

    cls = get_engine_class(module, role)

    return cls(**_settings)


def instantiate_from_path(path: os.PathLike, role: EngineRoles, **kwargs):
    data = load_config(path)
    return instantiate_from_dict(data, role, **kwargs)
