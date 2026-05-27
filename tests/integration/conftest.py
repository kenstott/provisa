import pytest


@pytest.fixture(autouse=True, scope="module")
def _reset_app_state():
    """Reset global app state between test modules.

    The module-level `state` singleton in provisa.api.app accumulates
    auth_config from whichever module-scoped lifespan fixture ran last.
    Resetting before each module ensures create_app() sees a clean slate.
    """
    from provisa.api import app as _app_module

    _app_module.state.auth_config = None
    yield
    _app_module.state.auth_config = None
