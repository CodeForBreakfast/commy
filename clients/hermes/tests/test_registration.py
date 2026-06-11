"""Faithful load + register test for the no-op commy Hermes adapter.

Drives the real Hermes registration path — ``PluginContext.register_platform``
→ ``platform_registry`` → ``Platform._missing_`` — with no mocks, mirroring how
the pod loads the plugin. ``hermes-agent`` is provided to the test venv via a
``--no-deps`` install (the host Hermes provides it at pod runtime), so these
assertions exercise real Hermes machinery, not stubbed behaviour.
"""

from gateway.config import Platform, PlatformConfig
from gateway.platform_registry import platform_registry
from gateway.platforms.base import BasePlatformAdapter
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

from commy import register
from commy.adapter import PLATFORM_LABEL, PLATFORM_NAME, CommyAdapter

_POD_ENV = {
    "COMMY_SERVER_DIR": "/srv/commy",
    "ZULIP_SITE": "https://zulip.example",
    "ZULIP_MINTER_EMAIL": "minter@example",
    "ZULIP_MINTER_API_KEY": "secret",
    "COMMY_PROJECT": "commy",
}


def _make_ctx() -> PluginContext:
    manager = PluginManager()
    manifest = PluginManifest(name="commy-platform", kind="platform")
    return PluginContext(manifest, manager)


def test_register_populates_platform_registry():
    register(_make_ctx())

    assert platform_registry.is_registered(PLATFORM_NAME)
    entry = platform_registry.get(PLATFORM_NAME)
    assert entry.label == PLATFORM_LABEL
    assert entry.source == "plugin"


def test_platform_enum_self_extends_after_registration():
    register(_make_ctx())

    platform = Platform(PLATFORM_NAME)
    assert platform.value == PLATFORM_NAME
    # _missing_ caches the pseudo-member, so lookups are identity-stable.
    assert Platform(PLATFORM_NAME) is platform


def test_factory_yields_base_platform_adapter():
    register(_make_ctx())

    entry = platform_registry.get(PLATFORM_NAME)
    adapter = entry.adapter_factory(PlatformConfig())

    assert isinstance(adapter, CommyAdapter)
    assert isinstance(adapter, BasePlatformAdapter)
    assert adapter.platform is Platform(PLATFORM_NAME)


def test_dormant_without_pod_config(monkeypatch):
    """check_fn gates on the pod's commy config: absent → unselectable."""
    for key in _POD_ENV:
        monkeypatch.delenv(key, raising=False)
    register(_make_ctx())

    entry = platform_registry.get(PLATFORM_NAME)
    assert entry.check_fn() is False


def test_active_with_pod_config(monkeypatch):
    """check_fn activates the platform once the pod's commy config is present."""
    for key, value in _POD_ENV.items():
        monkeypatch.setenv(key, value)
    register(_make_ctx())

    entry = platform_registry.get(PLATFORM_NAME)
    assert entry.check_fn() is True
