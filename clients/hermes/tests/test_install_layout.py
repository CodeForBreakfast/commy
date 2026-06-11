"""The plugin is discoverable + loadable via Hermes's entry-point mechanism.

Hermes finds pip/Nix-installed plugins by scanning the ``hermes_agent.plugins``
entry-point group with ``importlib.metadata.entry_points`` and loading each via
``ep.load()`` → ``register(ctx)`` (see hermes_cli/plugins.py
``_scan_entry_points`` / ``_load_entrypoint_module``). This is the documented
*recommended* distribution path: the package declares the entry point in
``pyproject.toml`` and is consumed via ``services.hermes-agent.extraPythonPackages``
(NixOS) or pip. ``hermes plugins enable commy-platform`` then activates it.

Requires the package installed in the environment (``scripts/test.sh``'s
``uv sync`` builds it) so the entry point is present in ``importlib.metadata``.
"""

import importlib.metadata

ENTRY_POINT_GROUP = "hermes_agent.plugins"
ENTRY_POINT_NAME = "commy-platform"


def _commy_entry_point() -> importlib.metadata.EntryPoint | None:
    eps = importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)
    return next((ep for ep in eps if ep.name == ENTRY_POINT_NAME), None)


def test_plugin_advertises_hermes_entry_point():
    ep = _commy_entry_point()
    assert ep is not None, (
        f"no '{ENTRY_POINT_NAME}' entry point in group '{ENTRY_POINT_GROUP}'; "
        "Hermes discovers entry-point plugins via importlib.metadata, so the "
        "package must declare it in pyproject.toml"
    )


def test_entry_point_loads_and_exposes_register():
    ep = _commy_entry_point()
    assert ep is not None
    loaded = ep.load()
    register = getattr(loaded, "register", None)
    assert callable(register), (
        "the entry-point target must expose a callable register(ctx) — "
        "Hermes loads the plugin with ep.load() then calls "
        "getattr(module, 'register')(ctx)"
    )
