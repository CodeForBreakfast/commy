{
  description = "commy plugin — substrate-agnostic agent communications (ass-15qi, ass-x09b)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f {
            inherit system;
            pkgs = nixpkgs.legacyPackages.${system};
          }
        );

      # Single source of truth for the plugin version — read from its pyproject
      # so the flake and the package never drift.
      pluginVersion =
        (builtins.fromTOML (builtins.readFile ./clients/hermes/pyproject.toml)).project.version;

      # The Hermes platform plugin (clients/hermes), built straight from the
      # monorepo subdir as a pyproject package that exposes the
      # `hermes_agent.plugins` entry point. Consumers add it to
      # `services.hermes-agent.extraPythonPackages`; Hermes then discovers it via
      # importlib.metadata and `hermes plugins enable commy-platform` activates
      # it. `gateway` / `BasePlatformAdapter` come from the host Hermes at
      # runtime, so there are no runtime deps and the Nix build neither imports
      # nor tests the module — the repo gate (clients/hermes/scripts/test.sh)
      # does that against a real `hermes-agent`.
      commyHermesPlugin =
        pythonPackages:
        pythonPackages.buildPythonPackage {
          pname = "commy-hermes";
          version = pluginVersion;
          pyproject = true;
          src = nixpkgs.lib.cleanSourceWith {
            src = ./clients/hermes;
            filter =
              path: _type:
              let
                base = baseNameOf path;
              in
              !(
                builtins.elem base [
                  ".venv"
                  "dist"
                  "build"
                  "__pycache__"
                  ".pytest_cache"
                  ".ruff_cache"
                ]
                || nixpkgs.lib.hasSuffix ".pyc" base
              );
          };
          build-system = [ pythonPackages.setuptools ];
          doCheck = false;
        };
    in
    {
      packages = forAllSystems (
        { pkgs, ... }:
        rec {
          commy-hermes = commyHermesPlugin pkgs.python3Packages;
          default = commy-hermes;
        }
      );

      # Adds `commy-hermes` to every Python package set, so a consumer can build
      # it against the same Python as their `hermes-agent` (version-matched) and
      # pass it to `services.hermes-agent.extraPythonPackages`.
      overlays.default = _final: prev: {
        pythonPackagesExtensions = (prev.pythonPackagesExtensions or [ ]) ++ [
          (pyfinal: _pyprev: { commy-hermes = commyHermesPlugin pyfinal; })
        ];
      };

      devShells = forAllSystems (
        { pkgs, ... }:
        let
          # Tooling the gate needs: bun runs the TS gate, uv drives the
          # clients/hermes Python gate (//#test:hermes → scripts/test.sh).
          # Shared by both shells so `bun run check`, the pre-commit hook,
          # and CI resolve identical tools rather than a global PATH install.
          gateTools = [
            pkgs.bun
            pkgs.uv
          ];
        in
        {
          default = pkgs.mkShell {
            packages = gateTools ++ [ pkgs.typescript-language-server ];
          };
          # Lean gate shell for CI: exactly gateTools, no interactive extras
          # (no language server). CI runs
          # `nix develop .#ci --command bun run check`. Mirrors brewlife's
          # dedicated .#ci shell so CI carries only what the gate needs.
          ci = pkgs.mkShell {
            packages = gateTools;
          };
        }
      );
    };
}
