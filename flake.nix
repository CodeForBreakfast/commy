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
    in
    {
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
