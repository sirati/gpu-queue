{
  description = "Rust GPU queue, daemon and cooperative Python leases";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forSystems = nixpkgs.lib.genAttrs systems;
    in {
      overlays.default = final: prev: {
        gpu-queue = final.callPackage ./package.nix { };
        pythonPackagesExtensions = prev.pythonPackagesExtensions ++ [
          (pythonFinal: pythonPrev: {
            gpu-queue = pythonFinal.callPackage ./python-package.nix {
              gpu-queue = final.gpu-queue;
            };
          })
        ];
      };
      packages = forSystems (system:
        let pkgs = import nixpkgs { inherit system; };
        in rec {
          gpu-queue = pkgs.callPackage ./package.nix { };
          python = pkgs.python3Packages.callPackage ./python-package.nix { inherit gpu-queue; };
          default = gpu-queue;
        });
      devShells = forSystems (system:
        let pkgs = import nixpkgs { inherit system; };
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [ cargo rustc rustfmt clippy gcc pkg-config maturin
              (python3.withPackages (ps: [ ps.pytest ])) ];
          };
        });
    };
}
