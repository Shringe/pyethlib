{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        pythonEnv = pkgs.python313.withPackages (
          pypkgs: with pypkgs; [
            google-cloud-bigquery
            packaging # Needed for bigquery
            whenever
          ]
        );
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.sqlitebrowser
            pythonEnv
          ];
        };

        packages.gen-requirements = pkgs.writeShellScriptBin "gen-requirements" ''
          ${pkgs.python313Packages.pip}/bin/pip freeze --path \
            ${pythonEnv}/lib/python3.13/site-packages \
            > requirements.txt \
            && echo "Written to requirements.txt"
        '';
      }
    );
}
