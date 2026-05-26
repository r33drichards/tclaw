{
  description = "tclaw – durable chat agent (OpenAI Agents SDK + Temporal)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      # NixOS module is system-independent
      nixosModules.default = import ./nix/module.nix;
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        python = pkgs.python312;

        # Python dependencies not in nixpkgs or needing overrides
        pythonOverrides = final: prev: {
          openai-agents = final.buildPythonPackage rec {
            pname = "openai-agents";
            version = "0.2.9";
            format = "pyproject";
            src = final.fetchPypi {
              pname = "openai_agents";
              inherit version;
              hash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
            };
            build-system = [ final.hatchling ];
            dependencies = with final; [ openai pydantic ];
            doCheck = false;
          };
        };

        pythonPkgs = python.override {
          packageOverrides = pythonOverrides;
        };

        tclaw = pythonPkgs.pkgs.buildPythonApplication {
          pname = "tclaw";
          version = "0.1.0";
          format = "pyproject";
          src = ./.;

          build-system = [ pythonPkgs.pkgs.hatchling ];

          dependencies = with pythonPkgs.pkgs; [
            temporalio
            openai-agents
            fastapi
            uvicorn
            sse-starlette
            redis
            asyncpg
            pydantic
            httpx
          ];

          doCheck = false;

          meta = {
            description = "Durable chat agent powered by OpenAI Agents SDK and Temporal";
            mainProgram = "tclaw-webhook";
          };
        };

      in {
        packages = {
          default = tclaw;
          inherit tclaw;
        };

        devShells.default = pkgs.mkShell {
          packages = [
            (python.withPackages (ps: with ps; [
              temporalio
              fastapi
              uvicorn
              sse-starlette
              redis
              asyncpg
              pydantic
              httpx
              pytest
              pytest-asyncio
            ]))
            pkgs.temporal-cli
            pkgs.redis
            pkgs.postgresql
          ];

          shellHook = ''
            echo "tclaw dev shell — Python ${python.version}"
            echo "  temporal server start-dev   # start Temporal"
            echo "  python -m tclaw.worker      # start worker"
            echo "  python -m tclaw.webhook     # start webhook"
          '';
        };
      }
    ) // {
      inherit nixosModules;
    };
}
