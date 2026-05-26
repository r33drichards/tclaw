{ config, lib, pkgs, ... }:

let
  cfg = config.services.tclaw;
in {
  options.services.tclaw = {
    enable = lib.mkEnableOption "tclaw: durable chat agent (OpenAI Agents SDK + Temporal)";

    package = lib.mkOption {
      type = lib.types.package;
      description = "The tclaw Python package.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "tclaw";
      description = "User to run services as.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "tclaw";
      description = "Group to run services as.";
    };

    temporalAddress = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:7233";
      description = "Temporal frontend address.";
    };

    temporalUiPort = lib.mkOption {
      type = lib.types.port;
      default = 8233;
    };

    taskQueue = lib.mkOption {
      type = lib.types.str;
      default = "chat";
    };

    webhookPort = lib.mkOption {
      type = lib.types.port;
      default = 8787;
    };

    model = lib.mkOption {
      type = lib.types.str;
      default = "gpt-4o";
      description = "OpenAI model to use for chat.";
    };

    titleModel = lib.mkOption {
      type = lib.types.str;
      default = "gpt-4o-mini";
      description = "OpenAI model for title generation.";
    };

    environmentFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      description = ''
        Path to a systemd EnvironmentFile containing OPENAI_API_KEY.
      '';
      example = "/run/secrets/tclaw-openai";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open the webhook port in the firewall.";
    };
  };

  config = lib.mkIf cfg.enable {
    users.users.${cfg.user} = lib.mkIf (cfg.user == "tclaw") {
      isSystemUser = true;
      group = cfg.group;
    };
    users.groups.${cfg.group} = lib.mkIf (cfg.group == "tclaw") {};

    environment.systemPackages = [ pkgs.temporal-cli ];

    networking.firewall.allowedTCPPorts =
      lib.mkIf cfg.openFirewall [ cfg.webhookPort ];

    # --- Redis (streaming fan-out) ---
    services.redis.servers.tclaw = {
      enable = true;
      port = 6379;
      bind = "127.0.0.1";
    };

    # --- Postgres (durable message history) ---
    services.postgresql = {
      enable = true;
      ensureDatabases = [ "chat" ];
      ensureUsers = [
        {
          name = cfg.user;
          ensureClauses.superuser = true;
        }
      ];
    };

    # --- Temporal dev server ---
    systemd.services.tclaw-temporal = {
      description = "Temporal dev server for tclaw";
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];
      serviceConfig = {
        User = cfg.user;
        Group = cfg.group;
        ExecStart = lib.concatStringsSep " " [
          "${pkgs.temporal-cli}/bin/temporal"
          "server start-dev"
          "--ip 127.0.0.1"
          "--port ${toString (lib.toInt (lib.last (lib.splitString ":" cfg.temporalAddress)))}"
          "--ui-port ${toString cfg.temporalUiPort}"
          "--log-level warn"
        ];
        Restart = "always";
        RestartSec = 2;
      };
    };

    # Common environment for worker + webhook
    systemd.services =
      let
        commonEnv = {
          TEMPORAL_ADDRESS = cfg.temporalAddress;
          TEMPORAL_NAMESPACE = "default";
          TASK_QUEUE = cfg.taskQueue;
          REDIS_URL = "redis://127.0.0.1:6379";
          DATABASE_URL = "postgresql:///chat?host=/run/postgresql";
          OPENAI_MODEL = cfg.model;
          OPENAI_TITLE_MODEL = cfg.titleModel;
        };
        commonAfter = [ "tclaw-temporal.service" "network.target" "postgresql.service" "redis-tclaw.service" ];
        commonRequires = [ "tclaw-temporal.service" "postgresql.service" "redis-tclaw.service" ];
      in {
        # --- tclaw worker ---
        tclaw-worker = {
          description = "tclaw Temporal worker (OpenAI Agent streaming activity)";
          after = commonAfter;
          requires = commonRequires;
          wantedBy = [ "multi-user.target" ];
          environment = commonEnv;
          serviceConfig = {
            User = cfg.user;
            Group = cfg.group;
            EnvironmentFile = lib.mkIf (cfg.environmentFile != null) cfg.environmentFile;
            ExecStart = "${cfg.package}/bin/tclaw-worker";
            Restart = "always";
            RestartSec = 3;
          };
        };

        # --- tclaw webhook ---
        tclaw-webhook = {
          description = "tclaw webhook (HTTP -> Temporal signalWithStart)";
          after = commonAfter;
          requires = commonRequires;
          wantedBy = [ "multi-user.target" ];
          environment = commonEnv // {
            WEBHOOK_PORT = toString cfg.webhookPort;
          };
          serviceConfig = {
            User = cfg.user;
            Group = cfg.group;
            EnvironmentFile = lib.mkIf (cfg.environmentFile != null) cfg.environmentFile;
            ExecStart = "${cfg.package}/bin/tclaw-webhook";
            Restart = "always";
            RestartSec = 3;
          };
        };
      };
  };
}
