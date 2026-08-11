// SPDX-License-Identifier: AGPL-3.0-or-later

var builder = DistributedApplication.CreateBuilder(args);

var grafanaConfigDir = Path.Combine(builder.AppHostDirectory, "grafana-config");

// --- Telemetry Stack ---
var prometheus = builder.AddContainer("prometheus", "prom/prometheus")
    .WithEndpoint(port: 9090, targetPort: 9090, name: "http")
    .WithArgs("--enable-feature=otlp-write-receiver");

var tempo = builder.AddContainer("tempo", "grafana/tempo")
    .WithEndpoint(port: 3200, targetPort: 3200, name: "http")
    .WithEndpoint(port: 4318, targetPort: 4318, name: "otlp")
    .WithBindMount(Path.Combine(grafanaConfigDir, "tempo.yml"), "/etc/tempo.yaml", isReadOnly: true)
    .WithArgs("-config.file=/etc/tempo.yaml");

var grafanaAdminPassword = builder.AddParameter("grafana-admin-password", secret: true);

var grafana = builder.AddContainer("grafana", "grafana/grafana")
    .WithEndpoint(port: 3000, targetPort: 3000, name: "http")
    .WithEnvironment("GF_SECURITY_ADMIN_PASSWORD", grafanaAdminPassword)
    .WithEnvironment("PROMETHEUS_URL", prometheus.GetEndpoint("http"))
    .WithEnvironment("TEMPO_URL", tempo.GetEndpoint("http"))
    .WithBindMount(Path.Combine(grafanaConfigDir, "datasources"), "/etc/grafana/provisioning/datasources", isReadOnly: true)
    .WithBindMount(Path.Combine(grafanaConfigDir, "dashboards"), "/etc/grafana/provisioning/dashboards", isReadOnly: true);
// -----------------------

var redis = builder.AddRedis("redis");
var kafka = builder.AddKafka("kafka");

builder.AddProject<Projects.Talaria_Client_Api>("talaria-client")
    .WithReplicas(3)
    .WithReference(redis)
    .WithReference(kafka)
    .WaitFor(redis)
    .WaitFor(kafka)
    .WithEnvironment("GRAFANA_OTLP_METRICS_ENDPOINT", $"{prometheus.GetEndpoint("http")}/api/v1/otlp/v1/metrics")
    .WithEnvironment("GRAFANA_OTLP_TRACES_ENDPOINT", $"{tempo.GetEndpoint("otlp")}/v1/traces");

builder.Build().Run();
