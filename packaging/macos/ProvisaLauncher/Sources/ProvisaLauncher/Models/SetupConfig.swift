import Foundation

/// Federation engine the wizard offers. `duckdb` is the native default; `trino` spins up the
/// Docker engine; `external` connects to an operator-supplied engine URL / host+port.
enum FederationEngineChoice: String {
    case duckdb, trino, external
}

/// Observability integration (obs is always-on built-in; these only redirect OTLP export).
/// `none` keeps telemetry in-app; `docker` runs the bundled collector+prometheus+grafana demo;
/// `collector` points at an existing OTLP collector.
enum ObsMode: String {
    case none, docker, collector
}

final class SetupConfig: ObservableObject {
    @Published var installDir: URL = FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent(".provisa")
    @Published var hostname: String = "localhost"
    @Published var uiPort: String = "3000"
    @Published var apiPort: String = "8000"
    @Published var flightPort: String = "8815"

    // ── Deployment (REQ-972..979) — default to the self-contained native tier ──
    @Published var engine: FederationEngineChoice = .duckdb
    /// External engine DSN (engine == .external), e.g. postgresql+psycopg://…
    @Published var engineUrl: String = ""
    /// External materialization-store DSN (optional).
    @Published var materializeUrl: String = ""
    /// Trino coordinator host/port — for engine == .external pointing at a Trino, or a chosen host.
    @Published var trinoHost: String = ""
    @Published var trinoPort: String = ""
    @Published var obsMode: ObsMode = .none
    /// OTLP collector endpoint (obsMode == .collector).
    @Published var otlpEndpoint: String = ""
    @Published var installDemo: Bool = false

    /// True when the chosen deployment runs on the user's Docker (Trino engine or Docker obs).
    var needsDocker: Bool {
        engine == .trino || obsMode == .docker
    }

    /// The federation engine id passed to the runtime (external → sqlalchemy driver via engineUrl).
    private var engineId: String {
        switch engine {
        case .duckdb:   return "duckdb"
        case .trino:    return "trino"
        case .external: return "sqlalchemy"
        }
    }

    /// Compose scale for the Docker tier's federation workers.
    var federationWorkers = 0

    /// Environment variables forwarded to first-launch.sh in non-interactive mode.
    var environment: [String: String] {
        var env: [String: String] = [
            "PROVISA_NONINTERACTIVE": "1",
            "PROVISA_INSTALL_DIR":    installDir.path,
            "PROVISA_WORKERS":        "\(federationWorkers)",
            "PROVISA_HOSTNAME":       hostname,
            "PROVISA_UI_PORT":        uiPort,
            "PROVISA_API_PORT":       apiPort,
            "PROVISA_FLIGHT_PORT":    flightPort,
            // Deployment (first-launch.sh:resolve_deployment)
            "PROVISA_ENGINE":         engineId,
            "PROVISA_OBS_MODE":       obsMode.rawValue,
            "PROVISA_INSTALL_DEMO":   installDemo ? "y" : "n",
            "PROVISA_DEMO_MODE":      "native",
        ]
        if !engineUrl.isEmpty       { env["PROVISA_ENGINE_URL"]      = engineUrl }
        if !materializeUrl.isEmpty  { env["PROVISA_MATERIALIZE_URL"] = materializeUrl }
        if !trinoHost.isEmpty       { env["PROVISA_TRINO_HOST"]      = trinoHost }
        if !trinoPort.isEmpty       { env["PROVISA_TRINO_PORT"]      = trinoPort }
        if obsMode == .collector && !otlpEndpoint.isEmpty {
            env["PROVISA_OTLP_ENDPOINT"] = otlpEndpoint
        }
        return env
    }
}
