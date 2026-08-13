import SwiftUI

/// Deployment step (REQ-972..979): pick the federation engine and optional
/// observability integration. The demo decision is a separate upfront step
/// (DemoView). Everything defaults to the self-contained Embedded Desktop tier
/// (no Docker).
struct DeploymentView: View {
    @ObservedObject var config: SetupConfig
    let onBack: () -> Void
    let onNext: () -> Void

    var body: some View {
        VStack(spacing: 0) {
            stepHeader(title: "Deployment",
                       subtitle: "Choose the query engine and optional add-ons")

            Spacer()

            VStack(alignment: .leading, spacing: 20) {
                // ── Federation engine ──
                Text("Federation engine")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.white.opacity(0.5))

                Picker("", selection: $config.engine) {
                    Text("Embedded Desktop (recommended)").tag(FederationEngineChoice.duckdb)
                    Text("Federation Engine - Docker").tag(FederationEngineChoice.trino)
                    Text("External engine").tag(FederationEngineChoice.external)
                }
                .pickerStyle(.radioGroup)
                .labelsHidden()

                if config.engine == .external {
                    TextField("postgresql+psycopg://user:pass@host:5432/db", text: $config.engineUrl)
                        .wizardField()
                    TextField("Materialization store URL (optional)", text: $config.materializeUrl)
                        .wizardField()
                }
                if config.needsDocker {
                    Label("Runs on your Docker (Docker Desktop or colima) — make sure Docker is installed and running.",
                          systemImage: "shippingbox")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.55))
                }

                Divider().background(.white.opacity(0.12))

                // ── Observability integration (obs is always-on built-in) ──
                Text("Observability integration")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.white.opacity(0.5))

                Picker("", selection: $config.obsMode) {
                    Text("Built-in only").tag(ObsMode.none)
                    Text("Bundled Grafana/Prometheus demo (Docker)").tag(ObsMode.docker)
                    Text("Export to my collector").tag(ObsMode.collector)
                }
                .pickerStyle(.radioGroup)
                .labelsHidden()

                if config.obsMode == .collector {
                    TextField("http://collector-host:4317", text: $config.otlpEndpoint)
                        .wizardField()
                }

                Divider().background(.white.opacity(0.12))

                // ── Data quality checker (REQ-1443) — parity with install.sh's prompt ──
                // The checker runs as a separate process aimed at Provisa's own pgwire endpoint;
                // its scan results land as ordinary source rows. Nothing is installed by default.
                Text("Data quality checker (optional)")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.white.opacity(0.5))

                Picker("", selection: $config.dqChecker) {
                    Text("None").tag(DqChecker.none)
                    Text("Soda Core — contracts (Elastic License 2.0; self-hosted only)")
                        .tag(DqChecker.soda)
                    Text("Great Expectations — suites (Apache 2.0)").tag(DqChecker.gx)
                }
                .pickerStyle(.radioGroup)
                .labelsHidden()
            }
            .padding(.horizontal, 60)

            Spacer()

            navButtons(onBack: onBack, onNext: onNext, nextLabel: "Continue")
        }
    }
}
