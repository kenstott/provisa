import SwiftUI

struct NetworkView: View {
    @ObservedObject var config: SetupConfig
    let onBack: () -> Void
    let onNext: () -> Void

    @State private var portWarnings: [String: String] = [:]

    var body: some View {
        VStack(spacing: 0) {
            stepHeader(title: "Network",
                       subtitle: "Where should Provisa listen?")

            Spacer()

            VStack(alignment: .leading, spacing: 20) {
                // Hostname
                fieldRow(icon: "network", title: "Hostname") {
                    TextField("localhost", text: $config.hostname)
                        .wizardField()
                }

                Divider().background(.white.opacity(0.12))

                // Ports
                Text("Ports")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.white.opacity(0.5))

                HStack(spacing: 16) {
                    portField(label: "Web UI",   binding: $config.uiPort,     key: "ui",     default: "3000")
                    portField(label: "API",      binding: $config.apiPort,    key: "api",    default: "8000")
                    portField(label: "Flight",   binding: $config.flightPort, key: "flight", default: "8815")
                }

                // Warnings
                if !portWarnings.isEmpty {
                    VStack(alignment: .leading, spacing: 4) {
                        ForEach(portWarnings.sorted(by: { $0.key < $1.key }), id: \.key) { _, msg in
                            Label(msg, systemImage: "exclamationmark.triangle.fill")
                                .font(.caption)
                                .foregroundStyle(.yellow)
                        }
                    }
                }
            }
            .padding(.horizontal, 60)

            Spacer()

            navButtons(onBack: onBack, onNext: onNext, nextLabel: "Install")
        }
        .onChange(of: config.uiPort)     { _ in checkPorts() }
        .onChange(of: config.apiPort)    { _ in checkPorts() }
        .onChange(of: config.flightPort) { _ in checkPorts() }
        .onAppear { checkPorts() }
    }

    @ViewBuilder
    private func portField(label: String, binding: Binding<String>, key: String, default defaultPort: String) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(label)
                .font(.caption)
                .foregroundStyle(.white.opacity(0.55))
            TextField(defaultPort, text: binding)
                .wizardField()
                .frame(width: 90)
        }
    }

    @ViewBuilder
    private func fieldRow<Content: View>(icon: String, title: String, @ViewBuilder content: () -> Content) -> some View {
        HStack(spacing: 12) {
            Image(systemName: icon)
                .frame(width: 22)
                .foregroundStyle(.white.opacity(0.45))
            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.55))
                content()
            }
        }
    }

    private func checkPorts() {
        let ports = [("ui", config.uiPort), ("api", config.apiPort), ("flight", config.flightPort)]
        Task.detached {
            var result: [String: String] = [:]
            for (key, port) in ports {
                guard let p = Int(port), p >= 1024, p <= 65535 else {
                    result[key] = "Port '\(port)' is not valid (1024–65535)"
                    continue
                }
                if checkPortInUse(p) {
                    result[key] = "Port \(p) appears to be in use"
                }
            }
            let captured = result
            await MainActor.run { portWarnings = captured }
        }
    }
}

// MARK: - Port check (free function — no actor isolation)

private func checkPortInUse(_ port: Int) -> Bool {
    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: "/usr/bin/lsof")
    proc.arguments = ["-iTCP:\(port)", "-sTCP:LISTEN", "-t"]
    let pipe = Pipe()
    proc.standardOutput = pipe
    proc.standardError  = Pipe()
    (try? proc.run()).map { proc.waitUntilExit() }
    return !pipe.fileHandleForReading.availableData.isEmpty
}

// MARK: - TextField style

extension View {
    func wizardField() -> some View {
        self
            .textFieldStyle(.plain)
            .font(.system(.body, design: .monospaced))
            .foregroundStyle(.white)
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(.white.opacity(0.08))
            .clipShape(RoundedRectangle(cornerRadius: 6))
            .overlay(
                RoundedRectangle(cornerRadius: 6)
                    .stroke(Color.white.opacity(0.15), lineWidth: 1)
            )
    }
}
