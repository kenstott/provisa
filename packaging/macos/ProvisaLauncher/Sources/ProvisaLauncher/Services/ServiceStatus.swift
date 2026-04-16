import AppKit
import Foundation

@MainActor
final class ServiceStatus: ObservableObject {
    @Published var isRunning: Bool = false
    @Published var isTransitioning: Bool = false

    var onStatusChange: ((Bool) -> Void)?

    private var timer: Timer?
    private var uiPort: String = "3000"

    init() {
        let cfg = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".provisa/config.yaml")
        if let text = try? String(contentsOf: cfg) {
            for line in text.components(separatedBy: "\n") {
                if line.hasPrefix("ui_port:") {
                    uiPort = line.components(separatedBy: ":").last?.trimmingCharacters(in: .whitespaces) ?? "3000"
                }
            }
        }
    }

    func startPolling() {
        timer = Timer.scheduledTimer(withTimeInterval: 5, repeats: true) { [weak self] _ in
            Task { @MainActor [weak self] in self?.poll() }
        }
        poll()
    }

    private func poll() {
        guard let url = URL(string: "http://localhost:\(uiPort)/health") else { return }
        URLSession.shared.dataTask(with: url) { [weak self] _, response, _ in
            Task { @MainActor [weak self] in
                guard let self else { return }
                let running = (response as? HTTPURLResponse)?.statusCode == 200
                if running != self.isRunning {
                    self.isRunning = running
                    self.onStatusChange?(running)
                }
            }
        }.resume()
    }

    func openUI() {
        if let url = URL(string: "http://localhost:\(uiPort)") {
            NSWorkspace.shared.open(url)
        }
    }

    func start() {
        runCLI("start")
    }

    func stop() {
        runCLI("stop")
    }

    private func runCLI(_ command: String) {
        isTransitioning = true
        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: "/usr/local/bin/provisa")
        proc.arguments = [command]
        proc.terminationHandler = { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.isTransitioning = false
                self?.poll()
            }
        }
        try? proc.run()
    }
}
