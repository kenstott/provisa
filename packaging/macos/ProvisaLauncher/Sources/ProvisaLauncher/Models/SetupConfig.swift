import Foundation

final class SetupConfig: ObservableObject {
    @Published var installDir: URL = FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent(".provisa")
    @Published var ramGB: Int = defaultRAM()
    @Published var hostname: String = "localhost"
    @Published var uiPort: String = "3000"
    @Published var apiPort: String = "8000"
    @Published var flightPort: String = "8815"

    var cpuCount: Int {
        let total = ProcessInfo.processInfo.processorCount
        return max(2, min(total / 2, 12))
    }

    var federationWorkers: Int {
        switch ramGB {
        case ..<24:  return 0
        case 24..<48: return 1
        case 48..<96: return 2
        default:      return 4
        }
    }

    /// Environment variables forwarded to first-launch.sh in non-interactive mode.
    var environment: [String: String] {
        [
            "PROVISA_NONINTERACTIVE": "1",
            "PROVISA_INSTALL_DIR":    installDir.path,
            "PROVISA_RAM_GB":         "\(ramGB)",
            "PROVISA_CPU_COUNT":      "\(cpuCount)",
            "PROVISA_WORKERS":        "\(federationWorkers)",
            "PROVISA_HOSTNAME":       hostname,
            "PROVISA_UI_PORT":        uiPort,
            "PROVISA_API_PORT":       apiPort,
            "PROVISA_FLIGHT_PORT":    flightPort,
        ]
    }
}

// MARK: - Helpers

private func defaultRAM() -> Int {
    let bytes = ProcessInfo.processInfo.physicalMemory
    let gb = Int(bytes / (1024 * 1024 * 1024))
    // Default to half the host RAM, clamped to sensible options
    let half = gb / 2
    for size in [128, 64, 32, 16, 8, 4] where half >= size { return size }
    return 4
}

let ramOptions: [Int] = [4, 8, 16, 32, 64, 128]
