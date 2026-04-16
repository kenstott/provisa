import Foundation

final class ScriptRunner {
    private var process: Process?

    /// Locate first-launch.sh.
    /// When running as a .app bundle the script lives in Contents/MacOS/.
    /// When running from a swift build debug binary it lives two levels up (repo root).
    static func scriptURL() -> URL? {
        // 1. Alongside the executable in the bundle
        let exe = URL(fileURLWithPath: ProcessInfo.processInfo.arguments[0])
        let bundleScript = exe.deletingLastPathComponent().appendingPathComponent("first-launch.sh")
        if FileManager.default.fileExists(atPath: bundleScript.path) {
            return bundleScript
        }

        // 2. Dev: repo root relative to CWD
        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
        let devScript = cwd
            .appendingPathComponent("packaging/macos/first-launch.sh")
        if FileManager.default.fileExists(atPath: devScript.path) {
            return devScript
        }

        return nil
    }

    @MainActor
    func run(config: SetupConfig, state: InstallState) {
        guard let scriptURL = Self.scriptURL() else {
            state.appendLog("[Error] first-launch.sh not found — reinstall Provisa.\n")
            state.finish(success: false)
            return
        }

        let proc = Process()
        process = proc
        proc.executableURL = URL(fileURLWithPath: "/bin/bash")
        proc.arguments = [scriptURL.path]

        var env = ProcessInfo.processInfo.environment
        for (k, v) in config.environment { env[k] = v }
        proc.environment = env

        let pipe = Pipe()
        proc.standardOutput = pipe
        proc.standardError  = pipe

        // Capture as a `let` constant so Swift 6 strict concurrency is satisfied.
        let capturedState = state

        pipe.fileHandleForReading.readabilityHandler = { handle in
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else { return }
            Task { @MainActor in
                capturedState.appendLog(text)
                capturedState.parseProgress(text)
            }
        }

        proc.terminationHandler = { p in
            Task { @MainActor in
                capturedState.finish(success: p.terminationStatus == 0)
            }
        }

        do {
            try proc.run()
        } catch {
            Task { @MainActor in
                state.appendLog("[Error] Could not start setup: \(error.localizedDescription)\n")
                state.finish(success: false)
            }
        }
    }

    func cancel() {
        process?.terminate()
    }
}
