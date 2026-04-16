import Foundation

enum InstallStepID: String, CaseIterable, Identifiable {
    case staging  = "Staging files"
    case vmStart  = "Starting virtual machine"
    case images   = "Loading container images"
    case build    = "Building Provisa"
    case finalize = "Finalizing"

    var id: String { rawValue }

    var icon: String {
        switch self {
        case .staging:  return "doc.on.doc"
        case .vmStart:  return "server.rack"
        case .images:   return "shippingbox"
        case .build:    return "hammer"
        case .finalize: return "checkmark.seal"
        }
    }
}

enum StepStatus { case pending, running, done, failed }

struct InstallStep: Identifiable {
    let id: InstallStepID
    var status: StepStatus = .pending
}

@MainActor
final class InstallState: ObservableObject {
    @Published var steps: [InstallStep] = InstallStepID.allCases.map { InstallStep(id: $0) }
    @Published var log: String = ""
    @Published var isComplete = false
    @Published var hasFailed = false

    func markRunning(_ id: InstallStepID) {
        update(id, status: .running)
    }

    func markDone(_ id: InstallStepID) {
        update(id, status: .done)
    }

    func appendLog(_ text: String) {
        log += text
    }

    func finish(success: Bool) {
        isComplete = true
        hasFailed = !success
        if success {
            for i in steps.indices { steps[i].status = .done }
        }
    }

    private func update(_ id: InstallStepID, status: StepStatus) {
        if let i = steps.firstIndex(where: { $0.id == id }) {
            steps[i].status = status
        }
    }

    // Called from background thread — route via Task
    nonisolated func parseProgress(_ text: String) {
        for line in text.components(separatedBy: "\n") {
            let action: @Sendable () async -> Void
            if      line.contains("PROGRESS:staging")  { action = { await self.markRunning(.staging) } }
            else if line.contains("PROGRESS:vm_start") { action = { await self.markDone(.staging);  await self.markRunning(.vmStart) } }
            else if line.contains("PROGRESS:images")   { action = { await self.markDone(.vmStart);  await self.markRunning(.images) } }
            else if line.contains("PROGRESS:build")    { action = { await self.markDone(.images);   await self.markRunning(.build) } }
            else if line.contains("PROGRESS:finalize") { action = { await self.markDone(.build);    await self.markRunning(.finalize) } }
            else { continue }
            Task { @MainActor in await action() }
        }
    }
}
