import Foundation

enum InstallStepID: String, CaseIterable, Identifiable {
    case staging    = "Staging files"
    case dockerStart = "Building images"
    case images     = "Loading images"
    case extensions = "Installing components"
    case finalize   = "Finalizing"

    var id: String { rawValue }

    var icon: String {
        switch self {
        case .staging:     return "doc.on.doc"
        case .dockerStart: return "hammer"
        case .images:      return "square.stack.3d.up"
        case .extensions:  return "puzzlepiece.extension"
        case .finalize:    return "checkmark.seal"
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
    // Default to the native step set; configure(needsDocker:) resets it once the
    // chosen tier is known (the Docker tier adds the image build/load steps).
    @Published var steps: [InstallStep] = InstallState.stepIDs(needsDocker: false).map { InstallStep(id: $0) }
    @Published var log: String = ""
    @Published var logPath: String?
    @Published var isComplete = false
    @Published var hasFailed = false

    /// The progress steps for a tier. Native (embedded, no Docker) has no image
    /// build or load — it only stages the bundled runtime, installs components,
    /// and finalizes. The Docker tier adds those middle steps (host
    /// `docker compose build` covers build + load).
    static func stepIDs(needsDocker: Bool) -> [InstallStepID] {
        needsDocker
            ? [.staging, .dockerStart, .images, .extensions, .finalize]
            : [.staging, .extensions, .finalize]
    }

    /// Reset the step list for the chosen tier before an install begins.
    func configure(needsDocker: Bool) {
        steps = InstallState.stepIDs(needsDocker: needsDocker).map { InstallStep(id: $0) }
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

    /// Mark `id` running and every earlier step in the current tier's list done.
    /// No-op if `id` isn't part of this tier (e.g. a build marker on native).
    private func advance(to id: InstallStepID) {
        guard let idx = steps.firstIndex(where: { $0.id == id }) else { return }
        for i in 0..<idx where steps[i].status != .done { steps[i].status = .done }
        steps[idx].status = .running
    }

    // Called from background thread — route via Task
    nonisolated func parseProgress(_ text: String) {
        for line in text.components(separatedBy: "\n") {
            let step: InstallStepID
            if      line.contains("PROGRESS:staging")    { step = .staging }
            else if line.contains("PROGRESS:build")      { step = .dockerStart }
            else if line.contains("PROGRESS:images")     { step = .images }
            else if line.contains("PROGRESS:extensions") { step = .extensions }
            else if line.contains("PROGRESS:finalize")   { step = .finalize }
            else { continue }
            Task { @MainActor in self.advance(to: step) }
        }
    }
}
