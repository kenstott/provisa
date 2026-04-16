import SwiftUI

struct InstallProgressView: View {
    @ObservedObject var state: InstallState
    let onCancel: () -> Void

    private var progress: Double {
        let done = state.steps.filter { $0.status == .done }.count
        return Double(done) / Double(max(state.steps.count, 1))
    }

    private var currentStepLabel: String {
        state.steps.first { $0.status == .running }?.id.rawValue
            ?? state.steps.first { $0.status == .pending }?.id.rawValue
            ?? (state.hasFailed ? "Installation failed" : "Complete")
    }

    var body: some View {
        VStack(spacing: 0) {
            stepHeader(title: "Installing",
                       subtitle: "This takes a few minutes — no internet required")

            Spacer()

            VStack(spacing: 32) {
                // Step list
                VStack(alignment: .leading, spacing: 4) {
                    ForEach(state.steps) { step in
                        stepRow(step)
                    }
                }
                .padding(.horizontal, 80)

                // Progress bar + label
                VStack(spacing: 10) {
                    ProgressView(value: progress)
                        .progressViewStyle(.linear)
                        .tint(.indigo)
                        .padding(.horizontal, 80)

                    Text(currentStepLabel)
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.55))
                        .animation(.easeInOut, value: currentStepLabel)
                }
            }

            Spacer()

            HStack {
                if state.hasFailed {
                    Label("Installation failed.",
                          systemImage: "xmark.circle.fill")
                        .foregroundStyle(.red)
                        .font(.callout)
                }
                Spacer()
                if !state.isComplete {
                    Button("Cancel", action: onCancel)
                        .buttonStyle(WizardSecondaryButtonStyle())
                } else if state.hasFailed {
                    Button("Retry") {}
                        .buttonStyle(WizardPrimaryButtonStyle())
                }
            }
            .padding(.horizontal, 40)
            .padding(.bottom, 28)
        }
    }

    @ViewBuilder
    private func stepRow(_ step: InstallStep) -> some View {
        HStack(spacing: 10) {
            stepIcon(step.status)
            Text(step.id.rawValue)
                .font(.callout)
                .foregroundStyle(foreground(for: step.status))
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 12)
        .background(step.status == .running ? Color.white.opacity(0.08) : .clear,
                    in: RoundedRectangle(cornerRadius: 8))
    }

    @ViewBuilder
    private func stepIcon(_ status: StepStatus) -> some View {
        switch status {
        case .pending:
            Image(systemName: "circle")
                .foregroundStyle(.white.opacity(0.25))
        case .running:
            ProgressView()
                .scaleEffect(0.6)
                .frame(width: 16, height: 16)
                .tint(.indigo)
        case .done:
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green)
        case .failed:
            Image(systemName: "xmark.circle.fill")
                .foregroundStyle(.red)
        }
    }

    private func foreground(for status: StepStatus) -> Color {
        switch status {
        case .pending: return .white.opacity(0.35)
        case .running: return .white
        case .done:    return .white.opacity(0.6)
        case .failed:  return .red
        }
    }
}
