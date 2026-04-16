import SwiftUI

struct InstallProgressView: View {
    @ObservedObject var state: InstallState
    let onCancel: () -> Void

    @State private var logProxy = ScrollViewProxy?.none
    @State private var scrollID: String = "bottom"

    var body: some View {
        VStack(spacing: 0) {
            stepHeader(title: "Installing",
                       subtitle: "This takes a few minutes — no internet required")

            Spacer(minLength: 16)

            HStack(alignment: .top, spacing: 0) {
                // Step list (left panel)
                VStack(alignment: .leading, spacing: 0) {
                    ForEach(state.steps) { step in
                        stepRow(step)
                    }
                    Spacer()
                }
                .frame(width: 220)
                .padding(.leading, 40)

                Divider()
                    .background(.white.opacity(0.12))
                    .padding(.vertical, 8)

                // Log output (right panel)
                ScrollViewReader { proxy in
                    ScrollView {
                        Text(state.log)
                            .font(.system(size: 11, design: .monospaced))
                            .foregroundStyle(.white.opacity(0.75))
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(12)
                            .id("logContent")

                        Color.clear.frame(height: 1).id(scrollID)
                    }
                    .background(.black.opacity(0.25))
                    .clipShape(RoundedRectangle(cornerRadius: 8))
                    .padding(.horizontal, 16)
                    .onChange(of: state.log) { _ in
                        proxy.scrollTo(scrollID, anchor: .bottom)
                    }
                }
            }

            Spacer(minLength: 16)

            // Footer
            HStack {
                if state.hasFailed {
                    Label("Installation failed. Check the log for details.",
                          systemImage: "xmark.circle.fill")
                        .foregroundStyle(.red)
                        .font(.callout)
                }
                Spacer()
                if !state.isComplete {
                    Button("Cancel", action: onCancel)
                        .buttonStyle(WizardSecondaryButtonStyle())
                } else if state.hasFailed {
                    Button("Retry") {
                        // Parent will re-run the install
                    }
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
        .padding(.vertical, 10)
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
