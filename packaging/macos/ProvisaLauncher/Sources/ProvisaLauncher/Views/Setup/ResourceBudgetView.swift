import SwiftUI

struct ResourceBudgetView: View {
    @ObservedObject var config: SetupConfig
    let onBack: () -> Void
    let onNext: () -> Void

    private let hostGB  = Int(ProcessInfo.processInfo.physicalMemory / (1024 * 1024 * 1024))
    private let hostCPU = ProcessInfo.processInfo.processorCount

    var body: some View {
        VStack(spacing: 0) {
            stepHeader(title: "Resource Budget",
                       subtitle: "How much of your Mac should Provisa use?")

            Spacer()

            VStack(spacing: 28) {
                // RAM picker
                VStack(alignment: .leading, spacing: 12) {
                    label("RAM", icon: "memorychip")
                    HStack(spacing: 8) {
                        ForEach(availableRAM, id: \.self) { gb in
                            ramButton(gb)
                        }
                    }
                    Text("Host has \(hostGB) GB installed")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.45))
                }

                Divider().background(.white.opacity(0.15))

                // Derived summary
                HStack(spacing: 40) {
                    derivedStat(icon: "cpu", label: "vCPUs", value: "\(config.cpuCount)")
                    derivedStat(icon: "server.rack", label: "Query Workers", value: "\(config.federationWorkers)")
                }

                Text("CPU and worker counts are derived from your RAM budget.")
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.4))
                    .multilineTextAlignment(.center)
            }
            .padding(.horizontal, 60)

            Spacer()

            navButtons(onBack: onBack, onNext: onNext, nextLabel: "Continue")
        }
    }

    private var availableRAM: [Int] {
        ramOptions.filter { $0 <= hostGB }
    }

    @ViewBuilder
    private func ramButton(_ gb: Int) -> some View {
        let selected = config.ramGB == gb
        Button {
            config.ramGB = gb
        } label: {
            VStack(spacing: 2) {
                Text("\(gb)")
                    .font(.system(size: 20, weight: .semibold))
                Text("GB")
                    .font(.caption2)
            }
            .frame(width: 60, height: 52)
            .background(selected ? Color.indigo : Color.white.opacity(0.08))
            .foregroundStyle(selected ? .white : .white.opacity(0.6))
            .clipShape(RoundedRectangle(cornerRadius: 10))
            .overlay(
                RoundedRectangle(cornerRadius: 10)
                    .stroke(selected ? Color.indigo : Color.white.opacity(0.12), lineWidth: 1.5)
            )
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private func derivedStat(icon: String, label: String, value: String) -> some View {
        VStack(spacing: 6) {
            Image(systemName: icon)
                .font(.title2)
                .foregroundStyle(.white.opacity(0.5))
            Text(value)
                .font(.system(size: 28, weight: .bold))
                .foregroundStyle(.white)
            Text(label)
                .font(.caption)
                .foregroundStyle(.white.opacity(0.5))
        }
    }
}

// MARK: - Shared helpers

func stepHeader(title: String, subtitle: String) -> some View {
    VStack(spacing: 6) {
        Text(title)
            .font(.system(size: 26, weight: .bold))
            .foregroundStyle(.white)
        Text(subtitle)
            .font(.callout)
            .foregroundStyle(.white.opacity(0.6))
    }
    .padding(.top, 40)
}

func label(_ text: String, icon: String) -> some View {
    Label(text, systemImage: icon)
        .font(.subheadline.weight(.semibold))
        .foregroundStyle(.white.opacity(0.7))
}

func navButtons(onBack: (() -> Void)? = nil, onNext: @escaping () -> Void, nextLabel: String = "Continue") -> some View {
    HStack {
        if let onBack {
            Button("Back", action: onBack)
                .buttonStyle(WizardSecondaryButtonStyle())
        }
        Spacer()
        Button(nextLabel, action: onNext)
            .buttonStyle(WizardPrimaryButtonStyle())
    }
    .padding(.horizontal, 60)
    .padding(.bottom, 36)
}

struct WizardPrimaryButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.headline)
            .frame(width: 160, height: 40)
            .background(Color.indigo.opacity(configuration.isPressed ? 0.8 : 1))
            .foregroundStyle(.white)
            .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

struct WizardSecondaryButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.callout)
            .frame(width: 80, height: 40)
            .foregroundStyle(.white.opacity(configuration.isPressed ? 0.4 : 0.6))
    }
}
