import SwiftUI

struct SetupWizardView: View {
    let onComplete: () -> Void

    @StateObject private var config = SetupConfig()
    @StateObject private var installState = InstallState()
    @State private var step = 0

    private let runner = ScriptRunner()

    var body: some View {
        ZStack {
            // Gradient background matching DMG design
            LinearGradient(
                colors: [Color(red: 0.06, green: 0.16, blue: 0.39),
                         Color(red: 0.31, green: 0.04, blue: 0.55)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            Group {
                switch step {
                case 0:
                    WelcomeView(onNext: { step = 1 })
                case 1:
                    InstallLocationView(config: config,
                                        onBack: { step = 0 },
                                        onNext: { step = 2 })
                case 2:
                    ResourceBudgetView(config: config,
                                       onBack: { step = 1 },
                                       onNext: { step = 3 })
                case 3:
                    NetworkView(config: config,
                                onBack: { step = 2 },
                                onNext: { beginInstall() })
                case 4:
                    InstallProgressView(state: installState,
                                        onCancel: { runner.cancel() })
                default:
                    DoneView(config: config, onOpen: {
                        if let url = URL(string: "http://localhost:\(config.uiPort)") {
                            NSWorkspace.shared.open(url)
                        }
                        onComplete()
                    })
                }
            }
            .transition(.asymmetric(
                insertion:  .move(edge: .trailing),
                removal:    .move(edge: .leading)
            ))
            .animation(.easeInOut(duration: 0.3), value: step)
        }
        .frame(width: 720, height: 540)
        .onChange(of: installState.isComplete) { done in
            if done {
                if !installState.hasFailed {
                    UserDefaults.standard.set(config.installDir.path, forKey: "provisaInstallDir")
                    startProvisa()
                }
                step = installState.hasFailed ? 4 : 5
            }
        }
    }

    private func startProvisa() {
        Task.detached {
            let proc = Process()
            proc.executableURL = URL(fileURLWithPath: "/usr/local/bin/provisa")
            proc.arguments = ["start"]
            try? proc.run()
            proc.waitUntilExit()
        }
    }

    private func beginInstall() {
        step = 4
        Task { @MainActor in
            runner.run(config: config, state: installState)
        }
    }
}
