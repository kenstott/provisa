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
                    // Demo selected → express path: accept defaults and install now.
                    // "Choose your options" → continue into the deployment wizard.
                    DemoView(config: config,
                             onBack: { step = 0 },
                             onNext: { if config.installDemo { beginInstall() } else { step = 2 } })
                case 2:
                    DeploymentView(config: config,
                                   onBack: { step = 1 },
                                   onNext: { step = 3 })
                case 3:
                    InstallLocationView(config: config,
                                        onBack: { step = 2 },
                                        onNext: { step = 4 })
                case 4:
                    NetworkView(config: config,
                                onBack: { step = 3 },
                                onNext: { beginInstall() })
                case 5:
                    InstallProgressView(state: installState,
                                        onCancel: { runner.cancel() })
                default:
                    DoneView(config: config, onOpen: {
                        // Open the UI at ?tour=1 when the demo was installed so the guided tour
                        // auto-starts fresh (App.tsx reads the query param).
                        let base = "http://localhost:\(config.uiPort)"
                        let target = config.installDemo ? "\(base)/?tour=1" : base
                        if let url = URL(string: target) {
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
                step = installState.hasFailed ? 5 : 6
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
        step = 5
        Task { @MainActor in
            // Show the step list for the chosen tier (native has no image/build steps).
            installState.configure(needsDocker: config.needsDocker)
            runner.run(config: config, state: installState)
        }
    }
}
