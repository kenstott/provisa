import SwiftUI

/// Demo step (REQ-972..979): a single upfront question. "Install the demo dataset"
/// is the express path — accept all defaults and start installing immediately.
/// "Choose your options" routes into the deployment wizard for a custom setup.
struct DemoView: View {
    @ObservedObject var config: SetupConfig
    let onBack: () -> Void
    let onNext: () -> Void

    var body: some View {
        VStack(spacing: 0) {
            stepHeader(title: "Demo dataset",
                       subtitle: "Would you like the guided tour?")

            Spacer()

            VStack(alignment: .leading, spacing: 16) {
                Picker("", selection: $config.installDemo) {
                    Text("Install the demo dataset and open the guided tour").tag(true)
                    Text("Choose your options").tag(false)
                }
                .pickerStyle(.radioGroup)
                .labelsHidden()

                Text("The demo is a complete, fully functional Provisa install — pick it with confidence; nothing is limited. To reconfigure later, just run the installer again.")
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.6))
                    .fixedSize(horizontal: false, vertical: true)
            }
            .padding(.horizontal, 60)

            Spacer()

            navButtons(onBack: onBack, onNext: onNext, nextLabel: "Continue")
        }
    }
}
