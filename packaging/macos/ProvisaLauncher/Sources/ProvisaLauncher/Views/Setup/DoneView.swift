import SwiftUI

struct DoneView: View {
    let config: SetupConfig
    let onOpen: () -> Void

    var body: some View {
        VStack(spacing: 0) {
            Spacer()

            VStack(spacing: 20) {
                Image(systemName: "checkmark.seal.fill")
                    .font(.system(size: 64))
                    .foregroundStyle(.green)

                Text("Provisa is Ready")
                    .font(.system(size: 32, weight: .bold))
                    .foregroundStyle(.white)

                Text("Open your browser to start querying your data sources.\nProvisa will remain in your menu bar.")
                    .font(.callout)
                    .foregroundStyle(.white.opacity(0.6))
                    .multilineTextAlignment(.center)
                    .lineSpacing(4)
            }

            Spacer()

            VStack(spacing: 12) {
                Button(action: onOpen) {
                    Label("Open Provisa", systemImage: "safari")
                        .font(.headline)
                        .frame(width: 220, height: 44)
                        .background(Color.indigo)
                        .foregroundStyle(.white)
                        .clipShape(RoundedRectangle(cornerRadius: 10))
                }
                .buttonStyle(.plain)

                Text("http://localhost:\(config.uiPort)")
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.35))
                    .fontDesign(.monospaced)
            }
            .padding(.bottom, 52)
        }
    }
}
