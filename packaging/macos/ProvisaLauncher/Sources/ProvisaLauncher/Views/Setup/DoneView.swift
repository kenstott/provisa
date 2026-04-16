import SwiftUI

struct DoneView: View {
    let config: SetupConfig
    let onOpen: () -> Void

    @State private var isReady = false

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
                    Label(isReady ? "Open Provisa" : "Starting Provisa…",
                          systemImage: isReady ? "safari" : "clock")
                        .font(.headline)
                        .frame(width: 220, height: 44)
                        .background(isReady ? Color.indigo : Color.gray.opacity(0.4))
                        .foregroundStyle(.white)
                        .clipShape(RoundedRectangle(cornerRadius: 10))
                }
                .buttonStyle(.plain)
                .disabled(!isReady)

                Text("http://localhost:\(config.uiPort)")
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.35))
                    .fontDesign(.monospaced)
            }
            .padding(.bottom, 52)
        }
        .task { await pollUntilReady() }
    }

    private func pollUntilReady() async {
        guard let url = URL(string: "http://localhost:\(config.uiPort)/health") else { return }
        while !isReady {
            if let (_, response) = try? await URLSession.shared.data(from: url),
               (response as? HTTPURLResponse)?.statusCode == 200 {
                isReady = true
                return
            }
            try? await Task.sleep(nanoseconds: 3_000_000_000)
        }
    }
}
