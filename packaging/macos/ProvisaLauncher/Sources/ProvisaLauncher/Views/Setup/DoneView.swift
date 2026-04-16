import SwiftUI

struct DoneView: View {
    let config: SetupConfig
    let onOpen: () -> Void

    @State private var isReady = false
    @State private var timedOut = false

    var body: some View {
        VStack(spacing: 0) {
            Spacer()

            VStack(spacing: 20) {
                Image(systemName: "checkmark.seal.fill")
                    .font(.system(size: 64))
                    .foregroundStyle(.green)

                Text("Installation Complete")
                    .font(.system(size: 32, weight: .bold))
                    .foregroundStyle(.white)

                Text(isReady
                     ? "Provisa is running. Open your browser to get started."
                     : "Provisa is starting up. This may take a minute.")
                    .font(.callout)
                    .foregroundStyle(.white.opacity(0.6))
                    .multilineTextAlignment(.center)
                    .lineSpacing(4)
                    .animation(.easeInOut, value: isReady)
            }

            Spacer()

            VStack(spacing: 16) {
                Button(action: onOpen) {
                    HStack(spacing: 10) {
                        if !isReady {
                            ProgressView()
                                .scaleEffect(0.7)
                                .tint(.white)
                        }
                        Text(isReady ? "Open Provisa" : "Starting…")
                            .font(.headline)
                    }
                    .frame(width: 220, height: 44)
                    .background(isReady ? Color.indigo : Color.indigo.opacity(0.5))
                    .foregroundStyle(.white)
                    .clipShape(RoundedRectangle(cornerRadius: 10))
                }
                .buttonStyle(.plain)
                .disabled(!isReady && !timedOut)
                .animation(.easeInOut, value: isReady)

                if timedOut && !isReady {
                    Button("Open anyway") { onOpen() }
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.5))
                        .buttonStyle(.plain)
                }

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
        let deadline = Date().addingTimeInterval(300)
        while !isReady {
            if let (_, response) = try? await URLSession.shared.data(from: url),
               (response as? HTTPURLResponse)?.statusCode == 200 {
                isReady = true
                return
            }
            if Date() > deadline {
                timedOut = true
                return
            }
            try? await Task.sleep(nanoseconds: 3_000_000_000)
        }
    }
}
