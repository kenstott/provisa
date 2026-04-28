import AppKit
import SwiftUI

struct InstallLocationView: View {
    @ObservedObject var config: SetupConfig
    let onBack: () -> Void
    let onNext: () -> Void

    @State private var freeGB: Int = 0

    var body: some View {
        VStack(spacing: 0) {
            Spacer()

            VStack(spacing: 8) {
                Text("Install Location")
                    .font(.system(size: 28, weight: .bold))
                    .foregroundStyle(.white)
                Text("Provisa will store its VM disk and data here.")
                    .font(.callout)
                    .foregroundStyle(.white.opacity(0.7))
            }

            Spacer()

            VStack(alignment: .leading, spacing: 16) {
                // Current path row
                HStack(spacing: 12) {
                    Image(systemName: "folder.fill")
                        .font(.title2)
                        .foregroundStyle(.white.opacity(0.8))
                    VStack(alignment: .leading, spacing: 2) {
                        Text(config.installDir.path)
                            .font(.system(.body, design: .monospaced))
                            .foregroundStyle(.white)
                            .lineLimit(2)
                            .truncationMode(.middle)
                        Text("\(freeGB) GB free on this volume")
                            .font(.caption)
                            .foregroundStyle(.white.opacity(0.55))
                    }
                    Spacer()
                    Button("Choose…") { pickFolder() }
                        .buttonStyle(.plain)
                        .padding(.horizontal, 14)
                        .padding(.vertical, 7)
                        .background(.white.opacity(0.15), in: RoundedRectangle(cornerRadius: 8))
                        .foregroundStyle(.white)
                }
                .padding(16)
                .background(.white.opacity(0.07), in: RoundedRectangle(cornerRadius: 12))
            }
            .padding(.horizontal, 60)

            Spacer()

            HStack {
                Button(action: onBack) {
                    Text("Back")
                        .frame(width: 100, height: 36)
                        .background(.white.opacity(0.12), in: RoundedRectangle(cornerRadius: 8))
                        .foregroundStyle(.white)
                }
                .buttonStyle(.plain)

                Spacer()

                Button(action: onNext) {
                    Text("Continue")
                        .frame(width: 140, height: 36)
                        .background(Color.indigo, in: RoundedRectangle(cornerRadius: 8))
                        .foregroundStyle(.white)
                }
                .buttonStyle(.plain)
            }
            .padding(.horizontal, 60)
            .padding(.bottom, 40)
        }
        .onAppear { refreshFreeSpace() }
    }

    private func pickFolder() {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.canCreateDirectories = true
        panel.allowsMultipleSelection = false
        panel.prompt = "Select"
        panel.message = "Choose a folder where Provisa will store its data and VM disk."
        panel.directoryURL = config.installDir.deletingLastPathComponent()
        if panel.runModal() == .OK, let url = panel.url {
            config.installDir = url.appendingPathComponent("provisa")
            refreshFreeSpace()
        }
    }

    private func refreshFreeSpace() {
        let checkURL = config.installDir.deletingLastPathComponent()
        if let attrs = try? FileManager.default.attributesOfFileSystem(forPath: checkURL.path),
           let free = attrs[.systemFreeSize] as? Int64 {
            freeGB = Int(free / (1024 * 1024 * 1024))
        } else {
            freeGB = 0
        }
    }
}
