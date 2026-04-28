import SwiftUI

struct StatusMenuView: View {
    @ObservedObject var status: ServiceStatus
    let onOpen:  () -> Void
    let onStart: () -> Void
    let onStop:  () -> Void
    let onQuit:  () -> Void

    var body: some View {
        VStack(spacing: 0) {
            // Header
            HStack(spacing: 10) {
                Circle()
                    .fill(status.isRunning ? Color.green : Color.secondary)
                    .frame(width: 10, height: 10)
                Text(status.isRunning ? "Running" : "Stopped")
                    .font(.headline)
                Spacer()
                Text("Provisa")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding(.horizontal, 16)
            .padding(.top, 14)
            .padding(.bottom, 10)

            Divider()

            // Actions
            Group {
                menuButton("Open Provisa",
                           icon: "safari",
                           disabled: !status.isRunning,
                           action: onOpen)

                Divider().padding(.horizontal, 16)

                if status.isRunning {
                    menuButton("Stop",
                               icon: "stop.circle",
                               disabled: status.isTransitioning,
                               action: onStop)
                } else {
                    menuButton("Start",
                               icon: "play.circle.fill",
                               disabled: status.isTransitioning,
                               action: onStart)
                }
            }

            Divider().padding(.top, 4)

            menuButton("Quit", icon: "power", action: onQuit)
                .padding(.bottom, 6)
        }
        .frame(width: 240)
        .background(.regularMaterial)
    }

    @ViewBuilder
    private func menuButton(
        _ title: String,
        icon: String,
        disabled: Bool = false,
        action: @escaping () -> Void
    ) -> some View {
        Button(action: action) {
            HStack(spacing: 10) {
                Image(systemName: icon)
                    .frame(width: 18)
                Text(title)
                Spacer()
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 8)
            .contentShape(Rectangle())
        }
        .buttonStyle(MenuItemButtonStyle())
        .disabled(disabled)
        .opacity(disabled ? 0.4 : 1)
    }
}

struct MenuItemButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .background(
                configuration.isPressed
                    ? Color.accentColor.opacity(0.15)
                    : Color.clear
            )
    }
}
