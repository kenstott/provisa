import AppKit
import SwiftUI

@MainActor
@objc final class AppDelegate: NSObject, NSApplicationDelegate {
    private var statusItem: NSStatusItem?
    private var popover: NSPopover?
    private var setupWindow: NSWindow?
    private var statusService: ServiceStatus?

    private static var sentinel: URL {
        let base: URL
        if let saved = UserDefaults.standard.string(forKey: "provisaInstallDir"), !saved.isEmpty {
            base = URL(fileURLWithPath: saved)
        } else {
            base = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".provisa")
        }
        return base.appendingPathComponent(".first-launch-complete")
    }

    func applicationDidFinishLaunching(_: Notification) {
        NSApp.setActivationPolicy(.accessory)
        if FileManager.default.fileExists(atPath: Self.sentinel.path) {
            activateMenuBar()
        } else {
            showSetupWizard()
        }
    }

    // MARK: - Setup wizard

    func showSetupWizard() {
        NSApp.setActivationPolicy(.regular)

        let view = SetupWizardView { [weak self] in
            self?.setupWindow?.close()
            self?.activateMenuBar()
            NSApp.setActivationPolicy(.accessory)
        }

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 720, height: 540),
            styleMask: [.titled, .closable, .miniaturizable],
            backing: .buffered,
            defer: false
        )
        window.center()
        window.contentViewController = NSHostingController(rootView: view)
        window.title = "Provisa Setup"
        window.isReleasedWhenClosed = false
        window.delegate = self
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        setupWindow = window
    }

    // MARK: - Menu bar

    private func activateMenuBar() {
        let service = ServiceStatus()
        statusService = service
        service.startPolling()

        let bar = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        statusItem = bar

        if let button = bar.button {
            button.image = NSImage(
                systemSymbolName: "circle.fill",
                accessibilityDescription: "Provisa"
            )
            button.action = #selector(handleBarClick(_:))
            button.target = self
            // Keep button icon colour in sync with service status
            service.onStatusChange = { [weak button] running in
                button?.contentTintColor = running ? .systemGreen : .secondaryLabelColor
            }
        }

        let pop = NSPopover()
        pop.contentViewController = NSHostingController(
            rootView: StatusMenuView(
                status: service,
                onOpen:  { service.openUI() },
                onStart: { service.start() },
                onStop:  { service.stop() },
                onQuit:  { NSApp.terminate(nil) }
            )
        )
        pop.contentSize = NSSize(width: 260, height: 300)
        pop.behavior = .transient
        popover = pop
    }

    @objc private func handleBarClick(_ sender: NSStatusBarButton) {
        guard let pop = popover else { return }
        if pop.isShown {
            pop.performClose(sender)
        } else {
            pop.show(relativeTo: sender.bounds, of: sender, preferredEdge: .minY)
        }
    }
}

extension AppDelegate: NSWindowDelegate {
    func windowWillClose(_ notification: Notification) {
        guard statusItem == nil else { return }
        activateMenuBar()
        NSApp.setActivationPolicy(.accessory)
    }
}
