import AppKit
import SwiftUI

@MainActor
@objc final class AppDelegate: NSObject, NSApplicationDelegate {
    private var statusItem: NSStatusItem?
    private var popover: NSPopover?
    private var setupWindow: NSWindow?
    private var statusService: ServiceStatus?

    private static var installBase: URL {
        if let saved = UserDefaults.standard.string(forKey: "provisaInstallDir"), !saved.isEmpty {
            return URL(fileURLWithPath: saved)
        }
        return FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".provisa")
    }

    private static var sentinel: URL { installBase.appendingPathComponent(".first-launch-complete") }
    private static var configFile: URL { installBase.appendingPathComponent("config.yaml") }

    func applicationDidFinishLaunching(_: Notification) {
        NSApp.setActivationPolicy(.accessory)
        if FileManager.default.fileExists(atPath: Self.sentinel.path) {
            // A completed install may carry a config written by an earlier release whose
            // shape predates keys the current CLI requires (e.g. `runtime:`), which makes
            // `provisa start` hang. Surface that instead of silently starting a broken config.
            if let (old, new) = staleConfig() {
                promptStaleConfig(old: old, new: new)
            } else {
                activateMenuBar()
            }
        } else {
            showSetupWizard()
        }
    }

    // MARK: - Prior-release config detection

    /// The release baked into this bundle (Contents/Resources/VERSION), or nil for a dev build.
    private func bundleVersion() -> String? {
        guard let url = Bundle.main.resourceURL?.appendingPathComponent("VERSION"),
              let raw = try? String(contentsOf: url, encoding: .utf8) else { return nil }
        let v = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return v.isEmpty ? nil : v
    }

    /// The `version:` stamped into config.yaml, or nil if the file has no such key
    /// (a config written before the version stamp existed — i.e. an earlier release).
    private func configVersion() -> String? {
        guard let text = try? String(contentsOf: Self.configFile, encoding: .utf8) else { return nil }
        for line in text.split(whereSeparator: \.isNewline) {
            let t = line.trimmingCharacters(in: .whitespaces)
            if t.hasPrefix("version:") {
                return String(t.dropFirst("version:".count)).trimmingCharacters(in: .whitespaces)
            }
        }
        return nil
    }

    /// Returns (old, new) when an existing config was written by a different release than
    /// this bundle; nil when it matches, is absent, or the bundle carries no version (dev build).
    private func staleConfig() -> (old: String, new: String)? {
        guard let new = bundleVersion() else { return nil }
        guard FileManager.default.fileExists(atPath: Self.configFile.path) else { return nil }
        let old = configVersion()
        if old == new { return nil }
        return (old: old ?? "an earlier release", new: new)
    }

    private func promptStaleConfig(old: String, new: String) {
        NSApp.setActivationPolicy(.regular)
        let alert = NSAlert()
        alert.messageText = "Configuration from an earlier release"
        alert.informativeText = """
        Provisa found a configuration written by \(old). This version is \(new). \
        A configuration from an earlier release may not start correctly.

        Overwrite it with updated settings, or keep your current one?
        """
        alert.addButton(withTitle: "Overwrite…")   // .alertFirstButtonReturn (default)
        alert.addButton(withTitle: "Keep Current")
        NSApp.activate(ignoringOtherApps: true)

        if alert.runModal() == .alertFirstButtonReturn {
            try? FileManager.default.removeItem(at: Self.sentinel)
            try? FileManager.default.removeItem(at: Self.configFile)
            showSetupWizard()   // regenerates config.yaml via first-launch.sh
        } else {
            activateMenuBar()
            NSApp.setActivationPolicy(.accessory)
        }
    }

    // MARK: - Setup wizard

    func showSetupWizard() {
        NSApp.setActivationPolicy(.regular)

        let view = SetupWizardView { [weak self] in
            self?.setupWindow?.close()
            // windowWillClose handles activateMenuBar() and setActivationPolicy
        }

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 720, height: 540),
            styleMask: [.titled, .closable, .miniaturizable],
            backing: .buffered,
            defer: false
        )
        window.center()
        // Force dark aqua so native controls (radio labels, checkboxes) render in
        // light text against the wizard's dark gradient — otherwise a Light-mode host
        // draws them black and illegible.
        window.appearance = NSAppearance(named: .darkAqua)
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
        guard let w = notification.object as? NSWindow, w === setupWindow else { return }
        guard statusItem == nil else { return }
        activateMenuBar()
        NSApp.setActivationPolicy(.accessory)
    }
}
