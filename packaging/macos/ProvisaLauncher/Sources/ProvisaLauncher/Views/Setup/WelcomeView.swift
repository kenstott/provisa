import SwiftUI

struct WelcomeView: View {
    let onNext: () -> Void

    @State private var checks: [RequirementCheck] = []

    var body: some View {
        VStack(spacing: 0) {
            Spacer()

            // Branding
            VStack(spacing: 8) {
                Image(systemName: "cylinder.split.1x2.fill")
                    .font(.system(size: 56))
                    .foregroundStyle(.white)

                Text("Provisa")
                    .font(.system(size: 44, weight: .bold, design: .default))
                    .foregroundStyle(.white)

                Text("Data Virtualization Platform")
                    .font(.title3)
                    .foregroundStyle(.white.opacity(0.7))
            }

            Spacer()

            // Requirement checks
            VStack(alignment: .leading, spacing: 10) {
                ForEach(checks) { check in
                    HStack(spacing: 10) {
                        Image(systemName: check.passed ? "checkmark.circle.fill" : "xmark.circle.fill")
                            .foregroundStyle(check.passed ? .green : .red)
                        Text(check.label)
                            .foregroundStyle(.white.opacity(0.85))
                            .font(.callout)
                        Spacer()
                        if let detail = check.detail {
                            Text(detail)
                                .foregroundStyle(.white.opacity(0.5))
                                .font(.caption)
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.vertical, 6)
                    .background(.white.opacity(0.06), in: RoundedRectangle(cornerRadius: 8))
                }
            }
            .padding(.horizontal, 60)

            Spacer()

            // CTA
            Button(action: onNext) {
                Text("Begin Setup")
                    .font(.headline)
                    .frame(width: 200, height: 44)
                    .background(allPassed ? Color.indigo : Color.gray)
                    .foregroundStyle(.white)
                    .clipShape(RoundedRectangle(cornerRadius: 10))
            }
            .buttonStyle(.plain)
            .disabled(!allPassed)
            .padding(.bottom, 40)
        }
        .onAppear { checks = RequirementCheck.run() }
    }

    private var allPassed: Bool { checks.allSatisfy(\.passed) }
}

// MARK: - Requirement checks

struct RequirementCheck: Identifiable {
    let id = UUID()
    let label: String
    let detail: String?
    let passed: Bool

    static func run() -> [RequirementCheck] {
        let arch = ProcessInfo.processInfo.machineHardwareName
        let isArm = arch.contains("arm64") || arch.contains("Apple")
        let gb = ProcessInfo.processInfo.physicalMemory / (1024 * 1024 * 1024)

        // Disk: need ~40 GB free
        let freeGB: Int
        if let attrs = try? FileManager.default.attributesOfFileSystem(forPath: NSHomeDirectory()),
           let free = attrs[.systemFreeSize] as? Int64 {
            freeGB = Int(free / (1024 * 1024 * 1024))
        } else {
            freeGB = 0
        }

        return [
            RequirementCheck(label: "Apple Silicon (arm64)",
                             detail: arch,
                             passed: isArm),
            RequirementCheck(label: "8 GB RAM or more",
                             detail: "\(gb) GB installed",
                             passed: gb >= 8),
            RequirementCheck(label: "40 GB free disk space",
                             detail: "\(freeGB) GB available",
                             passed: freeGB >= 40),
        ]
    }
}

// MARK: - ProcessInfo helper

extension ProcessInfo {
    var machineHardwareName: String {
        var size = 0
        sysctlbyname("hw.machine", nil, &size, nil, 0)
        var machine = [CChar](repeating: 0, count: size)
        sysctlbyname("hw.machine", &machine, &size, nil, 0)
        return String(cString: machine)
    }
}
