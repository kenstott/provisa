// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "ProvisaLauncher",
    platforms: [.macOS(.v13)],
    targets: [
        .executableTarget(
            name: "ProvisaLauncher",
            path: "Sources/ProvisaLauncher"
        ),
    ]
)
