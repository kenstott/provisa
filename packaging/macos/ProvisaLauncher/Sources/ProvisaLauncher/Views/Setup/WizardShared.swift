import SwiftUI

// MARK: - Shared helpers

func stepHeader(title: String, subtitle: String) -> some View {
    VStack(spacing: 6) {
        Text(title)
            .font(.system(size: 26, weight: .bold))
            .foregroundStyle(.white)
        Text(subtitle)
            .font(.callout)
            .foregroundStyle(.white.opacity(0.6))
    }
    .padding(.top, 40)
}

func label(_ text: String, icon: String) -> some View {
    Label(text, systemImage: icon)
        .font(.subheadline.weight(.semibold))
        .foregroundStyle(.white.opacity(0.7))
}

func navButtons(onBack: (() -> Void)? = nil, onNext: @escaping () -> Void, nextLabel: String = "Continue") -> some View {
    HStack {
        if let onBack {
            Button("Back", action: onBack)
                .buttonStyle(WizardSecondaryButtonStyle())
        }
        Spacer()
        Button(nextLabel, action: onNext)
            .buttonStyle(WizardPrimaryButtonStyle())
    }
    .padding(.horizontal, 60)
    .padding(.bottom, 36)
}

struct WizardPrimaryButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.headline)
            .frame(width: 160, height: 40)
            .background(Color.indigo.opacity(configuration.isPressed ? 0.8 : 1))
            .foregroundStyle(.white)
            .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

struct WizardSecondaryButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.callout)
            .frame(width: 80, height: 40)
            .foregroundStyle(.white.opacity(configuration.isPressed ? 0.4 : 0.6))
    }
}
