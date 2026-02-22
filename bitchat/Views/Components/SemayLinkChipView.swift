import SwiftUI

struct SemayLinkChipView: View {
    let urlString: String

    @Environment(\.colorScheme) private var colorScheme

    private var fgColor: Color {
        colorScheme == .dark ? Color.white : Color.black
    }

    private var bgColor: Color {
        colorScheme == .dark ? Color.gray.opacity(0.18) : Color.gray.opacity(0.12)
    }

    private var border: Color {
        Color.gray.opacity(0.25)
    }

    private var title: String {
        guard let url = URL(string: urlString) else { return "Open" }
        switch (url.host ?? "").lowercased() {
        case "business":
            return "Business"
        case "pin", "place":
            return "Place"
        case "promise":
            return "Promise"
        case "promise-response":
            return "Promise Reply"
        default:
            return "Open"
        }
    }

    private var symbolName: String {
        guard let url = URL(string: urlString) else { return "arrow.up.forward.app" }
        switch (url.host ?? "").lowercased() {
        case "business":
            return "building.2"
        case "pin", "place":
            return "mappin.and.ellipse"
        case "promise":
            return "hand.raised"
        case "promise-response":
            return "checkmark.seal"
        default:
            return "arrow.up.forward.app"
        }
    }

    var body: some View {
        Button {
            guard let url = URL(string: urlString) else { return }
            // In-app deep links should not depend on Safari / OS-level routing.
            NotificationCenter.default.post(name: .semayDeepLinkURL, object: url)
        } label: {
            HStack(spacing: 6) {
                Image(systemName: symbolName)
                Text(title)
                    .font(.bitchatSystem(size: 12, weight: .semibold, design: .monospaced))
            }
            .padding(.vertical, 6)
            .padding(.horizontal, 12)
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(bgColor)
            )
            .overlay(
                RoundedRectangle(cornerRadius: 12)
                    .stroke(border, lineWidth: 1)
            )
            .foregroundColor(fgColor)
        }
        .buttonStyle(.plain)
    }
}

