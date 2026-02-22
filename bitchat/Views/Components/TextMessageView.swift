//
// TextMessageView.swift
// bitchat
//
// This is free and unencumbered software released into the public domain.
// For more information, see <https://unlicense.org>
//

import SwiftUI

struct TextMessageView: View {
    @Environment(\.colorScheme) private var colorScheme: ColorScheme
    @EnvironmentObject private var viewModel: ChatViewModel
    @State private var selectedTranslationTarget: SemayTranslationLanguage?
    
    let message: BitchatMessage
    @Binding var expandedMessageIDs: Set<String>
    
    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Precompute heavy token scans once per row
            let cashuLinks = message.content.extractCashuLinks()
            let lightningLinks = message.content.extractLightningLinks()
            let semayLinks = message.content.extractSemayLinks()
            let supportedTargets = SemayTranslationService.shared.availableTargets(for: message.content)
            HStack(alignment: .top, spacing: 0) {
                let isLong = (message.content.count > TransportConfig.uiLongMessageLengthThreshold || message.content.hasVeryLongToken(threshold: TransportConfig.uiVeryLongTokenThreshold)) && cashuLinks.isEmpty
                let isExpanded = expandedMessageIDs.contains(message.id)
                Text(viewModel.formatMessageAsText(message, colorScheme: colorScheme))
                    .fixedSize(horizontal: false, vertical: true)
                    .lineLimit(isLong && !isExpanded ? TransportConfig.uiLongMessageLineLimit : nil)
                    .frame(maxWidth: .infinity, alignment: .leading)
                
                // Delivery status indicator for private messages
                if message.isPrivate && message.sender == viewModel.nickname,
                   let status = message.deliveryStatus {
                    DeliveryStatusView(status: status)
                        .padding(.leading, 4)
                }
            }
            
            // Expand/Collapse for very long messages
            if (message.content.count > TransportConfig.uiLongMessageLengthThreshold || message.content.hasVeryLongToken(threshold: TransportConfig.uiVeryLongTokenThreshold)) && cashuLinks.isEmpty {
                let isExpanded = expandedMessageIDs.contains(message.id)
                let labelKey = isExpanded ? LocalizedStringKey("content.message.show_less") : LocalizedStringKey("content.message.show_more")
                Button(labelKey) {
                    if isExpanded { expandedMessageIDs.remove(message.id) }
                    else { expandedMessageIDs.insert(message.id) }
                }
                .font(.bitchatSystem(size: 11, weight: .medium, design: .monospaced))
                .foregroundColor(Color.blue)
                .padding(.top, 4)
            }

            // Render payment chips (Lightning / Cashu) with rounded background
            if !lightningLinks.isEmpty || !cashuLinks.isEmpty {
                HStack(spacing: 8) {
                    ForEach(lightningLinks, id: \.self) { link in
                        PaymentChipView(paymentType: .lightning(link))
                    }
                    ForEach(cashuLinks, id: \.self) { link in
                        PaymentChipView(paymentType: .cashu(link))
                    }
                }
                .padding(.top, 6)
                .padding(.leading, 2)
            }

            if !supportedTargets.isEmpty {
                HStack(spacing: 8) {
                    Text("Translate")
                        .font(.bitchatSystem(size: 11, weight: .medium, design: .monospaced))
                        .foregroundColor(.secondary)

                    ForEach(supportedTargets, id: \.self) { target in
                        Button {
                            if selectedTranslationTarget == target {
                                selectedTranslationTarget = nil
                            } else {
                                selectedTranslationTarget = target
                            }
                        } label: {
                            Text("→\(target.shortCode)")
                                .font(.bitchatSystem(size: 11, weight: .medium, design: .monospaced))
                                .padding(.horizontal, 8)
                                .padding(.vertical, 2)
                                .background(
                                    selectedTranslationTarget == target
                                    ? Color.accentColor.opacity(0.2)
                                    : Color.secondary.opacity(0.12)
                                )
                                .clipShape(Capsule())
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.top, 6)
                .padding(.leading, 2)

                if let target = selectedTranslationTarget,
                   let translation = SemayTranslationService.shared.translate(message.content, to: target) {
                    Text(translation)
                        .font(.bitchatSystem(size: 12, weight: .regular))
                        .foregroundColor(.secondary)
                        .padding(.top, 4)
                        .padding(.leading, 2)
                        .textSelection(.enabled)
                } else if selectedTranslationTarget != nil {
                    Text("No offline translation for this message yet.")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                        .padding(.top, 4)
                        .padding(.leading, 2)
                }
            }

            // Render Semay deep links as in-app chips (business/place/promise)
            if !semayLinks.isEmpty {
                HStack(spacing: 8) {
                    ForEach(semayLinks, id: \.self) { link in
                        SemayLinkChipView(urlString: link)
                    }
                }
                .padding(.top, 6)
                .padding(.leading, 2)
            }
        }
    }
}

// Translation behavior is handled by the shared offline service in SemayTranslationService.swift.

@available(macOS 14, iOS 17, *)
#Preview {
    @Previewable @State var ids: Set<String> = []
    let keychain = PreviewKeychainManager()
    
    Group {
        List {
            TextMessageView(message: .preview, expandedMessageIDs: $ids)
                .listRowSeparator(.hidden)
                .listRowInsets(EdgeInsets())
                .listRowBackground(EmptyView())
        }
        .environment(\.colorScheme, .light)
        
        List {
            TextMessageView(message: .preview, expandedMessageIDs: $ids)
                .listRowSeparator(.hidden)
                .listRowInsets(EdgeInsets())
                .listRowBackground(EmptyView())
        }
        .environment(\.colorScheme, .dark)
    }
    .environmentObject(
        ChatViewModel(
            keychain: keychain,
            idBridge: NostrIdentityBridge(),
            identityManager: SecureIdentityStateManager(keychain)
        )
    )
}
