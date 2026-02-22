import Foundation

struct ContributionExportItem {
    let entityType: String
    let entityID: String
    let payload: [String: String]
    let createdAt: Int
}

protocol ContributionExporter {
    var exporterID: String { get }
    func export(items: [ContributionExportItem]) async throws
}

/// Phase 1 exporter: Semay-native sync only (BLE/Nostr and optional hub).
struct SemayContributionExporter: ContributionExporter {
    let exporterID = "semay"

    func export(items: [ContributionExportItem]) async throws {
        guard !items.isEmpty else { return }
        // Contributions already flow through the Semay outbox + Nostr/hub sync pipeline.
        _ = items
    }
}
