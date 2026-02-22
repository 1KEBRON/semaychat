import BitLogger
import Foundation

@MainActor
final class SemayEnvelopeSyncService: ObservableObject {
    static let shared = SemayEnvelopeSyncService()

    @Published private(set) var isSyncing = false
    @Published private(set) var lastSyncAt: Date?
    @Published private(set) var lastSummary: String?
    @Published private(set) var lastError: String?

    private var timer: Timer?
    private let intervalSeconds: TimeInterval = 20
    private let feedIntervalSeconds: TimeInterval = 60
    private var lastFeedSyncAt: Date?

    private init() {}

    func start() {
        guard timer == nil else { return }
        timer = Timer.scheduledTimer(withTimeInterval: intervalSeconds, repeats: true) { [weak self] _ in
            Task { @MainActor in
                await self?.syncNow()
            }
        }
        Task {
            await syncNow()
        }
    }

    func stop() {
        timer?.invalidate()
        timer = nil
    }

    func syncNow() async {
        guard !isSyncing else { return }
        isSyncing = true
        defer { isSyncing = false }

        let dataStore = SemayDataStore.shared

        let nostrPushReport = await dataStore.syncOutboxToNostr()
        var nodePushReport: SemayDataStore.OutboxSyncReport?
        if dataStore.hasConfiguredNode() {
            nodePushReport = await dataStore.syncOutboxToHub(allowDiscovery: false)
        }

        var nostrFeedReport: SemayDataStore.FeedSyncReport?
        var nodeFeedReport: SemayDataStore.FeedSyncReport?
        let now = Date()
        if lastFeedSyncAt == nil || now.timeIntervalSince(lastFeedSyncAt ?? .distantPast) >= feedIntervalSeconds {
            nostrFeedReport = await dataStore.syncFeedFromNostr()
            if dataStore.hasConfiguredNode() {
                nodeFeedReport = await dataStore.syncFeedFromHub(allowDiscovery: false)
            }
            lastFeedSyncAt = now
        }

        lastSyncAt = Date()

        let pushSummary: String = {
            if let nodePushReport {
                return "nostr \(nostrPushReport.summary) | node \(nodePushReport.summary)"
            }
            return "nostr \(nostrPushReport.summary)"
        }()

        if nostrFeedReport != nil || nodeFeedReport != nil {
            var pullParts: [String] = []
            if let nostrFeedReport {
                pullParts.append("nostr \(nostrFeedReport.summary)")
            }
            if let nodeFeedReport {
                pullParts.append("node \(nodeFeedReport.summary)")
            }
            lastSummary = "push: \(pushSummary) | pull: \(pullParts.joined(separator: " | "))"
        } else {
            lastSummary = "push: \(pushSummary)"
        }

        let errors = [
            nostrPushReport.errorMessage,
            nodePushReport?.errorMessage,
            nostrFeedReport?.errorMessage,
            nodeFeedReport?.errorMessage,
        ]
        .compactMap { $0 }
        .filter { !$0.isEmpty }

        lastError = errors.isEmpty ? nil : errors.joined(separator: " | ")

        if let error = lastError {
            SecureLogger.warning("Semay envelope sync warning: \(error)", category: .session)
        } else if nostrPushReport.attempted > 0
            || (nodePushReport?.attempted ?? 0) > 0
            || (nostrFeedReport?.applied ?? 0) > 0
            || (nodeFeedReport?.applied ?? 0) > 0 {
            SecureLogger.info("Semay envelope sync: \(lastSummary ?? "")", category: .session)
        }
    }
}
