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

        let pushReport = await SemayDataStore.shared.syncOutboxToHub()
        var feedReport: SemayDataStore.FeedSyncReport?
        let now = Date()
        if lastFeedSyncAt == nil || now.timeIntervalSince(lastFeedSyncAt ?? .distantPast) >= feedIntervalSeconds {
            feedReport = await SemayDataStore.shared.syncFeedFromHub()
            lastFeedSyncAt = now
        }
        lastSyncAt = Date()
        if let feedReport {
            lastSummary = "push: \(pushReport.summary) | pull: \(feedReport.summary)"
        } else {
            lastSummary = "push: \(pushReport.summary)"
        }

        let errors = [pushReport.errorMessage, feedReport?.errorMessage].compactMap { $0 }.filter { !$0.isEmpty }
        lastError = errors.isEmpty ? nil : errors.joined(separator: " | ")

        if let error = lastError {
            SecureLogger.warning("Semay envelope sync warning: \(error)", category: .session)
        } else if pushReport.attempted > 0 || (feedReport?.applied ?? 0) > 0 {
            SecureLogger.info("Semay envelope sync: \(lastSummary ?? "")", category: .session)
        }
    }
}
