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

        let report = await SemayDataStore.shared.syncOutboxToHub()
        lastSyncAt = Date()
        lastSummary = report.summary
        lastError = report.errorMessage

        if let error = report.errorMessage {
            SecureLogger.warning("Semay envelope sync warning: \(error)", category: .session)
        } else if report.attempted > 0 {
            SecureLogger.info("Semay envelope sync: \(report.summary)", category: .session)
        }
    }
}
