import Testing

@testable import bitchat

@MainActor
struct OfflineTileInstallMetricsTests {
    private let store = OfflineTileStore.shared

    @Test func installMetricsStartEmptyAfterReset() {
        store.debugResetInstallMetricsForTests()

        let snapshot = store.installMetricsSnapshot()

        #expect(snapshot.attempts == 0)
        #expect(snapshot.successes == 0)
        #expect(snapshot.failures == 0)
        #expect(snapshot.successRate == 0)
        #expect(snapshot.lastError == nil)
    }

    @Test func installMetricsTrackSuccessAndFailureDeterministically() {
        store.debugResetInstallMetricsForTests()

        store.debugRecordInstallOutcomeForTests(success: true)
        store.debugRecordInstallOutcomeForTests(success: false, errorMessage: "Hash mismatch")

        let afterFailure = store.installMetricsSnapshot()
        #expect(afterFailure.attempts == 2)
        #expect(afterFailure.successes == 1)
        #expect(afterFailure.failures == 1)
        #expect(afterFailure.successRate == 0.5)
        #expect(afterFailure.lastError == "Hash mismatch")

        store.debugRecordInstallOutcomeForTests(success: true)
        let final = store.installMetricsSnapshot()
        #expect(final.attempts == 3)
        #expect(final.successes == 2)
        #expect(final.failures == 1)
        #expect(final.successRate == (2.0 / 3.0))
        #expect(final.lastError == nil)
        #expect(final.updatedAt >= afterFailure.updatedAt)
    }
}
