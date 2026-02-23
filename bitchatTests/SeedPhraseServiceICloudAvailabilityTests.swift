import Testing
@testable import bitchat

@MainActor
@Suite(.serialized)
struct SeedPhraseServiceICloudAvailabilityTests {
    @Test func backupUnavailableWithoutEntitlements() {
        let service = SeedPhraseService(
            keychain: MockKeychain(),
            iCloudContextProvider: { true },
            iCloudEntitlementsProvider: { false }
        )

        #expect(service.isICloudBackupAvailable() == false)
    }

    @Test func backupUnavailableWithoutCloudContext() {
        let service = SeedPhraseService(
            keychain: MockKeychain(),
            iCloudContextProvider: { false },
            iCloudEntitlementsProvider: { true }
        )

        #expect(service.isICloudBackupAvailable() == false)
    }

    @Test func backupAvailableWhenContextAndEntitlementsExist() {
        let service = SeedPhraseService(
            keychain: MockKeychain(),
            iCloudContextProvider: { true },
            iCloudEntitlementsProvider: { true }
        )

        #if canImport(CloudKit)
        #expect(service.isICloudBackupAvailable() == true)
        #else
        #expect(service.isICloudBackupAvailable() == false)
        #endif
    }
}
