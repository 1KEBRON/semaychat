import Foundation
import Testing

@testable import bitchat

@MainActor
struct SemayServiceContributionShareTests {
    private let store = SemayDataStore.shared

    @Test func localCreateStaysPersonalByDefault() {
        store.debugResetForTests()

        let created = store.publishServiceDirectoryEntry(
            name: "Asmara Cafe",
            serviceType: "business",
            category: "restaurant",
            details: "Coffee and breakfast",
            city: "Asmara",
            country: "Eritrea",
            latitude: 15.3229,
            longitude: 38.9251,
            phone: "+2917000000",
            website: "https://example.er"
        )

        #expect(created != nil)
        #expect(created?.shareScope == .personal)
        #expect(created?.publishState == .localOnly)
        #expect(store.pendingOutboxCount() == 0)
        #expect(store.pendingContributionPublications().isEmpty)
    }

    @Test func explicitShareQueuesNetworkPublication() {
        store.debugResetForTests()
        guard let created = store.publishServiceDirectoryEntry(
            name: "Semay Pharmacy",
            serviceType: "business",
            category: "pharmacy",
            details: "Open until 21:00",
            city: "Asmara",
            country: "Eritrea",
            latitude: 15.3333,
            longitude: 38.9333,
            phone: "+2917111111",
            website: "https://pharmacy.example"
        ) else {
            #expect(Bool(false), "Expected local service creation to succeed")
            return
        }

        let result = store.requestNetworkShareForService(serviceID: created.serviceID)
        let outboxCount = store.pendingOutboxCount()
        let queued = store.pendingContributionPublications()

        #expect(result.accepted)
        #expect(result.reasons.isEmpty)
        #expect(outboxCount == 1)

        #expect(queued.count == 1)
        #expect(queued.first?.entityType == "service")
        #expect(queued.first?.entityID == created.serviceID)
        #expect(queued.first?.publishState == SemayContributionPublishState.pendingReview.rawValue)

        let refreshed = store.activeDirectoryServices.first(where: { $0.serviceID == created.serviceID })
        #expect(refreshed?.shareScope == .network)
        #expect(refreshed?.publishState == .pendingReview)
    }

    @Test func settingPersonalScopeStopsFurtherNetworkUpdates() {
        store.debugResetForTests()
        guard let created = store.publishServiceDirectoryEntry(
            name: "Semay Clinic",
            serviceType: "clinic",
            category: "clinic",
            details: "Primary care",
            city: "Asmara",
            country: "Eritrea",
            latitude: 15.3010,
            longitude: 38.9100,
            phone: "+2917222222",
            website: "https://clinic.example"
        ) else {
            #expect(Bool(false), "Expected local service creation to succeed")
            return
        }

        let shareResult = store.requestNetworkShareForService(serviceID: created.serviceID)
        #expect(shareResult.accepted)
        let outboxBeforePersonal = store.pendingOutboxCount()
        #expect(outboxBeforePersonal == 1)

        store.setServiceContributionScope(serviceID: created.serviceID, scope: .personal)
        #expect(store.pendingContributionPublications().isEmpty)
        #expect(store.pendingOutboxCount() == 0)

        guard var editable = store.activeDirectoryServices.first(where: { $0.serviceID == created.serviceID }) else {
            #expect(Bool(false), "Expected refreshed service to be available")
            return
        }
        editable.details = "Primary care and pediatrics"
        store.updateServiceDirectoryEntry(editable)

        #expect(store.pendingOutboxCount() == 0)
        let refreshed = store.activeDirectoryServices.first(where: { $0.serviceID == created.serviceID })
        #expect(refreshed?.shareScope == .personal)
        #expect(refreshed?.publishState == .localOnly)
    }
}
