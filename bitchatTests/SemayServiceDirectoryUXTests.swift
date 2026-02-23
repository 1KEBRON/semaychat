import Foundation
import Testing

@testable import bitchat

@MainActor
struct SemayServiceDirectoryUXTests {
    private let store = SemayDataStore.shared

    @Test func businessLinkCreatesCanonicalServiceAndPreservesManualWebsite() {
        store.debugResetForTests()

        let business = store.registerBusiness(
            name: "Asmara Fresh",
            category: "grocery",
            details: "Neighborhood market",
            latitude: 15.3301,
            longitude: 38.9322,
            phone: "+2917333333"
        )

        guard let serviceID = store.linkedServiceID(entityType: "business", entityID: business.businessID),
              var linked = store.activeDirectoryServices.first(where: { $0.serviceID == serviceID }) else {
            #expect(Bool(false), "Expected linked canonical service for business")
            return
        }

        linked.website = "https://asmarafresh.example"
        store.updateServiceDirectoryEntry(linked)

        let updatedBusiness = store.updateBusiness(
            businessID: business.businessID,
            name: "Asmara Fresh",
            category: "grocery",
            details: "Neighborhood market and delivery",
            latitude: 15.3301,
            longitude: 38.9322,
            phone: "+2917444444"
        )

        #expect(updatedBusiness != nil)

        let refreshed = store.activeDirectoryServices.first(where: { $0.serviceID == serviceID })
        #expect(refreshed?.website == "https://asmarafresh.example")
        #expect(refreshed?.phone == "+2917444444")
    }

    @Test func reviewActionsDriveDeterministicServiceOrdering() {
        store.debugResetForTests()

        guard let serviceA = store.publishServiceDirectoryEntry(
            name: "Semay Cafe One",
            serviceType: "business",
            category: "restaurant",
            details: "Coffee and tea",
            city: "Asmara",
            country: "Eritrea",
            latitude: 15.3240,
            longitude: 38.9260,
            phone: "+2917001111",
            website: "https://cafe-one.example"
        ),
        let serviceB = store.publishServiceDirectoryEntry(
            name: "Semay Cafe Two",
            serviceType: "business",
            category: "restaurant",
            details: "Pastries and snacks",
            city: "Asmara",
            country: "Eritrea",
            latitude: 15.3250,
            longitude: 38.9270,
            phone: "+2917002222",
            website: "https://cafe-two.example"
        ) else {
            #expect(Bool(false), "Expected service fixtures")
            return
        }

        // Use a low endorsement score so a subsequent report deterministically flips ordering.
        let endorsed = store.endorseServiceDirectoryEntry(serviceID: serviceA.serviceID, score: 0, reason: "verified")
        #expect(endorsed)

        let afterEndorse = store.activeDirectoryServices.map(\.serviceID)
        let endorseIndexA = afterEndorse.firstIndex(of: serviceA.serviceID)
        let endorseIndexB = afterEndorse.firstIndex(of: serviceB.serviceID)
        #expect(endorseIndexA != nil)
        #expect(endorseIndexB != nil)
        if let endorseIndexA, let endorseIndexB {
            #expect(endorseIndexA < endorseIndexB)
        }

        store.reportServiceDirectoryEntry(serviceID: serviceA.serviceID, reason: "mismatch")
        let afterReport = store.activeDirectoryServices.map(\.serviceID)
        let reportIndexA = afterReport.firstIndex(of: serviceA.serviceID)
        let reportIndexB = afterReport.firstIndex(of: serviceB.serviceID)
        #expect(reportIndexA != nil)
        #expect(reportIndexB != nil)
        if let reportIndexA, let reportIndexB {
            #expect(reportIndexB < reportIndexA)
        }

        #expect(afterReport.contains(serviceA.serviceID))
        #expect(afterReport.contains(serviceB.serviceID))
    }
}
