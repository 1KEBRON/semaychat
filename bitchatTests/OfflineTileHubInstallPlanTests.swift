import Foundation
import Testing

@testable import bitchat

@MainActor
struct OfflineTileHubInstallPlanTests {
    private let store = OfflineTileStore.shared

    @Test func installPlanResolvesDependencyOrderRootToLeaf() throws {
        let suffix = UUID().uuidString.lowercased()
        let coreID = "horn-core-\(suffix)"
        let regionID = "eritrea-core-\(suffix)"
        let detailID = "asmara-detail-\(suffix)"

        let core = try makeHubPack(id: coreID, packID: coreID)
        let region = try makeHubPack(id: regionID, packID: regionID, dependsOn: [coreID])
        let detail = try makeHubPack(id: detailID, packID: detailID, dependsOn: [regionID])

        let plan = store.installPlan(for: detail, catalog: [detail, region, core])

        #expect(!plan.hasCycle)
        #expect(plan.missingDependencies.isEmpty)
        #expect(plan.dependenciesToInstall.map { $0.packID ?? $0.id } == [coreID, regionID])
    }

    @Test func installPlanReportsMissingDependencyTokens() throws {
        let suffix = UUID().uuidString.lowercased()
        let detailID = "asmara-detail-\(suffix)"
        let missingID = "eritrea-core-missing-\(suffix)"
        let detail = try makeHubPack(id: detailID, packID: detailID, dependsOn: [missingID])

        let plan = store.installPlan(for: detail, catalog: [detail])

        #expect(!plan.hasCycle)
        #expect(plan.dependenciesToInstall.isEmpty)
        #expect(plan.missingDependencies == [missingID])
    }

    @Test func installPlanDetectsDependencyCycles() throws {
        let suffix = UUID().uuidString.lowercased()
        let firstID = "cycle-a-\(suffix)"
        let secondID = "cycle-b-\(suffix)"

        let first = try makeHubPack(id: firstID, packID: firstID, dependsOn: [secondID])
        let second = try makeHubPack(id: secondID, packID: secondID, dependsOn: [firstID])

        let plan = store.installPlan(for: first, catalog: [first, second])

        #expect(plan.hasCycle)
        #expect(plan.dependenciesToInstall.isEmpty)
    }

    @Test func installPlanDeduplicatesSharedDependencies() throws {
        let suffix = UUID().uuidString.lowercased()
        let coreID = "horn-core-\(suffix)"
        let regionID = "eritrea-core-\(suffix)"
        let detailID = "asmara-detail-\(suffix)"

        let core = try makeHubPack(id: coreID, packID: coreID)
        let region = try makeHubPack(id: regionID, packID: regionID, dependsOn: [coreID])
        let detail = try makeHubPack(
            id: detailID,
            packID: detailID,
            dependsOn: [regionID, coreID]
        )

        let plan = store.installPlan(for: detail, catalog: [detail, region, core])

        #expect(!plan.hasCycle)
        #expect(plan.missingDependencies.isEmpty)
        #expect(plan.dependenciesToInstall.map { $0.packID ?? $0.id } == [coreID, regionID])
    }

    private func makeHubPack(
        id: String,
        packID: String,
        dependsOn: [String] = []
    ) throws -> HubTilePack {
        var payload: [String: Any] = [
            "id": id,
            "pack_id": packID,
            "name": id,
            "file_name": "\(id).mbtiles",
            "size_bytes": 1_024,
            "min_zoom": 0,
            "max_zoom": 14,
            "attribution": "OpenStreetMap contributors",
            "download_url": "https://hub.semay.app/api/tiles/packs/\(id)/download",
            "pack_version": "1.0.0",
            "depends_on": dependsOn,
        ]
        payload["country_code"] = "ER"
        payload["country_name"] = "Eritrea"

        let data = try JSONSerialization.data(withJSONObject: payload, options: [])
        return try JSONDecoder().decode(HubTilePack.self, from: data)
    }
}
