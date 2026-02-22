import Foundation
import Testing
@testable import bitchat

struct OfflineTilePackSelectorTests {
    @Test func resolvesDependencyChainRootToLeaf() {
        let core = makePack(
            id: "horn-core",
            name: "Horn Core",
            minZoom: 0,
            maxZoom: 6,
            bounds: PackBounds(minLon: 30, minLat: 10, maxLon: 50, maxLat: 25)
        )
        let eritrea = makePack(
            id: "eritrea-core",
            name: "Eritrea Core",
            minZoom: 7,
            maxZoom: 12,
            dependsOn: ["horn-core"],
            bounds: PackBounds(minLon: 35, minLat: 12, maxLon: 44, maxLat: 19)
        )
        let detail = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["eritrea-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )

        let chain = OfflineTilePackSelector.resolveActivationChain(for: detail, packs: [detail, eritrea, core])
        #expect(chain?.map(\.packID) == ["horn-core", "eritrea-core", "asmara-detail"])
    }

    @Test func rejectsCandidateWithMissingDependency() {
        let detail = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["eritrea-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )

        let chain = OfflineTilePackSelector.resolveActivationChain(for: detail, packs: [detail])
        #expect(chain == nil)
    }

    @Test func bestPackUsesPreferredZoomWithinRegion() {
        let core = makePack(
            id: "eritrea-core",
            name: "Eritrea Core",
            minZoom: 7,
            maxZoom: 12,
            bounds: PackBounds(minLon: 35, minLat: 12, maxLon: 44, maxLat: 19)
        )
        let detail = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["eritrea-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )
        let packs = [core, detail]

        let lowZoom = OfflineTilePackSelector.bestPack(
            from: packs,
            latitude: 15.32,
            longitude: 38.92,
            preferredZoom: 9
        )
        #expect(lowZoom?.packID == "eritrea-core")

        let highZoom = OfflineTilePackSelector.bestPack(
            from: packs,
            latitude: 15.32,
            longitude: 38.92,
            preferredZoom: 14
        )
        #expect(highZoom?.packID == "asmara-detail")
    }

    @Test func bestPackSkipsNonActivatableDependencies() {
        let core = makePack(
            id: "eritrea-core",
            name: "Eritrea Core",
            minZoom: 7,
            maxZoom: 12,
            bounds: PackBounds(minLon: 35, minLat: 12, maxLon: 44, maxLat: 19)
        )
        let brokenDetail = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["missing-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )

        let chosen = OfflineTilePackSelector.bestPack(
            from: [core, brokenDetail],
            latitude: 15.32,
            longitude: 38.92,
            preferredZoom: 14
        )
        #expect(chosen?.packID == "eritrea-core")
    }

    @Test func blockingDependentsFindsDirectDependents() {
        let core = makePack(
            id: "horn-core",
            name: "Horn Core",
            minZoom: 0,
            maxZoom: 6,
            bounds: PackBounds(minLon: 30, minLat: 10, maxLon: 50, maxLat: 25)
        )
        let detail = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["horn-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )

        let blockers = OfflineTilePackSelector.blockingDependents(for: core, packs: [core, detail])
        #expect(blockers.map(\.packID) == ["asmara-detail"])
    }

    @Test func blockingDependentsMatchesByFileStemFallback() {
        let target = OfflineTilePack(
            packID: "generated-123",
            regionCode: "er",
            packVersion: "1.0.0",
            tileFormat: .raster,
            dependsOn: [],
            styleURL: nil,
            languageCode: "en",
            name: "Eritrea Core",
            path: "/tmp/eritrea-core.mbtiles",
            minZoom: 7,
            maxZoom: 12,
            attribution: "© Test",
            bounds: PackBounds(minLon: 35, minLat: 12, maxLon: 44, maxLat: 19),
            sizeBytes: 1024,
            lifecycleState: .installed
        )
        let detail = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["eritrea-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )

        let blockers = OfflineTilePackSelector.blockingDependents(for: target, packs: [target, detail])
        #expect(blockers.map(\.packID) == ["asmara-detail"])
    }

    @Test func cascadeDependentsIncludesTransitiveDependentsInDeleteOrder() {
        let target = makePack(
            id: "horn-core",
            name: "Horn Core",
            minZoom: 0,
            maxZoom: 6,
            bounds: PackBounds(minLon: 30, minLat: 10, maxLon: 50, maxLat: 25)
        )
        let middle = makePack(
            id: "eritrea-core",
            name: "Eritrea Core",
            minZoom: 7,
            maxZoom: 12,
            dependsOn: ["horn-core"],
            bounds: PackBounds(minLon: 35, minLat: 12, maxLon: 44, maxLat: 19)
        )
        let leaf = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["eritrea-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )

        let dependents = OfflineTilePackSelector.cascadeDependents(for: target, packs: [target, middle, leaf])
        #expect(dependents.map(\.packID) == ["asmara-detail", "eritrea-core"])
    }

    @Test func cascadeDependentsUsesIdentityFallbackAcrossPackIDChanges() {
        let target = OfflineTilePack(
            packID: "new-generated-id",
            regionCode: "er",
            packVersion: "1.0.0",
            tileFormat: .raster,
            dependsOn: [],
            styleURL: nil,
            languageCode: "en",
            name: "Eritrea Core",
            path: "/tmp/eritrea-core.mbtiles",
            minZoom: 7,
            maxZoom: 12,
            attribution: "© Test",
            bounds: PackBounds(minLon: 35, minLat: 12, maxLon: 44, maxLat: 19),
            sizeBytes: 1024,
            lifecycleState: .installed
        )
        let dependent = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["eritrea-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )

        let dependents = OfflineTilePackSelector.cascadeDependents(for: target, packs: [target, dependent])
        #expect(dependents.map(\.packID) == ["asmara-detail"])
    }

    @Test func missingDependenciesReturnsMissingTokens() {
        let target = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["eritrea-core", "horn-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )
        let installed = [
            makePack(
                id: "eritrea-core",
                name: "Eritrea Core",
                minZoom: 7,
                maxZoom: 12,
                bounds: PackBounds(minLon: 35, minLat: 12, maxLon: 44, maxLat: 19)
            )
        ]

        let missing = OfflineTilePackSelector.missingDependencies(for: target, packs: installed)
        #expect(missing == ["horn-core"])
    }

    @Test func missingDependenciesUsesIdentityFallbackAcrossPackIDChanges() {
        let available = OfflineTilePack(
            packID: "generated-777",
            regionCode: "er",
            packVersion: "1.0.0",
            tileFormat: .raster,
            dependsOn: [],
            styleURL: nil,
            languageCode: "en",
            name: "Eritrea Core",
            path: "/tmp/eritrea-core.mbtiles",
            minZoom: 7,
            maxZoom: 12,
            attribution: "© Test",
            bounds: PackBounds(minLon: 35, minLat: 12, maxLon: 44, maxLat: 19),
            sizeBytes: 1024,
            lifecycleState: .installed
        )
        let target = makePack(
            id: "asmara-detail",
            name: "Asmara Detail",
            minZoom: 13,
            maxZoom: 16,
            dependsOn: ["eritrea-core"],
            bounds: PackBounds(minLon: 38.8, minLat: 15.1, maxLon: 39.0, maxLat: 15.4)
        )

        let missing = OfflineTilePackSelector.missingDependencies(for: target, packs: [available, target])
        #expect(missing.isEmpty)
    }

    private func makePack(
        id: String,
        name: String,
        minZoom: Int,
        maxZoom: Int,
        dependsOn: [String] = [],
        bounds: PackBounds?
    ) -> OfflineTilePack {
        OfflineTilePack(
            packID: id,
            regionCode: "er",
            packVersion: "1.0.0",
            tileFormat: .raster,
            dependsOn: dependsOn,
            styleURL: nil,
            languageCode: "en",
            name: name,
            path: "/tmp/\(id).mbtiles",
            minZoom: minZoom,
            maxZoom: maxZoom,
            attribution: "© Test",
            bounds: bounds,
            sizeBytes: 1024,
            lifecycleState: .installed
        )
    }
}
