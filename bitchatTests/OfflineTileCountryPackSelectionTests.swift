import Foundation
import Testing
@testable import bitchat

@MainActor
struct OfflineTileCountryPackSelectionTests {
    private let store = OfflineTileStore.shared

    @Test func resolveCountryPackPrefersFeaturedPack() throws {
        let fallback = try makePack(
            id: "fallback-eritrea",
            packID: "eritrea-core",
            name: "Eritrea Core",
            version: "1.2.0",
            isFeatured: false,
            displayOrder: 2,
            countryCode: "ER",
            countryName: "Eritrea"
        )
        let featured = try makePack(
            id: "featured-eritrea",
            packID: "eritrea-core",
            name: "Eritrea Core",
            version: "1.1.0",
            isFeatured: true,
            displayOrder: 1,
            countryCode: "ER",
            countryName: "Eritrea"
        )

        let selected = store.resolveCountryPack(
            packID: "eritrea-core",
            catalog: [fallback, featured]
        )

        #expect(selected?.id == featured.id)
    }

    @Test func resolveCountryPackPrefersNewerVersionWhenPriorityTies() throws {
        let older = try makePack(
            id: "eritrea-v1",
            packID: "eritrea-core",
            name: "Eritrea Core",
            version: "1.0.0",
            isFeatured: true,
            displayOrder: 1,
            countryCode: "ER",
            countryName: "Eritrea"
        )
        let newer = try makePack(
            id: "eritrea-v2",
            packID: "eritrea-core",
            name: "Eritrea Core",
            version: "1.2.0",
            isFeatured: true,
            displayOrder: 1,
            countryCode: "ER",
            countryName: "Eritrea"
        )

        let selected = store.resolveCountryPack(
            packID: "eritrea-core",
            catalog: [older, newer]
        )

        #expect(selected?.id == newer.id)
    }

    @Test func resolveCountryPackFallsBackToRuntimePackID() throws {
        let runtimeOnly = try makePack(
            id: "ethiopia-runtime",
            packID: nil,
            name: "Ethiopia Core",
            version: "1.0.0",
            isFeatured: false,
            displayOrder: 5,
            countryCode: "ET",
            countryName: "Ethiopia"
        )

        let selected = store.resolveCountryPack(
            packID: "ethiopia-runtime",
            catalog: [runtimeOnly]
        )

        #expect(selected?.id == runtimeOnly.id)
    }

    @Test func resolveCountryPackReturnsNilWhenIdentifierMissing() throws {
        let eritrea = try makePack(
            id: "eritrea-pack",
            packID: "eritrea-core",
            name: "Eritrea Core",
            version: "1.0.0",
            isFeatured: true,
            displayOrder: 1,
            countryCode: "ER",
            countryName: "Eritrea"
        )

        #expect(store.resolveCountryPack(packID: " ", catalog: [eritrea]) == nil)
        #expect(store.resolveCountryPack(packID: "missing-pack", catalog: [eritrea]) == nil)
    }

    private func makePack(
        id: String,
        packID: String?,
        name: String,
        version: String,
        isFeatured: Bool,
        displayOrder: Int,
        countryCode: String,
        countryName: String
    ) throws -> HubTilePack {
        var payload: [String: Any] = [
            "id": id,
            "name": name,
            "file_name": "\(id).mbtiles",
            "size_bytes": 1024,
            "min_zoom": 7,
            "max_zoom": 12,
            "attribution": "Semay",
            "download_url": "https://hub.example/api/tiles/packs/\(id)/download",
            "pack_version": version,
            "country_code": countryCode,
            "country_name": countryName,
            "is_featured": isFeatured,
            "display_order": displayOrder,
        ]
        if let packID {
            payload["pack_id"] = packID
        }

        let data = try JSONSerialization.data(withJSONObject: payload, options: [])
        return try JSONDecoder().decode(HubTilePack.self, from: data)
    }
}
