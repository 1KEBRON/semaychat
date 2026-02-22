import Foundation
import Testing
@testable import bitchat

struct HubTilePackDecodingTests {
    @Test func decodesPackIDAndArrayDependencies() throws {
        let raw = """
        {
          "id": "abc123",
          "pack_id": "eritrea-core",
          "name": "Eritrea Core",
          "file_name": "eritrea-core.mbtiles",
          "size_bytes": 1048576,
          "min_zoom": 7,
          "max_zoom": 12,
          "attribution": "Semay",
          "bounds": "36.3,12.3,43.1,18.2",
          "sha256": "deadbeef",
          "signature": "cafebabe",
          "sig_alg": "ed25519",
          "download_url": "https://hub.example/api/tiles/packs/1/download",
          "last_updated": "2026-02-18T00:00:00Z",
          "region_code": "er",
          "pack_version": "1.2.3",
          "tile_format": "raster",
          "depends_on": ["horn-core"],
          "style_url": "mapbox://styles/semay/er",
          "lang": "ti",
          "country_code": "ER",
          "country_name": "Eritrea",
          "is_featured": true,
          "display_order": 1,
          "download_size_bytes": 4096,
          "min_app_version": "1.5.1"
        }
        """

        let pack = try JSONDecoder().decode(HubTilePack.self, from: Data(raw.utf8))
        #expect(pack.id == "abc123")
        #expect(pack.packID == "eritrea-core")
        #expect(pack.dependsOn == ["horn-core"])
        #expect(pack.regionCode == "er")
        #expect(pack.packVersion == "1.2.3")
        #expect(pack.tileFormat == "raster")
        #expect(pack.styleURL == "mapbox://styles/semay/er")
        #expect(pack.languageCode == "ti")
        #expect(pack.countryCode == "ER")
        #expect(pack.countryName == "Eritrea")
        #expect(pack.isFeatured == true)
        #expect(pack.displayOrder == 1)
        #expect(pack.downloadSizeBytes == 4096)
        #expect(pack.minAppVersion == "1.5.1")
    }

    @Test func decodesCommaSeparatedDependencies() throws {
        let raw = """
        {
          "id": "xyz789",
          "name": "Asmara Detail",
          "file_name": "asmara-detail.mbtiles",
          "size_bytes": 2048,
          "min_zoom": 13,
          "max_zoom": 16,
          "attribution": "Semay",
          "download_url": "https://hub.example/api/tiles/packs/2/download",
          "depends_on": "horn-core, eritrea-core"
        }
        """

        let pack = try JSONDecoder().decode(HubTilePack.self, from: Data(raw.utf8))
        #expect(pack.packID == nil)
        #expect(pack.dependsOn == ["horn-core", "eritrea-core"])
    }
}
