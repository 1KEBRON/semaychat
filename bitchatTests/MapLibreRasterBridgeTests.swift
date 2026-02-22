import Foundation
import SQLite3
import Testing
@testable import bitchat

private let testSQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

@Suite(.serialized)
struct MapLibreRasterBridgeTests {
    @Test func servesMBTilesThroughLocalhostBridge() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("semay-maplibre-bridge-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDirectory)
        }

        let mbtilesURL = tempDirectory.appendingPathComponent("bridge-test.mbtiles")
        let tileData = Data([0x89, 0x50, 0x4E, 0x47, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44])
        try createTestMBTiles(at: mbtilesURL.path, tileData: tileData)

        let pack = OfflineTilePack(
            packID: "bridge-test",
            regionCode: "er",
            packVersion: "1.0.0",
            tileFormat: .raster,
            dependsOn: [],
            styleURL: nil,
            languageCode: "en",
            name: "Bridge Test",
            path: mbtilesURL.path,
            minZoom: 0,
            maxZoom: 0,
            attribution: "© Test",
            bounds: nil,
            sizeBytes: Int64(tileData.count),
            lifecycleState: .installed
        )

        try MapLibreRasterBridge.shared.prepare(pack: pack)
        let styleURL = try #require(MapLibreRasterBridge.shared.styleURL())
        let styleData = try Data(contentsOf: styleURL)
        let style = try #require(try JSONSerialization.jsonObject(with: styleData) as? [String: Any])
        let sources = try #require(style["sources"] as? [String: Any])
        let source = try #require(sources["semay-offline-raster"] as? [String: Any])
        let tiles = try #require(source["tiles"] as? [String])
        let tileTemplate = try #require(tiles.first)
        #expect(tileTemplate.contains("127.0.0.1"))

        let tileURLString = tileTemplate
            .replacingOccurrences(of: "{z}", with: "0")
            .replacingOccurrences(of: "{x}", with: "0")
            .replacingOccurrences(of: "{y}", with: "0")
        let tileURL = try #require(URL(string: tileURLString))
        let (fetchedData, response) = try await URLSession.shared.data(from: tileURL)
        let http = try #require(response as? HTTPURLResponse)
        #expect(http.statusCode == 200)
        #expect(fetchedData == tileData)
    }

    @Test func fallsBackToDependencyPackWhenTopPackMissingTile() async throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("semay-maplibre-bridge-chain-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        defer {
            try? FileManager.default.removeItem(at: tempDirectory)
        }

        let coreURL = tempDirectory.appendingPathComponent("eritrea-core.mbtiles")
        let detailURL = tempDirectory.appendingPathComponent("asmara-detail.mbtiles")
        let coreTile = Data([0x89, 0x50, 0x4E, 0x47, 0xAA, 0xBB, 0xCC, 0xDD, 0x49, 0x45, 0x4E, 0x44])
        try createTestMBTiles(at: coreURL.path, tileData: coreTile)
        try createTestMBTiles(at: detailURL.path, tileData: nil)

        let core = OfflineTilePack(
            packID: "eritrea-core",
            regionCode: "er",
            packVersion: "1.0.0",
            tileFormat: .raster,
            dependsOn: [],
            styleURL: nil,
            languageCode: "en",
            name: "Eritrea Core",
            path: coreURL.path,
            minZoom: 0,
            maxZoom: 12,
            attribution: "© Test",
            bounds: nil,
            sizeBytes: Int64(coreTile.count),
            lifecycleState: .installed
        )

        let detail = OfflineTilePack(
            packID: "asmara-detail",
            regionCode: "er",
            packVersion: "1.0.0",
            tileFormat: .raster,
            dependsOn: ["eritrea-core"],
            styleURL: nil,
            languageCode: "en",
            name: "Asmara Detail",
            path: detailURL.path,
            minZoom: 13,
            maxZoom: 16,
            attribution: "© Test",
            bounds: nil,
            sizeBytes: 0,
            lifecycleState: .installed
        )

        try MapLibreRasterBridge.shared.prepare(packs: [core, detail])
        let styleURL = try #require(MapLibreRasterBridge.shared.styleURL())
        let styleData = try Data(contentsOf: styleURL)
        let style = try #require(try JSONSerialization.jsonObject(with: styleData) as? [String: Any])
        let sources = try #require(style["sources"] as? [String: Any])
        let source = try #require(sources["semay-offline-raster"] as? [String: Any])
        let tiles = try #require(source["tiles"] as? [String])
        let tileTemplate = try #require(tiles.first)

        let tileURLString = tileTemplate
            .replacingOccurrences(of: "{z}", with: "0")
            .replacingOccurrences(of: "{x}", with: "0")
            .replacingOccurrences(of: "{y}", with: "0")
        let tileURL = try #require(URL(string: tileURLString))
        let (fetchedData, response) = try await URLSession.shared.data(from: tileURL)
        let http = try #require(response as? HTTPURLResponse)
        #expect(http.statusCode == 200)
        #expect(fetchedData == coreTile)
    }

    private func createTestMBTiles(at path: String, tileData: Data?) throws {
        var db: OpaquePointer?
        guard sqlite3_open(path, &db) == SQLITE_OK else {
            throw sqliteError(db)
        }
        defer { sqlite3_close(db) }

        guard let db else {
            throw NSError(
                domain: "MapLibreRasterBridgeTests",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Could not open test MBTiles database"]
            )
        }

        try execute("""
            CREATE TABLE metadata (name TEXT, value TEXT);
            CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
            INSERT INTO metadata(name, value) VALUES ('name', 'Bridge Test');
            INSERT INTO metadata(name, value) VALUES ('minzoom', '0');
            INSERT INTO metadata(name, value) VALUES ('maxzoom', '0');
            """, on: db)

        if let tileData {
            let sql = "INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data) VALUES(0, 0, 0, ?)"
            var stmt: OpaquePointer?
            guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
                sqlite3_finalize(stmt)
                throw sqliteError(db)
            }
            defer { sqlite3_finalize(stmt) }

            _ = tileData.withUnsafeBytes { bytes in
                sqlite3_bind_blob(stmt, 1, bytes.baseAddress, Int32(tileData.count), testSQLITE_TRANSIENT)
            }
            guard sqlite3_step(stmt) == SQLITE_DONE else {
                throw sqliteError(db)
            }
        }
    }

    private func execute(_ sql: String, on db: OpaquePointer) throws {
        var errorMessage: UnsafeMutablePointer<Int8>?
        if sqlite3_exec(db, sql, nil, nil, &errorMessage) != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) } ?? "SQLite error"
            sqlite3_free(errorMessage)
            throw NSError(
                domain: "MapLibreRasterBridgeTests",
                code: Int(sqlite3_errcode(db)),
                userInfo: [NSLocalizedDescriptionKey: message]
            )
        }
    }

    private func sqliteError(_ db: OpaquePointer?) -> Error {
        let message = db.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "SQLite error"
        return NSError(
            domain: "MapLibreRasterBridgeTests",
            code: Int(db.map { sqlite3_errcode($0) } ?? SQLITE_ERROR),
            userInfo: [NSLocalizedDescriptionKey: message]
        )
    }
}
