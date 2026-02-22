import CryptoKit
import Foundation
import SQLite3
import Testing
@testable import bitchat

private let integritySQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

@Suite(.serialized)
@MainActor
struct OfflineTilePackIntegrityTests {
    @Test func validatesManifestSHA256WhenPresent() throws {
        let fileURL = try makeTempMBTiles()
        defer { try? FileManager.default.removeItem(at: fileURL.deletingLastPathComponent()) }

        let hash = try Data(contentsOf: fileURL).sha256Hash().hexEncodedString().lowercased()
        let result = try OfflineTileStore.shared.debugValidateImportedPackIntegrity(
            forMBTilesPath: fileURL.path,
            manifest: ["sha256": hash]
        )
        #expect(result.sha256 == hash)
    }

    @Test func rejectsManifestSHA256Mismatch() throws {
        let fileURL = try makeTempMBTiles()
        defer { try? FileManager.default.removeItem(at: fileURL.deletingLastPathComponent()) }

        #expect(throws: Error.self) {
            try OfflineTileStore.shared.debugValidateImportedPackIntegrity(
                forMBTilesPath: fileURL.path,
                manifest: ["sha256": String(repeating: "a", count: 64)]
            )
        }
    }

    @Test func signedPolicyRejectsUnsignedImport() throws {
        let fileURL = try makeTempMBTiles()
        defer { try? FileManager.default.removeItem(at: fileURL.deletingLastPathComponent()) }
        UserDefaults.standard.set(true, forKey: "semay.offline_maps.require_signed_packs")
        defer { UserDefaults.standard.set(false, forKey: "semay.offline_maps.require_signed_packs") }

        let hash = try Data(contentsOf: fileURL).sha256Hash().hexEncodedString().lowercased()
        #expect(throws: Error.self) {
            try OfflineTileStore.shared.debugValidateImportedPackIntegrity(
                forMBTilesPath: fileURL.path,
                manifest: ["sha256": hash]
            )
        }
    }

    @Test func validatesImportedManifestSignature() throws {
        let fileURL = try makeTempMBTiles()
        defer { try? FileManager.default.removeItem(at: fileURL.deletingLastPathComponent()) }

        let sha256 = try Data(contentsOf: fileURL).sha256Hash().hexEncodedString().lowercased()
        let signingKey = Curve25519.Signing.PrivateKey()
        let signature = try signingKey.signature(for: Data(sha256.utf8)).hexEncodedString()
        let nodeID = "node-\(UUID().uuidString)"
        let pubkeyHex = signingKey.publicKey.rawRepresentation.hexEncodedString()

        let result = try OfflineTileStore.shared.debugValidateImportedPackIntegrity(
            forMBTilesPath: fileURL.path,
            manifest: [
                "sha256": sha256,
                "signature": signature,
                "sig_alg": "ed25519",
                "node_id": nodeID,
                "signing_pubkey": pubkeyHex,
            ]
        )
        #expect(result.sha256 == sha256)
        #expect(result.signature == signature)
        #expect(result.sigAlg == "ed25519")
    }

    @Test func rejectsInvalidImportedManifestSignature() throws {
        let fileURL = try makeTempMBTiles()
        defer { try? FileManager.default.removeItem(at: fileURL.deletingLastPathComponent()) }

        let sha256 = try Data(contentsOf: fileURL).sha256Hash().hexEncodedString().lowercased()
        let signingKey = Curve25519.Signing.PrivateKey()
        var signatureBytes = [UInt8](try signingKey.signature(for: Data(sha256.utf8)))
        signatureBytes[0] ^= 0xFF
        let tamperedSignature = Data(signatureBytes).hexEncodedString()
        let nodeID = "node-\(UUID().uuidString)"
        let pubkeyHex = signingKey.publicKey.rawRepresentation.hexEncodedString()

        #expect(throws: Error.self) {
            try OfflineTileStore.shared.debugValidateImportedPackIntegrity(
                forMBTilesPath: fileURL.path,
                manifest: [
                    "sha256": sha256,
                    "signature": tamperedSignature,
                    "sig_alg": "ed25519",
                    "node_id": nodeID,
                    "signing_pubkey": pubkeyHex,
                ]
            )
        }
    }

    private func makeTempMBTiles() throws -> URL {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("semay-offline-integrity-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let mbtilesURL = root.appendingPathComponent("integrity-test.mbtiles")
        try createTestMBTiles(at: mbtilesURL.path)
        return mbtilesURL
    }

    private func createTestMBTiles(at path: String) throws {
        var db: OpaquePointer?
        guard sqlite3_open(path, &db) == SQLITE_OK else {
            throw sqliteError(db)
        }
        defer { sqlite3_close(db) }
        guard let db else {
            throw NSError(
                domain: "OfflineTilePackIntegrityTests",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Could not open test MBTiles database"]
            )
        }

        try execute("""
            CREATE TABLE metadata (name TEXT, value TEXT);
            CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
            INSERT INTO metadata(name, value) VALUES ('name', 'Integrity Test');
            INSERT INTO metadata(name, value) VALUES ('minzoom', '0');
            INSERT INTO metadata(name, value) VALUES ('maxzoom', '12');
            INSERT INTO metadata(name, value) VALUES ('bounds', '36.3,12.3,43.1,18.2');
            """, on: db)

        let tileData = Data([0x89, 0x50, 0x4E, 0x47, 0xAA, 0xBB, 0xCC, 0xDD, 0x49, 0x45, 0x4E, 0x44])
        let sql = "INSERT INTO tiles(zoom_level, tile_column, tile_row, tile_data) VALUES(0, 0, 0, ?)"
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            sqlite3_finalize(stmt)
            throw sqliteError(db)
        }
        defer { sqlite3_finalize(stmt) }
        _ = tileData.withUnsafeBytes { bytes in
            sqlite3_bind_blob(stmt, 1, bytes.baseAddress, Int32(tileData.count), integritySQLITE_TRANSIENT)
        }
        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw sqliteError(db)
        }
    }

    private func execute(_ sql: String, on db: OpaquePointer) throws {
        var errorMessage: UnsafeMutablePointer<Int8>?
        if sqlite3_exec(db, sql, nil, nil, &errorMessage) != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) } ?? "SQLite error"
            sqlite3_free(errorMessage)
            throw NSError(
                domain: "OfflineTilePackIntegrityTests",
                code: Int(sqlite3_errcode(db)),
                userInfo: [NSLocalizedDescriptionKey: message]
            )
        }
    }

    private func sqliteError(_ db: OpaquePointer?) -> Error {
        let message = db.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "SQLite error"
        return NSError(
            domain: "OfflineTilePackIntegrityTests",
            code: Int(db.map { sqlite3_errcode($0) } ?? SQLITE_ERROR),
            userInfo: [NSLocalizedDescriptionKey: message]
        )
    }
}
