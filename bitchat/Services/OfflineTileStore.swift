import CryptoKit
import Foundation
import SQLite3

private let selectedPackKey = "semay.offlineTiles.selectedPath"
private let hubBaseURLKey = "semay.hub.base_url"
private let hubIngestTokenKey = "semay.hub.ingest_token"
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
private let defaultHubCandidates = [
    "https://hub.semay.app",
    "http://semayhub.local:5000",
    "http://semayhub.local:5055",
    "http://localhost:5000",
    "http://localhost:5055",
    "http://127.0.0.1:5000",
    "http://127.0.0.1:5055",
]

struct OfflineTilePack: Equatable {
    let name: String
    let path: String
    let minZoom: Int
    let maxZoom: Int
    let attribution: String
    let bounds: PackBounds?
    let sizeBytes: Int64
}

struct PackBounds: Equatable {
    let minLon: Double
    let minLat: Double
    let maxLon: Double
    let maxLat: Double

    func contains(lat: Double, lon: Double) -> Bool {
        lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon
    }
}

struct HubTilePack: Decodable, Equatable, Identifiable {
    let id: String
    let name: String
    let fileName: String
    let sizeBytes: Int64
    let minZoom: Int
    let maxZoom: Int
    let attribution: String
    let bounds: String?
    let sha256: String?
    let signature: String?
    let sigAlg: String?
    let downloadURL: String
    let lastUpdated: String?

    enum CodingKeys: String, CodingKey {
        case id
        case name
        case fileName = "file_name"
        case sizeBytes = "size_bytes"
        case minZoom = "min_zoom"
        case maxZoom = "max_zoom"
        case attribution
        case bounds
        case sha256
        case signature
        case sigAlg = "sig_alg"
        case downloadURL = "download_url"
        case lastUpdated = "last_updated"
    }

    var parsedBounds: PackBounds? {
        parsePackBounds(bounds)
    }
}

private struct HubTileCatalogResponse: Decodable {
    let generatedAt: Int
    let packs: [HubTilePack]

    enum CodingKeys: String, CodingKey {
        case generatedAt = "generated_at"
        case packs
    }
}

private struct HubUploadResponse: Decodable {
    let success: Bool
    let pack: HubTilePack
}

@MainActor
final class OfflineTileStore: ObservableObject {
    static let shared = OfflineTileStore()

    @Published private(set) var availablePack: OfflineTilePack?
    @Published private(set) var packs: [OfflineTilePack] = []
    @Published private(set) var activeMapSourceBaseURL: String?
    @Published private(set) var activeNodeName: String?

    private var cachedNodeDescriptor: SemayNodeDescriptor?

    private init() {
        activeMapSourceBaseURL = configuredHubBaseURL()?.absoluteString
        refresh()
    }

    func reloadSourceConfig() {
        activeMapSourceBaseURL = configuredHubBaseURL()?.absoluteString
        activeNodeName = nil
        cachedNodeDescriptor = nil
    }

    func refresh() {
        let discovered = discoverPacks()
        packs = discovered

        let selectedPath = UserDefaults.standard.string(forKey: selectedPackKey)
        if let selectedPath, let match = discovered.first(where: { $0.path == selectedPath }) {
            availablePack = match
        } else {
            availablePack = discovered.first
            if let chosen = availablePack {
                UserDefaults.standard.set(chosen.path, forKey: selectedPackKey)
            }
        }
    }

    func selectPack(_ pack: OfflineTilePack?) {
        availablePack = pack
        if let pack {
            UserDefaults.standard.set(pack.path, forKey: selectedPackKey)
        } else {
            UserDefaults.standard.removeObject(forKey: selectedPackKey)
        }
    }

    func importPack(from url: URL) throws -> OfflineTilePack {
        let targetURL = try copyPackToLocalStorage(from: url, preferredName: url.lastPathComponent)
        guard let pack = readPackMetadata(path: targetURL.path) else {
            throw NSError(domain: "OfflineTileStore", code: 2, userInfo: [NSLocalizedDescriptionKey: "Invalid MBTiles file"])
        }
        refresh()
        selectPack(pack)
        return pack
    }

    func fetchHubCatalog() async throws -> [HubTilePack] {
        let baseURL = try await resolveHubBaseURL()
        let endpoint = baseURL.appendingPathComponent("api/tiles/packs")
        let request = authorizedRequest(url: endpoint, method: "GET")

        let (data, response) = try await URLSession.shared.data(for: request)
        try ensureSuccessStatus(response: response, data: data)

        let catalog = try JSONDecoder().decode(HubTileCatalogResponse.self, from: data)
        _ = catalog.generatedAt
        return catalog.packs
    }

    @discardableResult
    func installHubPack(_ pack: HubTilePack) async throws -> OfflineTilePack {
        let baseURL = try await resolveHubBaseURL()
        guard let downloadURL = resolveHubDownloadURL(pack.downloadURL, baseURL: baseURL) else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 11,
                userInfo: [NSLocalizedDescriptionKey: "Invalid pack download URL"]
            )
        }

        let request = authorizedRequest(url: downloadURL, method: "GET")
        let (tempURL, response) = try await URLSession.shared.download(for: request)
        try ensureSuccessStatus(response: response, data: nil)

        if let expectedHash = pack.sha256, !expectedHash.isEmpty {
            let computedHash = try sha256File(at: tempURL)
            if computedHash.lowercased() != expectedHash.lowercased() {
                throw NSError(
                    domain: "OfflineTileStore",
                    code: 12,
                    userInfo: [NSLocalizedDescriptionKey: "Pack hash mismatch during download verification"]
                )
            }
        }

        try await verifyPackSignatureIfPresent(pack, baseURL: baseURL)

        let targetURL = try copyPackToLocalStorage(from: tempURL, preferredName: pack.fileName)
        guard let installed = readPackMetadata(path: targetURL.path) else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 13,
                userInfo: [NSLocalizedDescriptionKey: "Downloaded file is not a valid MBTiles pack"]
            )
        }

        refresh()
        selectPack(installed)
        return installed
    }

    @discardableResult
    func installRecommendedPack() async throws -> OfflineTilePack {
        let packs = try await fetchHubCatalog()
        guard let preferred = Self.preferredCommunityPack(from: packs) else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 40,
                userInfo: [NSLocalizedDescriptionKey: "No offline maps are available right now."]
            )
        }
        return try await installHubPack(preferred)
    }

    func deletePack(_ pack: OfflineTilePack) {
        do {
            try FileManager.default.removeItem(atPath: pack.path)
        } catch {
            // Ignore delete failures; refresh anyway.
        }
        refresh()
    }

    func deleteAllPacks() {
        for pack in packs {
            try? FileManager.default.removeItem(atPath: pack.path)
        }
        refresh()
    }

    @discardableResult
    func publishPackToHub(_ pack: OfflineTilePack) async throws -> HubTilePack {
        let baseURL = try await resolveHubBaseURL()

        let endpoint = baseURL.appendingPathComponent("api/tiles/packs/upload")
        var request = authorizedRequest(url: endpoint, method: "POST")
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        request.setValue(URL(fileURLWithPath: pack.path).lastPathComponent, forHTTPHeaderField: "X-Tile-Filename")

        let sourceURL = URL(fileURLWithPath: pack.path)
        let (data, response) = try await URLSession.shared.upload(for: request, fromFile: sourceURL)
        try ensureSuccessStatus(response: response, data: data)

        let decoded = try JSONDecoder().decode(HubUploadResponse.self, from: data)
        return decoded.pack
    }

    func discoverMapSourceURL() async throws -> String {
        let url = try await resolveHubBaseURL(forceDiscovery: true)
        return url.absoluteString
    }

    func discoverHubBaseURL() async throws -> String {
        try await discoverMapSourceURL()
    }

    func bestPack(forLatitude lat: Double, longitude lon: Double) -> OfflineTilePack? {
        let bounded = packs.filter { $0.bounds != nil }
        guard !bounded.isEmpty else { return nil }
        return bounded.first(where: { $0.bounds?.contains(lat: lat, lon: lon) == true })
    }

    private func discoverPacks() -> [OfflineTilePack] {
        let fm = FileManager.default
        let docs = fm.urls(for: .documentDirectory, in: .userDomainMask).first
        let tilesDir = docs?.appendingPathComponent("tiles", isDirectory: true)
        let candidateFiles: [URL]
        if let tilesDir, fm.fileExists(atPath: tilesDir.path) {
            candidateFiles = (try? fm.contentsOfDirectory(at: tilesDir, includingPropertiesForKeys: nil)) ?? []
        } else if let docs {
            candidateFiles = (try? fm.contentsOfDirectory(at: docs, includingPropertiesForKeys: nil)) ?? []
        } else {
            candidateFiles = []
        }

        let mbtiles = candidateFiles.filter { $0.pathExtension.lowercased() == "mbtiles" }
        return mbtiles.compactMap { readPackMetadata(path: $0.path) }
    }

    private func readPackMetadata(path: String) -> OfflineTilePack? {
        var db: OpaquePointer?
        guard sqlite3_open(path, &db) == SQLITE_OK else {
            if db != nil { sqlite3_close(db) }
            return nil
        }
        defer { sqlite3_close(db) }

        func queryMetadataValue(_ key: String) -> String? {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = "SELECT value FROM metadata WHERE name = ? LIMIT 1"
            guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else { return nil }
            sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT)
            guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
            guard let cString = sqlite3_column_text(stmt, 0) else { return nil }
            return String(cString: cString)
        }

        let name = queryMetadataValue("name") ?? URL(fileURLWithPath: path).deletingPathExtension().lastPathComponent
        let minZoom = Int(queryMetadataValue("minzoom") ?? "0") ?? 0
        let maxZoom = Int(queryMetadataValue("maxzoom") ?? "16") ?? 16
        let attribution = queryMetadataValue("attribution") ?? "© OpenStreetMap contributors"
        let bounds = parseBounds(queryMetadataValue("bounds"))

        let sizeBytes = fileSize(path: path)
        return OfflineTilePack(
            name: name,
            path: path,
            minZoom: minZoom,
            maxZoom: maxZoom,
            attribution: attribution,
            bounds: bounds,
            sizeBytes: sizeBytes
        )
    }

    private func parseBounds(_ raw: String?) -> PackBounds? {
        parsePackBounds(raw)
    }

    private func fileSize(path: String) -> Int64 {
        let attrs = try? FileManager.default.attributesOfItem(atPath: path)
        if let size = attrs?[.size] as? NSNumber {
            return size.int64Value
        }
        return 0
    }

    private func documentsTileDirectory() throws -> URL {
        let fm = FileManager.default
        guard let docs = fm.urls(for: .documentDirectory, in: .userDomainMask).first else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 20,
                userInfo: [NSLocalizedDescriptionKey: "Documents directory unavailable"]
            )
        }
        let tilesDir = docs.appendingPathComponent("tiles", isDirectory: true)
        try fm.createDirectory(at: tilesDir, withIntermediateDirectories: true)
        return tilesDir
    }

    private func copyPackToLocalStorage(from sourceURL: URL, preferredName: String) throws -> URL {
        let fm = FileManager.default
        let tilesDir = try documentsTileDirectory()
        let sanitizedName = sanitizePackFileName(preferredName)
        let targetURL = tilesDir.appendingPathComponent(sanitizedName)

        if fm.fileExists(atPath: targetURL.path) {
            try fm.removeItem(at: targetURL)
        }

        try fm.copyItem(at: sourceURL, to: targetURL)
        return targetURL
    }

    private func sanitizePackFileName(_ originalName: String) -> String {
        let base = URL(fileURLWithPath: originalName).lastPathComponent
        if base.lowercased().hasSuffix(".mbtiles") {
            return base
        }
        return "\(base).mbtiles"
    }

    private func configuredHubBaseURL() -> URL? {
        let raw = UserDefaults.standard.string(forKey: hubBaseURLKey)?.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let raw, !raw.isEmpty else { return nil }
        return URL(string: raw)
    }

    private func hubIngestToken() -> String {
        UserDefaults.standard.string(forKey: hubIngestTokenKey)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    }

    private func authorizedRequest(url: URL, method: String) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.timeoutInterval = 120
        let token = hubIngestToken()
        if !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        return request
    }

    private func resolveHubDownloadURL(_ raw: String, baseURL: URL) -> URL? {
        if let absolute = URL(string: raw), absolute.scheme != nil {
            return absolute
        }
        return URL(string: raw, relativeTo: baseURL)?.absoluteURL
    }

    private func ensureSuccessStatus(response: URLResponse, data: Data?) throws {
        guard let http = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }
        guard (200...299).contains(http.statusCode) else {
            let message = data.flatMap { String(data: $0, encoding: .utf8) } ?? ""
            throw NSError(
                domain: "OfflineTileStore",
                code: http.statusCode,
                userInfo: [NSLocalizedDescriptionKey: "Offline map request failed (\(http.statusCode)): \(message)"]
            )
        }
    }

    private func sha256File(at fileURL: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: fileURL)
        defer { try? handle.close() }

        var hasher = SHA256()
        while true {
            let data = try handle.read(upToCount: 1024 * 1024) ?? Data()
            if data.isEmpty {
                break
            }
            hasher.update(data: data)
        }
        return hasher.finalize().map { String(format: "%02x", $0) }.joined()
    }

    private static func preferredCommunityPack(from packs: [HubTilePack]) -> HubTilePack? {
        if let combined = packs.first(where: {
            let n = $0.name.lowercased()
            return n.contains("eritrea") && n.contains("ethiopia")
        }) {
            return combined
        }
        if let horn = packs.first(where: { $0.name.lowercased().contains("horn") }) {
            return horn
        }
        return packs.first
    }

    private func resolveHubBaseURL(forceDiscovery: Bool = false) async throws -> URL {
        var configuredAlreadyTested = false
        if !forceDiscovery, let configured = configuredHubBaseURL() {
            configuredAlreadyTested = true
            if await canReachHubCatalog(baseURL: configured) {
                activeMapSourceBaseURL = configured.absoluteString
                cachedNodeDescriptor = try? await fetchNodeDescriptor(baseURL: configured)
                activeNodeName = cachedNodeDescriptor?.nodeName
                return configured
            }
        }

        var seen = Set<String>()
        var candidates: [URL] = []
        if let configured = configuredHubBaseURL(), !configuredAlreadyTested {
            candidates.append(configured)
            seen.insert(configured.absoluteString)
        }
        for raw in defaultHubCandidates {
            guard let url = URL(string: raw) else { continue }
            if seen.contains(url.absoluteString) {
                continue
            }
            seen.insert(url.absoluteString)
            candidates.append(url)
        }

        for candidate in candidates {
            if await canReachHubCatalog(baseURL: candidate) {
                UserDefaults.standard.set(candidate.absoluteString, forKey: hubBaseURLKey)
                activeMapSourceBaseURL = candidate.absoluteString
                cachedNodeDescriptor = try? await fetchNodeDescriptor(baseURL: candidate)
                activeNodeName = cachedNodeDescriptor?.nodeName
                return candidate
            }
        }

        throw NSError(
            domain: "OfflineTileStore",
            code: 30,
            userInfo: [NSLocalizedDescriptionKey: "Offline maps aren't available right now. You can keep using Semay online, or connect to a Semay node and try again."]
        )
    }

    private func canReachHubCatalog(baseURL: URL) async -> Bool {
        let endpoint = baseURL.appendingPathComponent("api/tiles/packs")
        var request = authorizedRequest(url: endpoint, method: "GET")
        request.timeoutInterval = 6
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else { return false }
            guard (200...299).contains(http.statusCode) else { return false }
            let decoded = try JSONDecoder().decode(HubTileCatalogResponse.self, from: data)
            _ = decoded.generatedAt
            return true
        } catch {
            return false
        }
    }

    private func fetchNodeDescriptor(baseURL: URL) async throws -> SemayNodeDescriptor {
        let endpoint = baseURL.appendingPathComponent("api/node")
        let request = authorizedRequest(url: endpoint, method: "GET")

        let (data, response) = try await URLSession.shared.data(for: request)
        try ensureSuccessStatus(response: response, data: data)
        return try JSONDecoder().decode(SemayNodeDescriptor.self, from: data)
    }

    private func verifyPackSignatureIfPresent(_ pack: HubTilePack, baseURL: URL) async throws {
        guard let sha256 = pack.sha256?.trimmingCharacters(in: .whitespacesAndNewlines),
              !sha256.isEmpty else { return }
        guard let signatureHex = pack.signature?.trimmingCharacters(in: .whitespacesAndNewlines),
              !signatureHex.isEmpty else { return }

        let descriptor: SemayNodeDescriptor?
        if let cached = cachedNodeDescriptor {
            descriptor = cached
        } else {
            let fetched = try? await fetchNodeDescriptor(baseURL: baseURL)
            cachedNodeDescriptor = fetched
            descriptor = fetched
        }
        guard let descriptor else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 14,
                userInfo: [NSLocalizedDescriptionKey: "Pack signature present but node signing key is unavailable"]
            )
        }

        let pubkeyHex = descriptor.signingPubkey?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let alg = descriptor.signingAlg?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !pubkeyHex.isEmpty, alg.lowercased() == "ed25519" else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 14,
                userInfo: [NSLocalizedDescriptionKey: "Pack signature present but node signing key is unavailable"]
            )
        }

        try SemayNodeTrustStore.shared.enforceTrustedSigningPubkey(nodeID: descriptor.nodeID, pubkeyHex: pubkeyHex)

        guard let pubData = Data(hexString: pubkeyHex), pubData.count == 32,
              let sigData = Data(hexString: signatureHex), sigData.count == 64 else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 15,
                userInfo: [NSLocalizedDescriptionKey: "Invalid pack signature encoding"]
            )
        }

        let message = Data(sha256.lowercased().utf8)
        let pubkey = try Curve25519.Signing.PublicKey(rawRepresentation: pubData)
        if !pubkey.isValidSignature(sigData, for: message) {
            throw NSError(
                domain: "OfflineTileStore",
                code: 16,
                userInfo: [NSLocalizedDescriptionKey: "Pack signature verification failed"]
            )
        }
    }
}

private func parsePackBounds(_ raw: String?) -> PackBounds? {
    guard let raw, !raw.isEmpty else { return nil }
    let parts = raw.split(separator: ",").map { String($0) }
    guard parts.count == 4,
          let minLon = Double(parts[0].trimmingCharacters(in: .whitespaces)),
          let minLat = Double(parts[1].trimmingCharacters(in: .whitespaces)),
          let maxLon = Double(parts[2].trimmingCharacters(in: .whitespaces)),
          let maxLat = Double(parts[3].trimmingCharacters(in: .whitespaces)) else {
        return nil
    }
    return PackBounds(minLon: minLon, minLat: minLat, maxLon: maxLon, maxLat: maxLat)
}
