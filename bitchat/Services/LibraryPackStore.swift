import CryptoKit
import Foundation

private let hubBaseURLKey = "semay.hub.base_url"
private let hubIngestTokenKey = "semay.hub.ingest_token"
private let libraryDirectoryName = "library"
private let defaultHubCandidates = [
    "https://hub.semay.app",
    "http://semayhub.local:5000",
    "http://semayhub.local:5055",
    "http://localhost:5000",
    "http://localhost:5055",
    "http://127.0.0.1:5000",
    "http://127.0.0.1:5055",
]

struct HubLibraryPack: Decodable, Equatable, Identifiable {
    let id: String
    let name: String
    let fileName: String
    let sizeBytes: Int64
    let languages: [String]?
    let itemCount: Int
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
        case languages
        case itemCount = "item_count"
        case sha256
        case signature
        case sigAlg = "sig_alg"
        case downloadURL = "download_url"
        case lastUpdated = "last_updated"
    }
}

private struct HubLibraryCatalogResponse: Decodable {
    let generatedAt: Int
    let packs: [HubLibraryPack]

    enum CodingKeys: String, CodingKey {
        case generatedAt = "generated_at"
        case packs
    }
}

struct SemayLibraryPackManifest: Codable, Equatable {
    let packName: String
    let languages: [String]?
    let items: [SemayLibraryItem]

    enum CodingKeys: String, CodingKey {
        case packName = "pack_name"
        case languages
        case items
    }
}

struct SemayLibraryItem: Codable, Identifiable, Equatable {
    let itemID: String
    let title: String
    let language: String?
    let contentMarkdown: String

    var id: String { itemID }

    enum CodingKeys: String, CodingKey {
        case itemID = "item_id"
        case title
        case language
        case contentMarkdown = "content_md"
    }
}

struct InstalledLibraryPack: Equatable, Identifiable {
    let packName: String
    let path: String
    let sizeBytes: Int64
    let languages: [String]?
    let itemCount: Int
    let manifest: SemayLibraryPackManifest

    var id: String { path }
}

@MainActor
final class LibraryPackStore: ObservableObject {
    static let shared = LibraryPackStore()

    @Published private(set) var packs: [InstalledLibraryPack] = []
    @Published private(set) var items: [SemayLibraryItem] = []
    @Published private(set) var activeSourceBaseURL: String?

    private var cachedNodeDescriptor: SemayNodeDescriptor?

    private init() {
        activeSourceBaseURL = configuredHubBaseURL()?.absoluteString
        refresh()
    }

    func refresh() {
        let discovered = discoverPacks()
        packs = discovered
        items = discovered.flatMap { $0.manifest.items }
    }

    func fetchCatalog() async throws -> [HubLibraryPack] {
        let baseURL = try await resolveHubBaseURL()
        let endpoint = baseURL.appendingPathComponent("api/library/packs")
        let request = authorizedRequest(url: endpoint, method: "GET")

        let (data, response) = try await URLSession.shared.data(for: request)
        try ensureSuccessStatus(response: response, data: data)

        let decoded = try JSONDecoder().decode(HubLibraryCatalogResponse.self, from: data)
        _ = decoded.generatedAt
        return decoded.packs
    }

    @discardableResult
    func installHubPack(_ pack: HubLibraryPack) async throws -> InstalledLibraryPack {
        let baseURL = try await resolveHubBaseURL()
        guard let downloadURL = resolveDownloadURL(pack.downloadURL, baseURL: baseURL) else {
            throw NSError(
                domain: "LibraryPackStore",
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
                    domain: "LibraryPackStore",
                    code: 12,
                    userInfo: [NSLocalizedDescriptionKey: "Pack hash mismatch during download verification"]
                )
            }
        }

        try await verifyPackSignatureIfPresent(pack, baseURL: baseURL)

        let targetURL = try copyPackToLocalStorage(from: tempURL, preferredName: pack.fileName)
        let installed = try readInstalledPack(path: targetURL.path)

        refresh()
        return installed
    }

    @discardableResult
    func installRecommendedPack() async throws -> InstalledLibraryPack {
        let packs = try await fetchCatalog()
        guard let first = packs.first else {
            throw NSError(
                domain: "LibraryPackStore",
                code: 40,
                userInfo: [NSLocalizedDescriptionKey: "No library packs are available right now."]
            )
        }
        return try await installHubPack(first)
    }

    // MARK: - Local storage

    private func discoverPacks() -> [InstalledLibraryPack] {
        let fm = FileManager.default
        guard let dir = try? documentsLibraryDirectory() else { return [] }
        let files = (try? fm.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil)) ?? []
        let jsonFiles = files.filter { $0.pathExtension.lowercased() == "json" }
        return jsonFiles.compactMap { try? readInstalledPack(path: $0.path) }
    }

    private func readInstalledPack(path: String) throws -> InstalledLibraryPack {
        let url = URL(fileURLWithPath: path)
        let data = try Data(contentsOf: url)
        let manifest = try JSONDecoder().decode(SemayLibraryPackManifest.self, from: data)

        let attrs = try? FileManager.default.attributesOfItem(atPath: path)
        let sizeBytes = (attrs?[.size] as? NSNumber)?.int64Value ?? Int64(data.count)

        return InstalledLibraryPack(
            packName: manifest.packName,
            path: path,
            sizeBytes: sizeBytes,
            languages: manifest.languages,
            itemCount: manifest.items.count,
            manifest: manifest
        )
    }

    private func documentsLibraryDirectory() throws -> URL {
        let fm = FileManager.default
        guard let docs = fm.urls(for: .documentDirectory, in: .userDomainMask).first else {
            throw NSError(
                domain: "LibraryPackStore",
                code: 20,
                userInfo: [NSLocalizedDescriptionKey: "Documents directory unavailable"]
            )
        }
        let dir = docs.appendingPathComponent(libraryDirectoryName, isDirectory: true)
        try fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    private func copyPackToLocalStorage(from sourceURL: URL, preferredName: String) throws -> URL {
        let fm = FileManager.default
        let dir = try documentsLibraryDirectory()
        let sanitized = sanitizePackFileName(preferredName)
        let targetURL = dir.appendingPathComponent(sanitized)

        if fm.fileExists(atPath: targetURL.path) {
            try fm.removeItem(at: targetURL)
        }

        try fm.copyItem(at: sourceURL, to: targetURL)
        return targetURL
    }

    private func sanitizePackFileName(_ originalName: String) -> String {
        let base = URL(fileURLWithPath: originalName).lastPathComponent
        if base.lowercased().hasSuffix(".json") {
            return base
        }
        return "\(base).json"
    }

    // MARK: - Source resolution + auth

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

    private func resolveDownloadURL(_ raw: String, baseURL: URL) -> URL? {
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
                domain: "LibraryPackStore",
                code: http.statusCode,
                userInfo: [NSLocalizedDescriptionKey: "Library request failed (\(http.statusCode)): \(message)"]
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

    private func fetchNodeDescriptor(baseURL: URL) async throws -> SemayNodeDescriptor {
        let endpoint = baseURL.appendingPathComponent("api/node")
        let request = authorizedRequest(url: endpoint, method: "GET")

        let (data, response) = try await URLSession.shared.data(for: request)
        try ensureSuccessStatus(response: response, data: data)
        return try JSONDecoder().decode(SemayNodeDescriptor.self, from: data)
    }

    private func verifyPackSignatureIfPresent(_ pack: HubLibraryPack, baseURL: URL) async throws {
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
                domain: "LibraryPackStore",
                code: 14,
                userInfo: [NSLocalizedDescriptionKey: "Pack signature present but node signing key is unavailable"]
            )
        }

        let pubkeyHex = descriptor.signingPubkey?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let alg = descriptor.signingAlg?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !pubkeyHex.isEmpty, alg.lowercased() == "ed25519" else {
            throw NSError(
                domain: "LibraryPackStore",
                code: 14,
                userInfo: [NSLocalizedDescriptionKey: "Pack signature present but node signing key is unavailable"]
            )
        }

        try SemayNodeTrustStore.shared.enforceTrustedSigningPubkey(nodeID: descriptor.nodeID, pubkeyHex: pubkeyHex)

        guard let pubData = Data(hexString: pubkeyHex), pubData.count == 32,
              let sigData = Data(hexString: signatureHex), sigData.count == 64 else {
            throw NSError(
                domain: "LibraryPackStore",
                code: 15,
                userInfo: [NSLocalizedDescriptionKey: "Invalid pack signature encoding"]
            )
        }

        let message = Data(sha256.lowercased().utf8)
        let pubkey = try Curve25519.Signing.PublicKey(rawRepresentation: pubData)
        if !pubkey.isValidSignature(sigData, for: message) {
            throw NSError(
                domain: "LibraryPackStore",
                code: 16,
                userInfo: [NSLocalizedDescriptionKey: "Pack signature verification failed"]
            )
        }
    }

    private func resolveHubBaseURL(forceDiscovery: Bool = false) async throws -> URL {
        var configuredAlreadyTested = false
        if !forceDiscovery, let configured = configuredHubBaseURL() {
            configuredAlreadyTested = true
            if await canReachLibraryCatalog(baseURL: configured) {
                activeSourceBaseURL = configured.absoluteString
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
            if await canReachLibraryCatalog(baseURL: candidate) {
                UserDefaults.standard.set(candidate.absoluteString, forKey: hubBaseURLKey)
                activeSourceBaseURL = candidate.absoluteString
                return candidate
            }
        }

        throw NSError(
            domain: "LibraryPackStore",
            code: 30,
            userInfo: [NSLocalizedDescriptionKey: "No reachable library source found. Connect to the internet and try again."]
        )
    }

    private func canReachLibraryCatalog(baseURL: URL) async -> Bool {
        let endpoint = baseURL.appendingPathComponent("api/library/packs")
        var request = authorizedRequest(url: endpoint, method: "GET")
        request.timeoutInterval = 6
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else { return false }
            guard (200...299).contains(http.statusCode) else { return false }
            let decoded = try JSONDecoder().decode(HubLibraryCatalogResponse.self, from: data)
            _ = decoded.generatedAt
            return true
        } catch {
            return false
        }
    }
}
