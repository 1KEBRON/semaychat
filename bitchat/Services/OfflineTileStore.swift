import CryptoKit
import Foundation
import MapKit
import SQLite3

private let selectedPackKey = "semay.offlineTiles.selectedPath"
private let seededStarterKey = "semay.offlineTiles.seededStarter"
private let hubBaseURLKey = "semay.hub.base_url"
private let hubIngestTokenKey = "semay.hub.ingest_token"
private let requireSignedOfflinePacksKey = "semay.offline_maps.require_signed_packs"
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
private let bundledStarterTilesName = "semay-starter-asmara"
private let bundledStarterTilesExtension = "mbtiles"
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
    enum TileFormat: String, Equatable {
        case raster
        case vector
        case unknown
    }

    enum LifecycleState: String, Equatable {
        case discovered
        case verified
        case installed
        case active
        case superseded
    }

    let packID: String
    let regionCode: String?
    let packVersion: String?
    let tileFormat: TileFormat
    let dependsOn: [String]
    let styleURL: String?
    let languageCode: String?
    let name: String
    let path: String
    let minZoom: Int
    let maxZoom: Int
    let attribution: String
    let bounds: PackBounds?
    let sizeBytes: Int64
    let lifecycleState: LifecycleState
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
    let packID: String?
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
    let regionCode: String?
    let packVersion: String?
    let tileFormat: String?
    let dependsOn: [String]?
    let styleURL: String?
    let languageCode: String?
    let countryCode: String?
    let countryName: String?
    let isFeatured: Bool?
    let displayOrder: Int?
    let downloadSizeBytes: Int64?
    let minAppVersion: String?

    enum CodingKeys: String, CodingKey {
        case id
        case packID = "pack_id"
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
        case regionCode = "region_code"
        case packVersion = "pack_version"
        case tileFormat = "tile_format"
        case dependsOn = "depends_on"
        case styleURL = "style_url"
        case languageCode = "lang"
        case countryCode = "country_code"
        case countryName = "country_name"
        case isFeatured = "is_featured"
        case displayOrder = "display_order"
        case downloadSizeBytes = "download_size_bytes"
        case minAppVersion = "min_app_version"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(String.self, forKey: .id)
        packID = try container.decodeIfPresent(String.self, forKey: .packID)
        name = try container.decode(String.self, forKey: .name)
        fileName = try container.decode(String.self, forKey: .fileName)
        sizeBytes = try container.decode(Int64.self, forKey: .sizeBytes)
        minZoom = try container.decode(Int.self, forKey: .minZoom)
        maxZoom = try container.decode(Int.self, forKey: .maxZoom)
        attribution = try container.decode(String.self, forKey: .attribution)
        bounds = try container.decodeIfPresent(String.self, forKey: .bounds)
        sha256 = try container.decodeIfPresent(String.self, forKey: .sha256)
        signature = try container.decodeIfPresent(String.self, forKey: .signature)
        sigAlg = try container.decodeIfPresent(String.self, forKey: .sigAlg)
        downloadURL = try container.decode(String.self, forKey: .downloadURL)
        lastUpdated = try container.decodeIfPresent(String.self, forKey: .lastUpdated)
        regionCode = try container.decodeIfPresent(String.self, forKey: .regionCode)
        packVersion = try container.decodeIfPresent(String.self, forKey: .packVersion)
        tileFormat = try container.decodeIfPresent(String.self, forKey: .tileFormat)
        styleURL = try container.decodeIfPresent(String.self, forKey: .styleURL)
        languageCode = try container.decodeIfPresent(String.self, forKey: .languageCode)
        countryCode = try container.decodeIfPresent(String.self, forKey: .countryCode)
        countryName = try container.decodeIfPresent(String.self, forKey: .countryName)
        isFeatured = try container.decodeIfPresent(Bool.self, forKey: .isFeatured)
        displayOrder = try container.decodeIfPresent(Int.self, forKey: .displayOrder)
        downloadSizeBytes = try container.decodeIfPresent(Int64.self, forKey: .downloadSizeBytes)
        minAppVersion = try container.decodeIfPresent(String.self, forKey: .minAppVersion)

        if let list = try? container.decode([String].self, forKey: .dependsOn) {
            dependsOn = list
        } else if let raw = try? container.decode(String.self, forKey: .dependsOn) {
            dependsOn = raw
                .split(separator: ",")
                .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
                .filter { !$0.isEmpty }
        } else {
            dependsOn = nil
        }
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

struct HubTilePackInstallPlan {
    let dependenciesToInstall: [HubTilePack]
    let alreadySatisfiedDependencies: [String]
    let missingDependencies: [String]
    let hasCycle: Bool
}

struct HubTilePackInstallResult {
    let primary: OfflineTilePack
    let installedDependencies: [OfflineTilePack]
    let alreadySatisfiedDependencies: [String]
}

struct OfflineTilePackDeletionPlan {
    let blockingDependents: [OfflineTilePack]

    var canDelete: Bool {
        blockingDependents.isEmpty
    }
}

struct OfflineTilePackCascadeDeletionPlan {
    let target: OfflineTilePack
    let dependents: [OfflineTilePack]

    var deletionOrder: [OfflineTilePack] {
        dependents + [target]
    }

    var hasDependents: Bool {
        !dependents.isEmpty
    }
}

struct OfflineTilePackActivationStatus {
    let canActivate: Bool
    let missingDependencies: [String]

    var hasBlockingDependencies: Bool {
        !missingDependencies.isEmpty
    }
}

private struct LocalPackManifest: Codable {
    let packID: String?
    let regionCode: String?
    let packVersion: String?
    let tileFormat: String?
    let dependsOn: [String]?
    let styleURL: String?
    let languageCode: String?
    let countryCode: String?
    let countryName: String?
    let isFeatured: Bool?
    let displayOrder: Int?
    let downloadSizeBytes: Int64?
    let minAppVersion: String?
    let sha256: String?
    let signature: String?
    let sigAlg: String?
    let nodeID: String?
    let signingPubkey: String?

    enum CodingKeys: String, CodingKey {
        case packID = "pack_id"
        case regionCode = "region_code"
        case packVersion = "pack_version"
        case tileFormat = "tile_format"
        case dependsOn = "depends_on"
        case styleURL = "style_url"
        case languageCode = "lang"
        case countryCode = "country_code"
        case countryName = "country_name"
        case isFeatured = "is_featured"
        case displayOrder = "display_order"
        case downloadSizeBytes = "download_size_bytes"
        case minAppVersion = "min_app_version"
        case sha256
        case signature
        case sigAlg = "sig_alg"
        case nodeID = "node_id"
        case signingPubkey = "signing_pubkey"
    }
}

@MainActor
final class OfflineTileStore: ObservableObject {
    static let shared = OfflineTileStore()
    nonisolated static let errorDomain = "OfflineTileStore"
    nonisolated static let signedPackPolicyErrorCode = 44
    nonisolated static let minimumUsableCoverageRatio = 0.15

    @Published private(set) var availablePack: OfflineTilePack?
    @Published private(set) var activePackChain: [OfflineTilePack] = []
    @Published private(set) var packs: [OfflineTilePack] = []
    @Published private(set) var activeMapSourceBaseURL: String?
    @Published private(set) var activeNodeName: String?

    private var cachedNodeDescriptor: SemayNodeDescriptor?

    private init() {
        activeMapSourceBaseURL = configuredHubBaseURL()?.absoluteString
        refresh()
    }

    var requireSignedPacks: Bool {
        UserDefaults.standard.bool(forKey: requireSignedOfflinePacksKey)
    }

    var canInstallBundledStarterPack: Bool {
        bundledStarterPackURL() != nil
    }

    var isBundledStarterSelected: Bool {
        guard let path = availablePack?.path.lowercased() else { return false }
        return path.hasSuffix("\(bundledStarterTilesName).\(bundledStarterTilesExtension)")
    }

    func reloadSourceConfig() {
        activeMapSourceBaseURL = configuredHubBaseURL()?.absoluteString
        activeNodeName = nil
        cachedNodeDescriptor = nil
    }

    func refresh() {
        refreshSelection(
            preferredPath: UserDefaults.standard.string(forKey: selectedPackKey),
            fallbackPath: availablePack?.path
        )
    }

    private func refreshSelection(preferredPath: String?, fallbackPath: String?) {
        var discovered = discoverPacks()
        if discovered.isEmpty && !UserDefaults.standard.bool(forKey: seededStarterKey) && !requireSignedPacks {
            // Seed a tiny offline starter pack from the app bundle so first launch is still useful offline.
            if (try? installBundledStarterPackToDocuments()) != nil {
                UserDefaults.standard.set(true, forKey: seededStarterKey)
            }
            discovered = discoverPacks()
        }

        let requestedPath = preferredPath?.trimmingCharacters(in: .whitespacesAndNewlines)
        let previousPath = fallbackPath?.trimmingCharacters(in: .whitespacesAndNewlines)

        var candidatePaths: [String] = []
        if let requestedPath, !requestedPath.isEmpty {
            candidatePaths.append(requestedPath)
        }
        if let previousPath, !previousPath.isEmpty, previousPath != requestedPath {
            candidatePaths.append(previousPath)
        }

        var chosenLeaf: OfflineTilePack?
        var chosenChain: [OfflineTilePack] = []

        for path in candidatePaths {
            guard let candidate = discovered.first(where: { $0.path == path }) else { continue }
            guard let chain = OfflineTilePackSelector.resolveActivationChain(for: candidate, packs: discovered) else { continue }
            chosenLeaf = candidate
            chosenChain = chain
            break
        }

        if chosenLeaf == nil {
            if let fallback = OfflineTilePackSelector.bestGeneralCandidate(from: discovered) {
                chosenLeaf = fallback
                chosenChain = OfflineTilePackSelector.resolveActivationChain(for: fallback, packs: discovered) ?? [fallback]
            }
        }

        let activePaths = Set(chosenChain.map(\.path))
        availablePack = chosenLeaf.map { withLifecycle($0, state: .active) }
        activePackChain = chosenChain.map { withLifecycle($0, state: .active) }

        if let chosenLeaf {
            UserDefaults.standard.set(chosenLeaf.path, forKey: selectedPackKey)
        } else {
            UserDefaults.standard.removeObject(forKey: selectedPackKey)
        }

        packs = discovered.map { pack in
            if activePaths.contains(pack.path) {
                return withLifecycle(pack, state: .active)
            }
            if !activePaths.isEmpty {
                return withLifecycle(pack, state: .superseded)
            }
            return withLifecycle(pack, state: pack.lifecycleState)
        }

        for pack in packs {
            recordPackInstall(
                pack,
                state: pack.lifecycleState,
                isActive: activePaths.contains(pack.path)
            )
        }
        SemayDataStore.shared.setActiveOfflinePacks(paths: Array(activePaths))
    }

    func selectPack(_ pack: OfflineTilePack?) {
        let previousPath = availablePack?.path
        if let pack {
            refreshSelection(preferredPath: pack.path, fallbackPath: previousPath)
        } else {
            UserDefaults.standard.removeObject(forKey: selectedPackKey)
            refreshSelection(preferredPath: nil, fallbackPath: nil)
        }
    }

    @discardableResult
    func installBundledStarterPack() throws -> OfflineTilePack {
        if requireSignedPacks {
            throw NSError(
                domain: OfflineTileStore.errorDomain,
                code: OfflineTileStore.signedPackPolicyErrorCode,
                userInfo: [NSLocalizedDescriptionKey: "Signed-pack policy is enabled. Bundled starter pack is not permitted."]
            )
        }
        let targetURL = try installBundledStarterPackToDocuments()
        guard let installed = readPackMetadata(path: targetURL.path) else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 18,
                userInfo: [NSLocalizedDescriptionKey: "Bundled starter pack is not a valid MBTiles file"]
            )
        }
        UserDefaults.standard.set(true, forKey: seededStarterKey)
        recordPackInstall(
            installed,
            state: .installed,
            isActive: false
        )
        refresh()
        selectPack(installed)
        return withLifecycle(installed, state: .active)
    }

    func importPack(from url: URL) throws -> OfflineTilePack {
        let targetURL = try copyPackToLocalStorage(from: url, preferredName: url.lastPathComponent)
        do {
            let manifest = readManifest(forMBTilesPath: targetURL.path)
            let integrity = try validateImportedPackIntegrity(forMBTilesPath: targetURL.path, manifest: manifest)
            try enforceSignedPolicyForImportedPackIfNeeded(integrity: integrity)
            guard let pack = readPackMetadata(path: targetURL.path) else {
                throw NSError(
                    domain: "OfflineTileStore",
                    code: 2,
                    userInfo: [NSLocalizedDescriptionKey: "Invalid MBTiles file"]
                )
            }
            recordPackInstall(
                pack,
                state: .verified,
                isActive: false,
                sha256: integrity.sha256 ?? "",
                signature: integrity.signature ?? "",
                sigAlg: integrity.sigAlg ?? ""
            )
            refresh()
            selectPack(pack)
            return withLifecycle(pack, state: .active)
        } catch {
            cleanupLocalPackArtifacts(atMBTilesPath: targetURL.path)
            throw error
        }
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

    func featuredCountryPacks() async throws -> [HubTilePack] {
        let packs = try await fetchHubCatalog()
        let countryPacks = packs.filter { pack in
            let hasCountry = normalizedOptional(pack.countryCode) != nil || normalizedOptional(pack.countryName) != nil
            let hasRegion = normalizedOptional(pack.regionCode) != nil
            return hasCountry || hasRegion
        }
        return countryPacks.sorted(by: compareCountryCatalogPreference)
    }

    func resolveCountryPack(packID: String, catalog: [HubTilePack]) -> HubTilePack? {
        guard let target = normalizedToken(packID) else {
            return nil
        }
        let matches = catalog.filter { pack in
            normalizedToken(pack.packID) == target || normalizedToken(pack.id) == target
        }
        guard !matches.isEmpty else {
            return nil
        }
        return matches.sorted(by: compareCountryCatalogPreference).first
    }

    @discardableResult
    func installCountryPack(packID: String) async throws -> OfflineTilePack {
        let trimmedPackID = packID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !trimmedPackID.isEmpty else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 61,
                userInfo: [NSLocalizedDescriptionKey: "Pack identifier is required."]
            )
        }

        let catalog = try await fetchHubCatalog()
        guard let selected = resolveCountryPack(packID: trimmedPackID, catalog: catalog) else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 62,
                userInfo: [NSLocalizedDescriptionKey: "Offline pack '\(packID)' is not available in the current catalog."]
            )
        }

        let result = try await installHubPackWithDependencies(selected, catalog: catalog)
        return result.primary
    }

    func installPlan(for pack: HubTilePack, catalog: [HubTilePack]) -> HubTilePackInstallPlan {
        let installedPacks = packs.isEmpty ? discoverPacks() : packs
        let plan = buildDependencyPlan(
            for: pack,
            catalog: catalog,
            installedPacks: installedPacks
        )
        return HubTilePackInstallPlan(
            dependenciesToInstall: plan.dependenciesToInstall,
            alreadySatisfiedDependencies: plan.alreadySatisfiedDependencies.sorted(),
            missingDependencies: plan.missingDependencies.sorted(),
            hasCycle: plan.hasCycle
        )
    }

    @discardableResult
    func installHubPack(_ pack: HubTilePack) async throws -> OfflineTilePack {
        let result = try await installHubPackWithDependencies(pack)
        return result.primary
    }

    @discardableResult
    func installHubPackWithDependencies(
        _ pack: HubTilePack,
        catalog catalogOverride: [HubTilePack]? = nil
    ) async throws -> HubTilePackInstallResult {
        let baseURL = try await resolveHubBaseURL()
        let catalog: [HubTilePack]
        if let catalogOverride {
            catalog = catalogOverride
        } else {
            catalog = try await fetchHubCatalog()
        }
        let initiallyInstalled = discoverPacks()
        let dependencyPlan = buildDependencyPlan(
            for: pack,
            catalog: catalog,
            installedPacks: initiallyInstalled
        )

        if dependencyPlan.hasCycle {
            throw NSError(
                domain: "OfflineTileStore",
                code: 42,
                userInfo: [NSLocalizedDescriptionKey: "Dependency cycle detected for \(pack.name)."]
            )
        }
        if !dependencyPlan.missingDependencies.isEmpty {
            let missing = dependencyPlan.missingDependencies.sorted()
            throw NSError(
                domain: "OfflineTileStore",
                code: 41,
                userInfo: [
                    NSLocalizedDescriptionKey: "Missing required dependencies: \(missing.joined(separator: ", "))",
                    "missing_dependencies": missing,
                ]
            )
        }
        try enforceSignedPolicyForHubInstallIfNeeded(
            primary: pack,
            dependencyPlan: dependencyPlan,
            installedPacks: initiallyInstalled
        )

        var installedDependencyPacks: [OfflineTilePack] = []
        var installedNow = initiallyInstalled
        for dependency in dependencyPlan.dependenciesToInstall {
            if installedPack(matching: dependency, in: installedNow) != nil {
                continue
            }
            let installed = try await downloadAndInstallHubPack(dependency, baseURL: baseURL)
            installedDependencyPacks.append(installed)
            installedNow.removeAll { $0.path == installed.path }
            installedNow.append(installed)
        }

        let installedPrimary = try await downloadAndInstallHubPack(pack, baseURL: baseURL)

        refresh()
        selectPack(installedPrimary)
        let activePrimary = withLifecycle(installedPrimary, state: .active)
        return HubTilePackInstallResult(
            primary: activePrimary,
            installedDependencies: installedDependencyPacks,
            alreadySatisfiedDependencies: dependencyPlan.alreadySatisfiedDependencies.sorted()
        )
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

    func deletionPlan(for pack: OfflineTilePack) -> OfflineTilePackDeletionPlan {
        let installed = packs.isEmpty ? discoverPacks() : packs
        let blockingDependents = OfflineTilePackSelector.blockingDependents(
            for: pack,
            packs: installed
        )
        return OfflineTilePackDeletionPlan(blockingDependents: blockingDependents)
    }

    func cascadeDeletionPlan(for pack: OfflineTilePack) -> OfflineTilePackCascadeDeletionPlan {
        let installed = packs.isEmpty ? discoverPacks() : packs
        let dependents = OfflineTilePackSelector.cascadeDependents(
            for: pack,
            packs: installed
        )
        return OfflineTilePackCascadeDeletionPlan(
            target: pack,
            dependents: dependents
        )
    }

    func activationStatus(for pack: OfflineTilePack) -> OfflineTilePackActivationStatus {
        let installed = packs.isEmpty ? discoverPacks() : packs
        if OfflineTilePackSelector.resolveActivationChain(for: pack, packs: installed) != nil {
            return OfflineTilePackActivationStatus(canActivate: true, missingDependencies: [])
        }
        return OfflineTilePackActivationStatus(
            canActivate: false,
            missingDependencies: OfflineTilePackSelector.missingDependencies(
                for: pack,
                packs: installed
            )
        )
    }

    func activationChain(for pack: OfflineTilePack) -> [OfflineTilePack] {
        let installed = packs.isEmpty ? discoverPacks() : packs
        return OfflineTilePackSelector.resolveActivationChain(for: pack, packs: installed) ?? []
    }

    func deletePack(_ pack: OfflineTilePack) throws {
        let plan = deletionPlan(for: pack)
        if !plan.blockingDependents.isEmpty {
            let names = plan.blockingDependents.map(\.name).joined(separator: ", ")
            throw NSError(
                domain: "OfflineTileStore",
                code: 50,
                userInfo: [
                    NSLocalizedDescriptionKey: "Cannot delete \(pack.name). Required by: \(names).",
                    "blocking_dependents": plan.blockingDependents.map(\.packID),
                ]
            )
        }

        try? FileManager.default.removeItem(atPath: pack.path)
        try? FileManager.default.removeItem(atPath: "\(pack.path).manifest.json")
        try? FileManager.default.removeItem(atPath: "\(pack.path).sha256")
        SemayDataStore.shared.removeOfflinePackInstall(path: pack.path)
        refresh()
    }

    @discardableResult
    func deletePackCascadingDependents(_ pack: OfflineTilePack) throws -> [OfflineTilePack] {
        let plan = cascadeDeletionPlan(for: pack)
        for candidate in plan.deletionOrder {
            try? FileManager.default.removeItem(atPath: candidate.path)
            try? FileManager.default.removeItem(atPath: "\(candidate.path).manifest.json")
            try? FileManager.default.removeItem(atPath: "\(candidate.path).sha256")
            SemayDataStore.shared.removeOfflinePackInstall(path: candidate.path)
        }
        refresh()
        return plan.deletionOrder
    }

    func deleteAllPacks() {
        for pack in packs {
            try? FileManager.default.removeItem(atPath: pack.path)
            try? FileManager.default.removeItem(atPath: "\(pack.path).manifest.json")
            try? FileManager.default.removeItem(atPath: "\(pack.path).sha256")
            SemayDataStore.shared.removeOfflinePackInstall(path: pack.path)
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
        if let manifestHeader = publishManifestHeaderValue(for: pack) {
            request.setValue(manifestHeader, forHTTPHeaderField: "X-Semay-Pack-Manifest-B64")
        }

        let sourceURL = URL(fileURLWithPath: pack.path)
        let (data, response) = try await URLSession.shared.upload(for: request, fromFile: sourceURL)
        try ensureSuccessStatus(response: response, data: data)

        let decoded = try JSONDecoder().decode(HubUploadResponse.self, from: data)
        return decoded.pack
    }

    private func publishManifestHeaderValue(for pack: OfflineTilePack) -> String? {
        let payload = publishManifestPayload(for: pack)
        guard !payload.isEmpty else { return nil }
        guard JSONSerialization.isValidJSONObject(payload),
              let data = try? JSONSerialization.data(withJSONObject: payload, options: []) else {
            return nil
        }
        return data.base64EncodedString()
    }

    private func publishManifestPayload(for pack: OfflineTilePack) -> [String: Any] {
        var payload: [String: Any] = [:]
        let manifest = readManifest(forMBTilesPath: pack.path)
        let index = SemayDataStore.shared.offlinePackInstall(path: pack.path)

        if let packID = normalizedOptional(manifest?.packID)
            ?? normalizedOptional(index?.packID)
            ?? normalizedOptional(pack.packID) {
            payload["pack_id"] = packID
        }
        if let regionCode = normalizedOptional(manifest?.regionCode)
            ?? normalizedOptional(index?.regionCode)
            ?? normalizedOptional(pack.regionCode) {
            payload["region_code"] = regionCode
        }
        if let packVersion = normalizedOptional(manifest?.packVersion)
            ?? normalizedOptional(index?.packVersion)
            ?? normalizedOptional(pack.packVersion) {
            payload["pack_version"] = packVersion
        }
        if let tileFormat = normalizedOptional(manifest?.tileFormat)
            ?? normalizedOptional(index?.tileFormat)
            ?? normalizedOptional(pack.tileFormat.rawValue) {
            payload["tile_format"] = tileFormat
        }
        let dependsOn: [String]
        if let manifestDepends = manifest?.dependsOn, !manifestDepends.isEmpty {
            dependsOn = manifestDepends
        } else if let indexDepends = normalizedOptional(index?.dependsOn) {
            dependsOn = parseDependsOn(indexDepends)
        } else {
            dependsOn = pack.dependsOn
        }
        if !dependsOn.isEmpty {
            payload["depends_on"] = dependsOn
        }
        if let styleURL = normalizedOptional(manifest?.styleURL)
            ?? normalizedOptional(index?.styleURL)
            ?? normalizedOptional(pack.styleURL) {
            payload["style_url"] = styleURL
        }
        if let languageCode = normalizedOptional(manifest?.languageCode)
            ?? normalizedOptional(index?.languageCode)
            ?? normalizedOptional(pack.languageCode) {
            payload["lang"] = languageCode
        }
        if let countryCode = normalizedOptional(manifest?.countryCode) {
            payload["country_code"] = countryCode
        }
        if let countryName = normalizedOptional(manifest?.countryName) {
            payload["country_name"] = countryName
        }
        if let isFeatured = manifest?.isFeatured {
            payload["is_featured"] = isFeatured
        }
        if let displayOrder = manifest?.displayOrder {
            payload["display_order"] = displayOrder
        }
        if let downloadSize = manifest?.downloadSizeBytes ?? (pack.sizeBytes > 0 ? pack.sizeBytes : nil) {
            payload["download_size_bytes"] = Int(downloadSize)
        }
        if let minAppVersion = normalizedOptional(manifest?.minAppVersion) {
            payload["min_app_version"] = minAppVersion
        }

        let sidecarSHA = readSHA256Sidecar(forMBTilesPath: pack.path)
        if let sha256 = normalizedSHA256(manifest?.sha256)
            ?? normalizedSHA256(index?.sha256)
            ?? normalizedSHA256(sidecarSHA) {
            payload["sha256"] = sha256
        }
        if let signature = normalizedOptional(manifest?.signature)
            ?? normalizedOptional(index?.signature) {
            payload["signature"] = signature
        }
        if let sigAlg = normalizedOptional(manifest?.sigAlg)
            ?? normalizedOptional(index?.sigAlg) {
            payload["sig_alg"] = sigAlg
        }
        if let nodeID = normalizedOptional(manifest?.nodeID) {
            payload["node_id"] = nodeID
        }
        if let signingPubkey = normalizedOptional(manifest?.signingPubkey) {
            payload["signing_pubkey"] = signingPubkey
        }
        return payload
    }

    func discoverMapSourceURL() async throws -> String {
        let url = try await resolveHubBaseURL(forceDiscovery: true)
        return url.absoluteString
    }

    func discoverHubBaseURL() async throws -> String {
        try await discoverMapSourceURL()
    }

    func bestPack(forLatitude lat: Double, longitude lon: Double, preferredZoom: Int? = nil) -> OfflineTilePack? {
        OfflineTilePackSelector.bestPack(
            from: packs,
            latitude: lat,
            longitude: lon,
            preferredZoom: preferredZoom
        )
    }

    func bestPackOrNil(for region: MKCoordinateRegion) -> OfflineTilePack? {
        let preferredZoom = preferredZoomLevel(for: region)
        if let best = bestPack(
            forLatitude: region.center.latitude,
            longitude: region.center.longitude,
            preferredZoom: preferredZoom
        ) {
            return best
        }
        return packs.first(where: { $0.bounds == nil && activationStatus(for: $0).canActivate })
    }

    func hasUsablePack(for region: MKCoordinateRegion) -> Bool {
        guard let pack = bestPackOrNil(for: region) else { return false }
        return isPackUsable(pack, for: region)
    }

    func isPackUsable(
        _ pack: OfflineTilePack,
        for region: MKCoordinateRegion,
        minimumCoverage: Double = OfflineTileStore.minimumUsableCoverageRatio
    ) -> Bool {
        coverageRatio(for: pack, in: region) >= max(0.0, min(1.0, minimumCoverage))
    }

    func coverageRatio(for pack: OfflineTilePack, in region: MKCoordinateRegion) -> Double {
        guard activationStatus(for: pack).canActivate else { return 0 }
        let starterSuffix = "\(bundledStarterTilesName).\(bundledStarterTilesExtension)"
        if pack.path.lowercased().hasSuffix(starterSuffix) {
            // Starter pack is city-scale and intentionally sparse; treat wide viewports as unusable.
            let viewportSpan = max(region.span.latitudeDelta, region.span.longitudeDelta)
            if viewportSpan > 0.12 {
                return 0.0
            }
        }
        guard let bounds = pack.bounds else { return 1.0 }

        let viewport = viewportBounds(for: region)
        let viewportArea = max(
            0.000001,
            (viewport.maxLat - viewport.minLat) * (viewport.maxLon - viewport.minLon)
        )
        let overlapLat = max(0.0, min(viewport.maxLat, bounds.maxLat) - max(viewport.minLat, bounds.minLat))
        let overlapLon = max(0.0, min(viewport.maxLon, bounds.maxLon) - max(viewport.minLon, bounds.minLon))
        let overlapArea = overlapLat * overlapLon
        return max(0.0, min(1.0, overlapArea / viewportArea))
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

    private func preferredZoomLevel(for region: MKCoordinateRegion) -> Int {
        let lonDelta = max(0.0001, min(360, region.span.longitudeDelta))
        let zoom = log2(360.0 / lonDelta)
        return max(0, min(22, Int(round(zoom))))
    }

    private func viewportBounds(for region: MKCoordinateRegion) -> PackBounds {
        let latDelta = max(0.0001, min(180.0, region.span.latitudeDelta))
        let lonDelta = max(0.0001, min(360.0, region.span.longitudeDelta))
        let minLat = max(-90.0, region.center.latitude - (latDelta / 2.0))
        let maxLat = min(90.0, region.center.latitude + (latDelta / 2.0))
        let minLon = max(-180.0, region.center.longitude - (lonDelta / 2.0))
        let maxLon = min(180.0, region.center.longitude + (lonDelta / 2.0))
        return PackBounds(
            minLon: min(minLon, maxLon),
            minLat: min(minLat, maxLat),
            maxLon: max(minLon, maxLon),
            maxLat: max(minLat, maxLat)
        )
    }

    private func inferredBoundsFromTiles(db: OpaquePointer?) -> PackBounds? {
        guard let db else { return nil }

        var zoomStmt: OpaquePointer?
        defer { sqlite3_finalize(zoomStmt) }
        guard sqlite3_prepare_v2(
            db,
            "SELECT MAX(zoom_level) FROM tiles",
            -1,
            &zoomStmt,
            nil
        ) == SQLITE_OK else {
            return nil
        }
        guard sqlite3_step(zoomStmt) == SQLITE_ROW else { return nil }
        let zoom = sqlite3_column_int(zoomStmt, 0)
        guard zoom >= 0 else { return nil }

        var extentStmt: OpaquePointer?
        defer { sqlite3_finalize(extentStmt) }
        guard sqlite3_prepare_v2(
            db,
            "SELECT MIN(tile_column), MAX(tile_column), MIN(tile_row), MAX(tile_row) FROM tiles WHERE zoom_level = ?",
            -1,
            &extentStmt,
            nil
        ) == SQLITE_OK else {
            return nil
        }
        sqlite3_bind_int(extentStmt, 1, zoom)
        guard sqlite3_step(extentStmt) == SQLITE_ROW else { return nil }
        guard sqlite3_column_type(extentStmt, 0) != SQLITE_NULL,
              sqlite3_column_type(extentStmt, 1) != SQLITE_NULL,
              sqlite3_column_type(extentStmt, 2) != SQLITE_NULL,
              sqlite3_column_type(extentStmt, 3) != SQLITE_NULL else {
            return nil
        }

        let minX = Int(sqlite3_column_int(extentStmt, 0))
        let maxX = Int(sqlite3_column_int(extentStmt, 1))
        let minTmsY = Int(sqlite3_column_int(extentStmt, 2))
        let maxTmsY = Int(sqlite3_column_int(extentStmt, 3))
        guard minX <= maxX, minTmsY <= maxTmsY else { return nil }

        let zoomInt = Int(zoom)
        guard zoomInt >= 0, zoomInt <= 30 else { return nil }
        let tileCount = 1 << zoomInt
        guard tileCount > 0 else { return nil }

        let topXYZY = tileCount - 1 - maxTmsY
        let bottomXYZY = tileCount - 1 - minTmsY

        let leftLon = tileLongitude(x: minX, zoom: zoomInt)
        let rightLon = tileLongitude(x: maxX + 1, zoom: zoomInt)
        let topLat = tileLatitude(y: topXYZY, zoom: zoomInt)
        let bottomLat = tileLatitude(y: bottomXYZY + 1, zoom: zoomInt)

        return PackBounds(
            minLon: max(-180.0, min(leftLon, rightLon)),
            minLat: max(-90.0, min(bottomLat, topLat)),
            maxLon: min(180.0, max(leftLon, rightLon)),
            maxLat: min(90.0, max(bottomLat, topLat))
        )
    }

    private func tileLongitude(x: Int, zoom: Int) -> Double {
        let n = Double(1 << zoom)
        return (Double(x) / n) * 360.0 - 180.0
    }

    private func tileLatitude(y: Int, zoom: Int) -> Double {
        let n = Double(1 << zoom)
        let mercator = Double.pi * (1.0 - (2.0 * Double(y) / n))
        return atan(sinh(mercator)) * 180.0 / Double.pi
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
        guard minZoom >= 0, maxZoom >= minZoom, maxZoom <= 22 else {
            return nil
        }
        let attribution = queryMetadataValue("attribution") ?? "© OpenStreetMap contributors"
        let bounds = parseBounds(queryMetadataValue("bounds")) ?? inferredBoundsFromTiles(db: db)
        if let bounds {
            guard bounds.minLat >= -90, bounds.maxLat <= 90, bounds.minLon >= -180, bounds.maxLon <= 180 else {
                return nil
            }
            guard bounds.minLat <= bounds.maxLat, bounds.minLon <= bounds.maxLon else {
                return nil
            }
        }

        let fallbackPackID = URL(fileURLWithPath: path).deletingPathExtension().lastPathComponent.lowercased()
        let metadataPackID = queryMetadataValue("pack_id")
        let metadataRegionCode = queryMetadataValue("region_code")
        let metadataPackVersion = queryMetadataValue("pack_version")
        let metadataFormat = queryMetadataValue("tile_format") ?? queryMetadataValue("format")
        let metadataDependsOn = queryMetadataValue("depends_on")
        let metadataStyleURL = queryMetadataValue("style_url")
        let metadataLang = queryMetadataValue("lang")
        let manifest = readManifest(forMBTilesPath: path)
        let index = SemayDataStore.shared.offlinePackInstall(path: path)
        let resolvedDependsOn: [String]
        if let indexDepends = normalizedOptional(index?.dependsOn) {
            resolvedDependsOn = parseDependsOn(indexDepends)
        } else if let manifestDepends = manifest?.dependsOn {
            resolvedDependsOn = manifestDepends
        } else {
            resolvedDependsOn = parseDependsOn(metadataDependsOn)
        }

        let sizeBytes = fileSize(path: path)
        return OfflineTilePack(
            packID: (index?.packID ?? manifest?.packID ?? metadataPackID ?? fallbackPackID).lowercased(),
            regionCode: normalizedOptional(index?.regionCode ?? manifest?.regionCode ?? metadataRegionCode),
            packVersion: normalizedOptional(index?.packVersion ?? manifest?.packVersion ?? metadataPackVersion),
            tileFormat: parseTileFormat(index?.tileFormat ?? manifest?.tileFormat ?? metadataFormat),
            dependsOn: resolvedDependsOn,
            styleURL: normalizedOptional(index?.styleURL ?? manifest?.styleURL ?? metadataStyleURL),
            languageCode: normalizedOptional(index?.languageCode ?? manifest?.languageCode ?? metadataLang),
            name: name,
            path: path,
            minZoom: minZoom,
            maxZoom: maxZoom,
            attribution: attribution,
            bounds: bounds,
            sizeBytes: sizeBytes,
            lifecycleState: parseLifecycle(index?.lifecycleState) ?? .discovered
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

    private func readManifest(forMBTilesPath path: String) -> LocalPackManifest? {
        let manifestPath = "\(path).manifest.json"
        guard let data = try? Data(contentsOf: URL(fileURLWithPath: manifestPath)) else {
            return nil
        }
        return try? JSONDecoder().decode(LocalPackManifest.self, from: data)
    }

    private func writeManifest(for pack: HubTilePack, mbtilesPath: String) {
        let fallbackPackID = URL(fileURLWithPath: pack.fileName)
            .deletingPathExtension()
            .lastPathComponent
            .lowercased()
        let descriptor = cachedNodeDescriptor
        let manifest = LocalPackManifest(
            packID: normalizedOptional(pack.packID) ?? fallbackPackID,
            regionCode: pack.regionCode,
            packVersion: pack.packVersion,
            tileFormat: pack.tileFormat,
            dependsOn: pack.dependsOn,
            styleURL: pack.styleURL,
            languageCode: pack.languageCode,
            countryCode: pack.countryCode,
            countryName: pack.countryName,
            isFeatured: pack.isFeatured,
            displayOrder: pack.displayOrder,
            downloadSizeBytes: pack.downloadSizeBytes,
            minAppVersion: pack.minAppVersion,
            sha256: normalizedOptional(pack.sha256),
            signature: normalizedOptional(pack.signature),
            sigAlg: normalizedOptional(pack.sigAlg),
            nodeID: normalizedOptional(descriptor?.nodeID),
            signingPubkey: normalizedOptional(descriptor?.signingPubkey)
        )
        guard let data = try? JSONEncoder().encode(manifest) else { return }
        let manifestURL = URL(fileURLWithPath: "\(mbtilesPath).manifest.json")
        try? data.write(to: manifestURL, options: [.atomic])
    }

    private func withLifecycle(_ pack: OfflineTilePack, state: OfflineTilePack.LifecycleState) -> OfflineTilePack {
        OfflineTilePack(
            packID: pack.packID,
            regionCode: pack.regionCode,
            packVersion: pack.packVersion,
            tileFormat: pack.tileFormat,
            dependsOn: pack.dependsOn,
            styleURL: pack.styleURL,
            languageCode: pack.languageCode,
            name: pack.name,
            path: pack.path,
            minZoom: pack.minZoom,
            maxZoom: pack.maxZoom,
            attribution: pack.attribution,
            bounds: pack.bounds,
            sizeBytes: pack.sizeBytes,
            lifecycleState: state
        )
    }

    private func mergePack(
        _ installed: OfflineTilePack,
        withHubPack hubPack: HubTilePack,
        preferredState: OfflineTilePack.LifecycleState
    ) -> OfflineTilePack {
        let fallbackHubPackID = URL(fileURLWithPath: hubPack.fileName)
            .deletingPathExtension()
            .lastPathComponent
            .lowercased()
        let resolvedPackID = normalizedOptional(hubPack.packID)
            ?? (installed.packID.isEmpty ? fallbackHubPackID : installed.packID)
        return OfflineTilePack(
            packID: resolvedPackID,
            regionCode: normalizedOptional(hubPack.regionCode) ?? installed.regionCode,
            packVersion: normalizedOptional(hubPack.packVersion) ?? installed.packVersion,
            tileFormat: parseTileFormat(hubPack.tileFormat ?? installed.tileFormat.rawValue),
            dependsOn: hubPack.dependsOn ?? installed.dependsOn,
            styleURL: normalizedOptional(hubPack.styleURL) ?? installed.styleURL,
            languageCode: normalizedOptional(hubPack.languageCode) ?? installed.languageCode,
            name: installed.name,
            path: installed.path,
            minZoom: installed.minZoom,
            maxZoom: installed.maxZoom,
            attribution: installed.attribution,
            bounds: installed.bounds,
            sizeBytes: installed.sizeBytes,
            lifecycleState: preferredState
        )
    }

    private func recordPackInstall(
        _ pack: OfflineTilePack,
        state: OfflineTilePack.LifecycleState,
        isActive: Bool,
        sha256: String = "",
        signature: String = "",
        sigAlg: String = ""
    ) {
        SemayDataStore.shared.upsertOfflinePackInstall(
            .init(
                packID: pack.packID,
                regionCode: pack.regionCode ?? "",
                packVersion: pack.packVersion ?? "",
                tileFormat: pack.tileFormat.rawValue,
                installedPath: pack.path,
                sha256: sha256,
                signature: signature,
                sigAlg: sigAlg,
                minZoom: pack.minZoom,
                maxZoom: pack.maxZoom,
                bounds: formatPackBounds(pack.bounds),
                sizeBytes: pack.sizeBytes,
                lifecycleState: state.rawValue,
                isActive: isActive,
                dependsOn: pack.dependsOn.joined(separator: ","),
                styleURL: pack.styleURL ?? "",
                languageCode: pack.languageCode ?? ""
            )
        )
    }

    private func formatPackBounds(_ bounds: PackBounds?) -> String {
        guard let bounds else { return "" }
        return "\(bounds.minLon),\(bounds.minLat),\(bounds.maxLon),\(bounds.maxLat)"
    }

    private func parseTileFormat(_ raw: String?) -> OfflineTilePack.TileFormat {
        let value = raw?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        switch value {
        case "raster", "png", "jpg", "jpeg", "webp":
            return .raster
        case "vector", "mvt", "pbf":
            return .vector
        default:
            return .unknown
        }
    }

    private func parseLifecycle(_ raw: String?) -> OfflineTilePack.LifecycleState? {
        guard let normalized = normalizedOptional(raw) else { return nil }
        return OfflineTilePack.LifecycleState(rawValue: normalized.lowercased())
    }

    private func parseDependsOn(_ raw: String?) -> [String] {
        guard let raw = normalizedOptional(raw) else { return [] }
        if raw.hasPrefix("["),
           let data = raw.data(using: .utf8),
           let decoded = try? JSONDecoder().decode([String].self, from: data) {
            return decoded.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
        }
        return raw
            .split(separator: ",")
            .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }

    private struct ResolvedHubDependencyPlan {
        let dependenciesToInstall: [HubTilePack]
        let alreadySatisfiedDependencies: Set<String>
        let missingDependencies: Set<String>
        let hasCycle: Bool
    }

    private func downloadAndInstallHubPack(_ pack: HubTilePack, baseURL: URL) async throws -> OfflineTilePack {
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

        if let expectedHash = normalizedOptional(pack.sha256) {
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
        writeManifest(for: pack, mbtilesPath: targetURL.path)
        writeSHA256Sidecar(forMBTilesPath: targetURL.path, sha256: normalizedOptional(pack.sha256))
        guard let installedRaw = readPackMetadata(path: targetURL.path) else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 13,
                userInfo: [NSLocalizedDescriptionKey: "Downloaded file is not a valid MBTiles pack"]
            )
        }
        let installed = mergePack(installedRaw, withHubPack: pack, preferredState: .verified)
        recordPackInstall(
            installed,
            state: .installed,
            isActive: false,
            sha256: normalizedOptional(pack.sha256) ?? "",
            signature: normalizedOptional(pack.signature) ?? "",
            sigAlg: normalizedOptional(pack.sigAlg) ?? ""
        )
        return installed
    }

    private func buildDependencyPlan(
        for pack: HubTilePack,
        catalog: [HubTilePack],
        installedPacks: [OfflineTilePack]
    ) -> ResolvedHubDependencyPlan {
        var catalogByToken: [String: [HubTilePack]] = [:]
        for item in catalog {
            for token in hubPackIdentityTokens(item) {
                catalogByToken[token, default: []].append(item)
            }
        }
        for (token, packs) in catalogByToken {
            catalogByToken[token] = packs.sorted(by: compareHubCatalogPreference)
        }

        var availableTokens = Set(installedPacks.flatMap(offlinePackIdentityTokens(_:)))
        var visited = Set<String>()
        var visiting = Set<String>()
        var emitted = Set<String>()
        var missingDependencies = Set<String>()
        var satisfiedDependencies = Set<String>()
        var dependenciesToInstall: [HubTilePack] = []
        var hasCycle = false

        func visit(_ current: HubTilePack) -> Bool {
            let currentKey = canonicalHubPackKey(current)
            if visiting.contains(currentKey) {
                hasCycle = true
                return false
            }
            if visited.contains(currentKey) {
                return true
            }

            visiting.insert(currentKey)
            var allResolved = true

            for dependencyID in normalizedDependencyIDs(current.dependsOn) {
                if availableTokens.contains(dependencyID) {
                    satisfiedDependencies.insert(dependencyID)
                    continue
                }

                guard let dependencyPack = catalogByToken[dependencyID]?.first else {
                    missingDependencies.insert(dependencyID)
                    allResolved = false
                    continue
                }

                guard visit(dependencyPack) else {
                    allResolved = false
                    continue
                }

                let dependencyKey = canonicalHubPackKey(dependencyPack)
                if !emitted.contains(dependencyKey) {
                    dependenciesToInstall.append(dependencyPack)
                    emitted.insert(dependencyKey)
                    availableTokens.formUnion(hubPackIdentityTokens(dependencyPack))
                }
            }

            visiting.remove(currentKey)
            visited.insert(currentKey)
            return allResolved && !hasCycle
        }

        _ = visit(pack)

        return ResolvedHubDependencyPlan(
            dependenciesToInstall: dependenciesToInstall,
            alreadySatisfiedDependencies: satisfiedDependencies,
            missingDependencies: missingDependencies,
            hasCycle: hasCycle
        )
    }

    private func installedPack(matching hubPack: HubTilePack, in installedPacks: [OfflineTilePack]) -> OfflineTilePack? {
        let hubTokens = hubPackIdentityTokens(hubPack)
        guard !hubTokens.isEmpty else { return nil }
        return installedPacks.first { !offlinePackIdentityTokens($0).isDisjoint(with: hubTokens) }
    }

    private func normalizedDependencyIDs(_ values: [String]?) -> [String] {
        var seen = Set<String>()
        var result: [String] = []
        for value in values ?? [] {
            guard let token = normalizedToken(value) else { continue }
            if seen.insert(token).inserted {
                result.append(token)
            }
        }
        return result
    }

    private func hubPackIdentityTokens(_ pack: HubTilePack) -> Set<String> {
        var tokens = Set<String>()
        if let token = normalizedToken(pack.packID) {
            tokens.insert(token)
        }
        if let token = normalizedToken(pack.id) {
            tokens.insert(token)
        }
        if let token = normalizedToken(URL(fileURLWithPath: pack.fileName).deletingPathExtension().lastPathComponent) {
            tokens.insert(token)
        }
        if let token = normalizedToken(pack.name) {
            tokens.insert(token)
        }
        return tokens
    }

    private func offlinePackIdentityTokens(_ pack: OfflineTilePack) -> Set<String> {
        var tokens = Set<String>()
        if let token = normalizedToken(pack.packID) {
            tokens.insert(token)
        }
        if let token = normalizedToken(URL(fileURLWithPath: pack.path).deletingPathExtension().lastPathComponent) {
            tokens.insert(token)
        }
        if let token = normalizedToken(pack.name) {
            tokens.insert(token)
        }
        return tokens
    }

    private func canonicalHubPackKey(_ pack: HubTilePack) -> String {
        if let token = normalizedToken(pack.packID) {
            return token
        }
        if let token = normalizedToken(URL(fileURLWithPath: pack.fileName).deletingPathExtension().lastPathComponent) {
            return token
        }
        if let token = normalizedToken(pack.id) {
            return token
        }
        return normalizedToken(pack.name) ?? "unknown-pack"
    }

    private func compareHubCatalogPreference(_ lhs: HubTilePack, _ rhs: HubTilePack) -> Bool {
        let lhsVersion = parseVersion(lhs.packVersion)
        let rhsVersion = parseVersion(rhs.packVersion)
        if lhsVersion != rhsVersion {
            return compareVersions(lhsVersion, rhsVersion) == .orderedDescending
        }
        if lhs.maxZoom != rhs.maxZoom {
            return lhs.maxZoom > rhs.maxZoom
        }
        if lhs.minZoom != rhs.minZoom {
            return lhs.minZoom < rhs.minZoom
        }
        let lhsUpdated = normalizedOptional(lhs.lastUpdated) ?? ""
        let rhsUpdated = normalizedOptional(rhs.lastUpdated) ?? ""
        if lhsUpdated != rhsUpdated {
            return lhsUpdated > rhsUpdated
        }
        return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
    }

    private func compareCountryCatalogPreference(_ lhs: HubTilePack, _ rhs: HubTilePack) -> Bool {
        let lhsFeatured = lhs.isFeatured ?? false
        let rhsFeatured = rhs.isFeatured ?? false
        if lhsFeatured != rhsFeatured {
            return lhsFeatured && !rhsFeatured
        }

        let lhsOrder = lhs.displayOrder ?? Int.max
        let rhsOrder = rhs.displayOrder ?? Int.max
        if lhsOrder != rhsOrder {
            return lhsOrder < rhsOrder
        }

        let lhsCountry = normalizedOptional(lhs.countryName) ?? normalizedOptional(lhs.countryCode) ?? ""
        let rhsCountry = normalizedOptional(rhs.countryName) ?? normalizedOptional(rhs.countryCode) ?? ""
        if lhsCountry != rhsCountry {
            return lhsCountry.localizedCaseInsensitiveCompare(rhsCountry) == .orderedAscending
        }

        return compareHubCatalogPreference(lhs, rhs)
    }

    private func parseVersion(_ raw: String?) -> [Int] {
        guard let raw = normalizedOptional(raw) else { return [0, 0, 0] }
        return raw.split(separator: ".").map { Int($0) ?? 0 }
    }

    private func compareVersions(_ lhs: [Int], _ rhs: [Int]) -> ComparisonResult {
        let count = max(lhs.count, rhs.count)
        for index in 0..<count {
            let left = index < lhs.count ? lhs[index] : 0
            let right = index < rhs.count ? rhs[index] : 0
            if left != right {
                return left < right ? .orderedAscending : .orderedDescending
            }
        }
        return .orderedSame
    }

    private func normalizedToken(_ value: String?) -> String? {
        normalizedOptional(value)?.lowercased()
    }

    private func isHubPackSigned(_ pack: HubTilePack) -> Bool {
        normalizedOptional(pack.sha256) != nil && normalizedOptional(pack.signature) != nil
    }

    private func isInstalledPackSigned(_ pack: OfflineTilePack) -> Bool {
        if let install = SemayDataStore.shared.offlinePackInstall(path: pack.path) {
            let hasHash = normalizedOptional(install.sha256) != nil
            let hasSignature = normalizedOptional(install.signature) != nil
            if hasHash && hasSignature {
                return true
            }
        }
        if let manifest = readManifest(forMBTilesPath: pack.path) {
            let hasHash = normalizedOptional(manifest.sha256) != nil
            let hasSignature = normalizedOptional(manifest.signature) != nil
            if hasHash && hasSignature {
                return true
            }
        }
        return false
    }

    private func enforceSignedPolicyForImportedPackIfNeeded(
        integrity: ImportedPackIntegrityResult
    ) throws {
        guard requireSignedPacks else { return }
        let hasHash = normalizedOptional(integrity.sha256) != nil
        let hasSignature = normalizedOptional(integrity.signature) != nil
        guard hasHash && hasSignature else {
            throw NSError(
                domain: OfflineTileStore.errorDomain,
                code: OfflineTileStore.signedPackPolicyErrorCode,
                userInfo: [NSLocalizedDescriptionKey: "Signed-pack policy is enabled. Imported pack is missing hash/signature metadata."]
            )
        }
    }

    private func enforceSignedPolicyForHubInstallIfNeeded(
        primary: HubTilePack,
        dependencyPlan: ResolvedHubDependencyPlan,
        installedPacks: [OfflineTilePack]
    ) throws {
        guard requireSignedPacks else { return }

        var unsignedNames = Set<String>()
        for candidate in dependencyPlan.dependenciesToInstall + [primary] {
            if !isHubPackSigned(candidate) {
                unsignedNames.insert(candidate.name)
            }
        }

        for token in dependencyPlan.alreadySatisfiedDependencies {
            guard let matchedInstalled = installedPacks.first(where: { !offlinePackIdentityTokens($0).isDisjoint(with: [token]) }) else {
                continue
            }
            if !isInstalledPackSigned(matchedInstalled) {
                unsignedNames.insert(matchedInstalled.name)
            }
        }

        if !unsignedNames.isEmpty {
            let names = unsignedNames.sorted().joined(separator: ", ")
            throw NSError(
                domain: OfflineTileStore.errorDomain,
                code: OfflineTileStore.signedPackPolicyErrorCode,
                userInfo: [NSLocalizedDescriptionKey: "Signed-pack policy is enabled. Unsigned pack(s): \(names)."]
            )
        }
    }

    private func normalizedOptional(_ value: String?) -> String? {
        guard let value = value?.trimmingCharacters(in: .whitespacesAndNewlines),
              !value.isEmpty else {
            return nil
        }
        return value
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

    private func bundledStarterPackURL() -> URL? {
        Bundle.main.url(forResource: bundledStarterTilesName, withExtension: bundledStarterTilesExtension)
    }

    private func installBundledStarterPackToDocuments() throws -> URL {
        guard let sourceURL = bundledStarterPackURL() else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 17,
                userInfo: [NSLocalizedDescriptionKey: "Bundled starter pack is unavailable"]
            )
        }
        return try copyPackToLocalStorage(
            from: sourceURL,
            preferredName: "\(bundledStarterTilesName).\(bundledStarterTilesExtension)"
        )
    }

    private func copyPackToLocalStorage(from sourceURL: URL, preferredName: String) throws -> URL {
        let fm = FileManager.default
        let tilesDir = try documentsTileDirectory()
        let sanitizedName = sanitizePackFileName(preferredName)
        let targetURL = tilesDir.appendingPathComponent(sanitizedName)
        let targetManifestURL = URL(fileURLWithPath: targetURL.path + ".manifest.json")
        let targetSHA256URL = URL(fileURLWithPath: targetURL.path + ".sha256")

        if fm.fileExists(atPath: targetURL.path) {
            try fm.removeItem(at: targetURL)
        }
        if fm.fileExists(atPath: targetManifestURL.path) {
            try? fm.removeItem(at: targetManifestURL)
        }
        if fm.fileExists(atPath: targetSHA256URL.path) {
            try? fm.removeItem(at: targetSHA256URL)
        }

        try fm.copyItem(at: sourceURL, to: targetURL)

        let sourceManifestURL = URL(fileURLWithPath: sourceURL.path + ".manifest.json")
        if fm.fileExists(atPath: sourceManifestURL.path) {
            try? fm.copyItem(at: sourceManifestURL, to: targetManifestURL)
        }
        let sourceSHA256URL = URL(fileURLWithPath: sourceURL.path + ".sha256")
        if fm.fileExists(atPath: sourceSHA256URL.path) {
            try? fm.copyItem(at: sourceSHA256URL, to: targetSHA256URL)
        }
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

    private func verifySHA256SidecarIfPresent(forMBTilesPath path: String) throws -> String? {
        let sidecarURL = URL(fileURLWithPath: "\(path).sha256")
        guard FileManager.default.fileExists(atPath: sidecarURL.path) else {
            return nil
        }
        guard let expected = readSHA256Sidecar(forMBTilesPath: path) else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 22,
                userInfo: [NSLocalizedDescriptionKey: "Invalid .sha256 sidecar format for imported pack"]
            )
        }
        let computed = try sha256File(at: URL(fileURLWithPath: path)).lowercased()
        guard computed == expected else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 23,
                userInfo: [NSLocalizedDescriptionKey: "Imported pack SHA256 sidecar verification failed"]
            )
        }
        return expected
    }

    private struct ImportedPackIntegrityResult {
        let sha256: String?
        let signature: String?
        let sigAlg: String?
    }

    private func validateImportedPackIntegrity(
        forMBTilesPath path: String,
        manifest: LocalPackManifest?
    ) throws -> ImportedPackIntegrityResult {
        let sidecarHash = try verifySHA256SidecarIfPresent(forMBTilesPath: path)
        let manifestHash = try verifyManifestSHA256IfPresent(manifest: manifest, forMBTilesPath: path)
        if let sidecarHash, let manifestHash, sidecarHash != manifestHash {
            throw NSError(
                domain: "OfflineTileStore",
                code: 28,
                userInfo: [NSLocalizedDescriptionKey: "Imported pack hash metadata mismatch between .sha256 and manifest"]
            )
        }

        var resolvedHash = sidecarHash ?? manifestHash
        let signature = normalizedOptional(manifest?.signature)
        let sigAlg = normalizedOptional(manifest?.sigAlg)
        if signature != nil {
            if resolvedHash == nil {
                resolvedHash = try sha256File(at: URL(fileURLWithPath: path)).lowercased()
            }
            try verifyImportedManifestSignature(
                manifest: manifest,
                sha256: resolvedHash ?? ""
            )
        }

        return ImportedPackIntegrityResult(
            sha256: resolvedHash,
            signature: signature,
            sigAlg: sigAlg
        )
    }

    #if DEBUG
    func debugValidateImportedPackIntegrity(
        forMBTilesPath path: String,
        manifest: [String: Any],
        enforceSignedPolicy: Bool = true
    ) throws -> (sha256: String?, signature: String?, sigAlg: String?) {
        let data = try JSONSerialization.data(withJSONObject: manifest, options: [])
        let decoded = try JSONDecoder().decode(LocalPackManifest.self, from: data)
        let result = try validateImportedPackIntegrity(forMBTilesPath: path, manifest: decoded)
        if enforceSignedPolicy {
            try enforceSignedPolicyForImportedPackIfNeeded(integrity: result)
        }
        return (result.sha256, result.signature, result.sigAlg)
    }
    #endif

    private func verifyManifestSHA256IfPresent(
        manifest: LocalPackManifest?,
        forMBTilesPath path: String
    ) throws -> String? {
        guard let expected = normalizedSHA256(manifest?.sha256) else {
            return nil
        }
        let computed = try sha256File(at: URL(fileURLWithPath: path)).lowercased()
        guard computed == expected else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 24,
                userInfo: [NSLocalizedDescriptionKey: "Imported pack manifest SHA256 verification failed"]
            )
        }
        return expected
    }

    private func verifyImportedManifestSignature(
        manifest: LocalPackManifest?,
        sha256: String
    ) throws {
        guard let signatureHex = normalizedOptional(manifest?.signature) else {
            return
        }

        let alg = normalizedOptional(manifest?.sigAlg)?.lowercased() ?? "ed25519"
        guard alg == "ed25519" else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 25,
                userInfo: [NSLocalizedDescriptionKey: "Unsupported imported pack signature algorithm: \(alg)"]
            )
        }

        guard let nodeID = normalizedOptional(manifest?.nodeID),
              let pubkeyHex = normalizedOptional(manifest?.signingPubkey) else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 26,
                userInfo: [NSLocalizedDescriptionKey: "Pack signature present but manifest signing key metadata is unavailable"]
            )
        }

        try SemayNodeTrustStore.shared.enforceTrustedSigningPubkey(nodeID: nodeID, pubkeyHex: pubkeyHex)

        guard let pubData = Data(hexString: pubkeyHex), pubData.count == 32,
              let sigData = Data(hexString: signatureHex), sigData.count == 64 else {
            throw NSError(
                domain: "OfflineTileStore",
                code: 27,
                userInfo: [NSLocalizedDescriptionKey: "Invalid imported pack signature encoding"]
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

    private func normalizedSHA256(_ value: String?) -> String? {
        guard let normalized = normalizedOptional(value)?.lowercased(),
              normalized.count == 64,
              normalized.allSatisfy({ ("0"..."9").contains($0) || ("a"..."f").contains($0) }) else {
            return nil
        }
        return normalized
    }

    private func readSHA256Sidecar(forMBTilesPath path: String) -> String? {
        let sidecarURL = URL(fileURLWithPath: "\(path).sha256")
        guard let content = try? String(contentsOf: sidecarURL, encoding: .utf8) else {
            return nil
        }
        guard let firstToken = content.split(whereSeparator: \.isWhitespace).first else {
            return nil
        }
        return normalizedSHA256(String(firstToken))
    }

    private func writeSHA256Sidecar(forMBTilesPath path: String, sha256: String?) {
        guard let normalized = normalizedSHA256(sha256) else {
            return
        }
        let sidecarURL = URL(fileURLWithPath: "\(path).sha256")
        try? "\(normalized)\n".write(to: sidecarURL, atomically: true, encoding: .utf8)
    }

    private func cleanupLocalPackArtifacts(atMBTilesPath path: String) {
        let fm = FileManager.default
        let targets = [
            path,
            "\(path).manifest.json",
            "\(path).sha256",
        ]
        for target in targets where fm.fileExists(atPath: target) {
            try? fm.removeItem(atPath: target)
        }
    }

    private static func preferredCommunityPack(from packs: [HubTilePack]) -> HubTilePack? {
        if packs.isEmpty { return nil }

        func normalized(_ value: String?) -> String {
            value?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        }

        let sorted = packs.sorted { lhs, rhs in
            let lhsFeatured = lhs.isFeatured ?? false
            let rhsFeatured = rhs.isFeatured ?? false
            if lhsFeatured != rhsFeatured {
                return lhsFeatured && !rhsFeatured
            }
            let lhsOrder = lhs.displayOrder ?? Int.max
            let rhsOrder = rhs.displayOrder ?? Int.max
            if lhsOrder != rhsOrder {
                return lhsOrder < rhsOrder
            }
            if lhs.maxZoom != rhs.maxZoom {
                return lhs.maxZoom > rhs.maxZoom
            }
            if lhs.minZoom != rhs.minZoom {
                return lhs.minZoom < rhs.minZoom
            }
            return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
        }

        if let featuredEritrea = sorted.first(where: { pack in
            let countryCode = normalized(pack.countryCode)
            let regionCode = normalized(pack.regionCode)
            let name = pack.name.lowercased()
            return countryCode == "er" || regionCode == "er" || name.contains("eritrea")
        }) {
            return featuredEritrea
        }

        if let horn = sorted.first(where: { normalized($0.regionCode).contains("horn") || $0.name.lowercased().contains("horn") }) {
            return horn
        }
        return sorted.first
    }

    private func resolveHubBaseURL(forceDiscovery: Bool = false) async throws -> URL {
        var configuredAlreadyTested = false
        if !forceDiscovery, let configured = configuredHubBaseURL() {
            configuredAlreadyTested = true
            if await canReachHubCatalog(baseURL: configured) {
                activeMapSourceBaseURL = configured.absoluteString
                if let descriptor = try? await fetchNodeDescriptor(baseURL: configured) {
                    applyNodeDescriptor(descriptor)
                } else {
                    cachedNodeDescriptor = nil
                    activeNodeName = nil
                }
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
                if let descriptor = try? await fetchNodeDescriptor(baseURL: candidate) {
                    applyNodeDescriptor(descriptor)
                } else {
                    cachedNodeDescriptor = nil
                    activeNodeName = nil
                }
                return candidate
            }
        }

        throw NSError(
            domain: "OfflineTileStore",
            code: 30,
            userInfo: [NSLocalizedDescriptionKey: "Offline maps aren't available right now. You can keep using Semay online, or try again later."]
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

    private func applyNodeDescriptor(_ descriptor: SemayNodeDescriptor) {
        cachedNodeDescriptor = descriptor
        activeNodeName = descriptor.nodeName
        if let mapLibreEnabled = descriptor.mapLibreEnabled {
            MapEngineCoordinator.shared.setRemoteMapLibreDisabled(!mapLibreEnabled)
        }
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

struct OfflineTilePackSelector {
    static func resolveActivationChain(
        for candidate: OfflineTilePack,
        packs: [OfflineTilePack]
    ) -> [OfflineTilePack]? {
        let byPackID = Dictionary(grouping: packs, by: { normalizedPackID($0.packID) })
        var stack = Set<String>()
        var resolvedIDs = Set<String>()
        var chain: [OfflineTilePack] = []

        func bestCandidate(forPackID packID: String) -> OfflineTilePack? {
            guard let candidates = byPackID[packID], !candidates.isEmpty else {
                return nil
            }
            return candidates.sorted(by: compareCandidatePreference).first
        }

        func resolve(_ pack: OfflineTilePack) -> Bool {
            let normalizedID = normalizedPackID(pack.packID)
            if resolvedIDs.contains(normalizedID) {
                return true
            }
            if stack.contains(normalizedID) {
                return false
            }
            stack.insert(normalizedID)
            defer { stack.remove(normalizedID) }

            for dependencyID in pack.dependsOn.map(normalizedPackID).filter({ !$0.isEmpty }) {
                guard let dependency = bestCandidate(forPackID: dependencyID) else {
                    return false
                }
                guard resolve(dependency) else {
                    return false
                }
            }

            resolvedIDs.insert(normalizedID)
            chain.append(pack)
            return true
        }

        guard resolve(candidate) else {
            return nil
        }
        return chain
    }

    static func bestPack(
        from packs: [OfflineTilePack],
        latitude lat: Double,
        longitude lon: Double,
        preferredZoom: Int? = nil
    ) -> OfflineTilePack? {
        let bounded = packs.filter { $0.bounds?.contains(lat: lat, lon: lon) == true }
        let activatable = bounded.filter { resolveActivationChain(for: $0, packs: packs) != nil }
        guard !activatable.isEmpty else { return nil }

        return activatable.sorted {
            compareByRuntimeFit(lhs: $0, rhs: $1, preferredZoom: preferredZoom)
        }.first
    }

    static func bestGeneralCandidate(from packs: [OfflineTilePack]) -> OfflineTilePack? {
        let activatable = packs.filter { resolveActivationChain(for: $0, packs: packs) != nil }
        guard !activatable.isEmpty else { return nil }

        return activatable.sorted {
            compareByRuntimeFit(lhs: $0, rhs: $1, preferredZoom: nil)
        }.first
    }

    static func blockingDependents(
        for target: OfflineTilePack,
        packs: [OfflineTilePack]
    ) -> [OfflineTilePack] {
        let targetTokens = identityTokens(for: target)
        guard !targetTokens.isEmpty else { return [] }

        return packs
            .filter { $0.path != target.path }
            .filter { dependent in
                let dependencyTokens = normalizedDependencyIDs(from: dependent.dependsOn)
                return !dependencyTokens.isDisjoint(with: targetTokens)
            }
            .sorted { lhs, rhs in
                lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            }
    }

    static func missingDependencies(
        for candidate: OfflineTilePack,
        packs: [OfflineTilePack]
    ) -> [String] {
        let availableTokens = Set(packs.flatMap { identityTokens(for: $0) })
        let required = normalizedDependencyIDs(from: candidate.dependsOn)
        return required
            .filter { !availableTokens.contains($0) }
            .sorted()
    }

    static func cascadeDependents(
        for target: OfflineTilePack,
        packs: [OfflineTilePack]
    ) -> [OfflineTilePack] {
        let targetTokens = identityTokens(for: target)
        guard !targetTokens.isEmpty else { return [] }

        let installedWithoutTarget = packs.filter { $0.path != target.path }
        var dependentsByDependencyToken: [String: [OfflineTilePack]] = [:]
        for pack in installedWithoutTarget {
            for dependencyToken in normalizedDependencyIDs(from: pack.dependsOn) {
                dependentsByDependencyToken[dependencyToken, default: []].append(pack)
            }
        }

        var maxDepthByPath: [String: Int] = [:]
        var queue: [(token: String, depth: Int)] = targetTokens.map { ($0, 0) }
        var cursor = 0
        var seenTokenDepth: [String: Int] = [:]

        while cursor < queue.count {
            let current = queue[cursor]
            cursor += 1

            if let seenDepth = seenTokenDepth[current.token], seenDepth >= current.depth {
                continue
            }
            seenTokenDepth[current.token] = current.depth

            let dependents = dependentsByDependencyToken[current.token] ?? []
            for dependent in dependents {
                let nextDepth = current.depth + 1
                let existingDepth = maxDepthByPath[dependent.path] ?? -1
                if nextDepth > existingDepth {
                    maxDepthByPath[dependent.path] = nextDepth
                    for identityToken in identityTokens(for: dependent) {
                        queue.append((identityToken, nextDepth))
                    }
                }
            }
        }

        return installedWithoutTarget
            .filter { maxDepthByPath[$0.path] != nil }
            .sorted { lhs, rhs in
                let lhsDepth = maxDepthByPath[lhs.path] ?? 0
                let rhsDepth = maxDepthByPath[rhs.path] ?? 0
                if lhsDepth != rhsDepth {
                    return lhsDepth > rhsDepth
                }
                return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            }
    }

    private static func compareByRuntimeFit(
        lhs: OfflineTilePack,
        rhs: OfflineTilePack,
        preferredZoom: Int?
    ) -> Bool {
        let lhsDistance = zoomDistance(pack: lhs, preferredZoom: preferredZoom)
        let rhsDistance = zoomDistance(pack: rhs, preferredZoom: preferredZoom)
        if lhsDistance != rhsDistance {
            return lhsDistance < rhsDistance
        }
        if lhs.maxZoom != rhs.maxZoom {
            return lhs.maxZoom > rhs.maxZoom
        }
        if lhs.minZoom != rhs.minZoom {
            return lhs.minZoom < rhs.minZoom
        }

        let lhsVersion = parseVersion(lhs.packVersion)
        let rhsVersion = parseVersion(rhs.packVersion)
        if lhsVersion != rhsVersion {
            return compareVersions(lhsVersion, rhsVersion) == .orderedDescending
        }

        let lhsFormat = formatRank(lhs.tileFormat)
        let rhsFormat = formatRank(rhs.tileFormat)
        if lhsFormat != rhsFormat {
            return lhsFormat > rhsFormat
        }

        return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
    }

    private static func compareCandidatePreference(
        lhs: OfflineTilePack,
        rhs: OfflineTilePack
    ) -> Bool {
        let lhsVersion = parseVersion(lhs.packVersion)
        let rhsVersion = parseVersion(rhs.packVersion)
        if lhsVersion != rhsVersion {
            return compareVersions(lhsVersion, rhsVersion) == .orderedDescending
        }
        if lhs.maxZoom != rhs.maxZoom {
            return lhs.maxZoom > rhs.maxZoom
        }
        if lhs.minZoom != rhs.minZoom {
            return lhs.minZoom < rhs.minZoom
        }
        return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
    }

    private static func zoomDistance(pack: OfflineTilePack, preferredZoom: Int?) -> Int {
        guard let preferredZoom else { return 0 }
        if preferredZoom < pack.minZoom {
            return pack.minZoom - preferredZoom
        }
        if preferredZoom > pack.maxZoom {
            return preferredZoom - pack.maxZoom
        }
        return 0
    }

    private static func parseVersion(_ raw: String?) -> [Int] {
        guard let raw = raw?.trimmingCharacters(in: .whitespacesAndNewlines), !raw.isEmpty else {
            return [0, 0, 0]
        }
        return raw.split(separator: ".").map { Int($0) ?? 0 }
    }

    private static func compareVersions(_ lhs: [Int], _ rhs: [Int]) -> ComparisonResult {
        let count = max(lhs.count, rhs.count)
        for index in 0..<count {
            let left = index < lhs.count ? lhs[index] : 0
            let right = index < rhs.count ? rhs[index] : 0
            if left != right {
                return left < right ? .orderedAscending : .orderedDescending
            }
        }
        return .orderedSame
    }

    private static func normalizedPackID(_ raw: String) -> String {
        raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private static func normalizedDependencyIDs(from values: [String]) -> Set<String> {
        Set(values.map(normalizedPackID).filter { !$0.isEmpty })
    }

    private static func identityTokens(for pack: OfflineTilePack) -> Set<String> {
        var tokens = Set<String>()
        let packID = normalizedPackID(pack.packID)
        if !packID.isEmpty {
            tokens.insert(packID)
        }
        let stem = normalizedPackID(URL(fileURLWithPath: pack.path).deletingPathExtension().lastPathComponent)
        if !stem.isEmpty {
            tokens.insert(stem)
        }
        let name = normalizedPackID(pack.name)
        if !name.isEmpty {
            tokens.insert(name)
        }
        return tokens
    }

    private static func formatRank(_ format: OfflineTilePack.TileFormat) -> Int {
        switch format {
        case .vector:
            return 2
        case .raster:
            return 1
        case .unknown:
            return 0
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
