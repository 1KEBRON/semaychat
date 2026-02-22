import Foundation
import Network
import SQLite3

private extension Array where Element: Hashable {
    func uniqued() -> [Element] {
        var seen = Set<Element>()
        var result: [Element] = []
        for value in self where !seen.contains(value) {
            seen.insert(value)
            result.append(value)
        }
        return result
    }
}

final class MapLibreRasterBridge {
    static let shared = MapLibreRasterBridge()

    private var dbByPath: [String: OpaquePointer] = [:]
    private var activePackPaths: [String] = []
    private var activePacksByPath: [String: OfflineTilePack] = [:]
    private var styleFileURL: URL?
    private var tileServer = LocalhostTileServer()
    private var serverPort: UInt16?
    private let dbQueue = DispatchQueue(label: "semay.maplibre.raster.db", qos: .utility)
    private let dbQueueKey = DispatchSpecificKey<UInt8>()
    private let dbQueueValue: UInt8 = 1

    private init() {
        dbQueue.setSpecific(key: dbQueueKey, value: dbQueueValue)
    }

    func prepare(pack: OfflineTilePack) throws {
        try prepare(packs: [pack])
    }

    func prepare(packs: [OfflineTilePack]) throws {
        let requested = deduplicatedPacks(packs)
        guard !requested.isEmpty else {
            throw NSError(
                domain: "MapLibreRasterBridge",
                code: 21,
                userInfo: [NSLocalizedDescriptionKey: "No offline packs available for raster bridge"]
            )
        }

        if requested.contains(where: { $0.tileFormat == .vector }) {
            throw NSError(
                domain: "MapLibreRasterBridge",
                code: 20,
                userInfo: [NSLocalizedDescriptionKey: "Vector packs are not supported by the raster bridge"]
            )
        }

        let requestedPaths = requested.map(\.path)
        if requestedPaths == activePackPaths, styleFileURL != nil {
            return
        }

        closeDatabase()
        try openDatabases(paths: requestedPaths)
        let port = try ensureServerStarted()
        styleFileURL = try buildStyleFile(packs: requested, port: port)
        activePackPaths = requestedPaths
        activePacksByPath = Dictionary(uniqueKeysWithValues: requested.map { ($0.path, $0) })
    }

    func styleURL() -> URL? {
        styleFileURL
    }

    func readTile(z: Int, x: Int, y: Int) -> Data? {
        if DispatchQueue.getSpecific(key: dbQueueKey) == dbQueueValue {
            return readTileLocked(z: z, x: x, y: y)
        }
        return dbQueue.sync {
            readTileLocked(z: z, x: x, y: y)
        }
    }

    private func openDatabases(paths: [String]) throws {
        try dbQueue.sync {
            var opened: [String: OpaquePointer] = [:]
            do {
                for path in paths {
                    var conn: OpaquePointer?
                    let result = sqlite3_open_v2(path, &conn, SQLITE_OPEN_READONLY, nil)
                    guard result == SQLITE_OK, let conn else {
                        if conn != nil {
                            sqlite3_close(conn)
                        }
                        throw NSError(
                            domain: "MapLibreRasterBridge",
                            code: 2,
                            userInfo: [NSLocalizedDescriptionKey: "Could not open MBTiles at \(path)"]
                        )
                    }
                    opened[path] = conn
                }
                dbByPath = opened
            } catch {
                for (_, conn) in opened {
                    sqlite3_close(conn)
                }
                dbByPath = [:]
                throw error
            }
        }
    }

    private func closeDatabase() {
        dbQueue.sync {
            for (_, conn) in dbByPath {
                sqlite3_close(conn)
            }
            dbByPath = [:]
        }
        activePackPaths = []
        activePacksByPath = [:]
    }

    private func readTileLocked(z: Int, x: Int, y: Int) -> Data? {
        guard z >= 0, x >= 0, y >= 0 else { return nil }

        for path in activePackPaths.reversed() {
            guard let db = dbByPath[path] else { continue }
            if let tile = readTile(from: db, z: z, x: x, y: y) {
                return tile
            }
        }
        return nil
    }

    private func readTile(from db: OpaquePointer, z: Int, x: Int, y: Int) -> Data? {
        let tmsY = (1 << z) - 1 - y
        let sql = "SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ? LIMIT 1"
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            sqlite3_finalize(stmt)
            return nil
        }
        defer { sqlite3_finalize(stmt) }

        sqlite3_bind_int(stmt, 1, Int32(z))
        sqlite3_bind_int(stmt, 2, Int32(x))
        sqlite3_bind_int(stmt, 3, Int32(tmsY))

        guard sqlite3_step(stmt) == SQLITE_ROW,
              let blob = sqlite3_column_blob(stmt, 0) else {
            return nil
        }
        let size = sqlite3_column_bytes(stmt, 0)
        return Data(bytes: blob, count: Int(size))
    }

    private func ensureServerStarted() throws -> UInt16 {
        if let serverPort {
            return serverPort
        }
        let port = try tileServer.start { [weak self] path in
            guard let self else {
                return .notFound
            }
            return self.response(for: path)
        }
        serverPort = port
        return port
    }

    private func buildStyleFile(packs: [OfflineTilePack], port: UInt16) throws -> URL {
        let minZoom = packs.map(\.minZoom).min() ?? 0
        let maxZoom = packs.map(\.maxZoom).max() ?? 16
        let attribution = packs
            .map(\.attribution)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .uniqued()
            .joined(separator: " • ")

        let seed = packs.map(\.path).joined(separator: "|")
        let revision = seed.hashValue.magnitude
        let tileTemplate = "http://127.0.0.1:\(port)/tiles/{z}/{x}/{y}.png?rev=\(revision)"
        let style: [String: Any] = [
            "version": 8,
            "name": "Semay Offline Raster",
            "sources": [
                "semay-offline-raster": [
                    "type": "raster",
                    "tiles": [tileTemplate],
                    "tileSize": 256,
                    "minzoom": minZoom,
                    "maxzoom": maxZoom,
                    "attribution": attribution
                ]
            ],
            "layers": [
                [
                    "id": "semay-offline-raster",
                    "type": "raster",
                    "source": "semay-offline-raster",
                    "minzoom": minZoom,
                    "maxzoom": maxZoom
                ]
            ]
        ]

        let data = try JSONSerialization.data(withJSONObject: style, options: [.prettyPrinted, .sortedKeys])
        let cacheDirectory = try FileManager.default.url(
            for: .cachesDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let fileName = "semay-maplibre-style-\(revision).json"
        let fileURL = cacheDirectory.appendingPathComponent(fileName)
        try data.write(to: fileURL, options: [.atomic])
        return fileURL
    }

    private func deduplicatedPacks(_ packs: [OfflineTilePack]) -> [OfflineTilePack] {
        var seen = Set<String>()
        var result: [OfflineTilePack] = []
        for pack in packs {
            let normalized = pack.path.trimmingCharacters(in: .whitespacesAndNewlines)
            if normalized.isEmpty || seen.contains(normalized) {
                continue
            }
            seen.insert(normalized)
            result.append(pack)
        }
        return result
    }

    private func response(for requestPath: String) -> LocalhostTileServer.Response {
        if requestPath == "/health" {
            return .ok(body: Data("ok".utf8), contentType: "text/plain; charset=utf-8")
        }

        guard let (z, x, y) = parseTilePath(requestPath),
              let tile = readTile(z: z, x: x, y: y) else {
            return .notFound
        }

        return .ok(
            body: tile,
            contentType: contentType(for: tile)
        )
    }

    private func parseTilePath(_ requestPath: String) -> (z: Int, x: Int, y: Int)? {
        let path = requestPath.split(separator: "?", maxSplits: 1).first.map(String.init) ?? requestPath
        let components = path.split(separator: "/", omittingEmptySubsequences: true)
        guard components.count >= 4, components[0] == "tiles" else { return nil }
        guard let z = Int(components[1]), let x = Int(components[2]) else { return nil }

        var yRaw = String(components[3])
        if let atIndex = yRaw.firstIndex(of: "@") {
            yRaw = String(yRaw[..<atIndex])
        }
        if let dotIndex = yRaw.firstIndex(of: ".") {
            yRaw = String(yRaw[..<dotIndex])
        }
        guard let y = Int(yRaw) else { return nil }
        return (z, x, y)
    }

    private func contentType(for data: Data) -> String {
        if data.count >= 8, data.starts(with: [0x89, 0x50, 0x4E, 0x47]) {
            return "image/png"
        }
        if data.count >= 3, data.starts(with: [0xFF, 0xD8, 0xFF]) {
            return "image/jpeg"
        }
        if data.count >= 12,
           data.starts(with: [0x52, 0x49, 0x46, 0x46]),
           data[8...11].elementsEqual([0x57, 0x45, 0x42, 0x50]) {
            return "image/webp"
        }
        return "application/octet-stream"
    }
}

private final class LocalhostTileServer {
    struct Response {
        let statusCode: Int
        let body: Data
        let contentType: String
        let cacheControl: String

        static let notFound = Response(
            statusCode: 404,
            body: Data("not found".utf8),
            contentType: "text/plain; charset=utf-8",
            cacheControl: "no-store"
        )

        static func ok(body: Data, contentType: String) -> Response {
            Response(
                statusCode: 200,
                body: body,
                contentType: contentType,
                cacheControl: "public, max-age=86400"
            )
        }
    }

    private let queue = DispatchQueue(label: "semay.maplibre.raster.http", qos: .utility)
    private var listener: NWListener?
    private var requestHandler: ((String) -> Response)?

    func start(handler: @escaping (String) -> Response) throws -> UInt16 {
        if let listener, let port = listener.port?.rawValue {
            requestHandler = handler
            return port
        }

        requestHandler = handler
        let listener = try NWListener(using: .tcp, on: .any)
        let readySemaphore = DispatchSemaphore(value: 0)
        var startupError: Error?
        var resolvedPort: UInt16?

        listener.stateUpdateHandler = { state in
            switch state {
            case .ready:
                resolvedPort = listener.port?.rawValue
                readySemaphore.signal()
            case .failed(let error):
                startupError = error
                readySemaphore.signal()
            default:
                break
            }
        }

        listener.newConnectionHandler = { [weak self] connection in
            self?.handle(connection)
        }

        listener.start(queue: queue)
        if readySemaphore.wait(timeout: .now() + 2) == .timedOut {
            listener.cancel()
            throw NSError(
                domain: "LocalhostTileServer",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Timed out starting local tile server"]
            )
        }

        if let startupError {
            listener.cancel()
            throw startupError
        }

        guard let resolvedPort else {
            listener.cancel()
            throw NSError(
                domain: "LocalhostTileServer",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "Local tile server missing bound port"]
            )
        }

        self.listener = listener
        return resolvedPort
    }

    private func handle(_ connection: NWConnection) {
        connection.stateUpdateHandler = { [weak self] state in
            switch state {
            case .ready:
                self?.receiveRequest(on: connection)
            case .failed, .cancelled:
                connection.cancel()
            default:
                break
            }
        }
        connection.start(queue: queue)
    }

    private func receiveRequest(on connection: NWConnection, buffered: Data = Data()) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 4096) { [weak self] data, _, isComplete, error in
            guard let self else {
                connection.cancel()
                return
            }
            if error != nil {
                connection.cancel()
                return
            }

            var combined = buffered
            if let data, !data.isEmpty {
                combined.append(data)
            }

            let headerTerminator = Data("\r\n\r\n".utf8)
            let headerComplete = combined.range(of: headerTerminator) != nil
            if !headerComplete, !isComplete, combined.count < 16_384 {
                self.receiveRequest(on: connection, buffered: combined)
                return
            }

            let path = Self.parsePath(from: combined)
            let response = path.flatMap { self.requestHandler?($0) } ?? .notFound
            self.send(response: response, on: connection)
        }
    }

    private func send(response: Response, on connection: NWConnection) {
        var header = "HTTP/1.1 \(response.statusCode) \(Self.reasonPhrase(response.statusCode))\r\n"
        header += "Content-Type: \(response.contentType)\r\n"
        header += "Content-Length: \(response.body.count)\r\n"
        header += "Cache-Control: \(response.cacheControl)\r\n"
        header += "Connection: close\r\n\r\n"

        var payload = Data(header.utf8)
        payload.append(response.body)

        connection.send(content: payload, completion: .contentProcessed { _ in
            connection.cancel()
        })
    }

    private static func parsePath(from data: Data?) -> String? {
        guard let data, let request = String(data: data, encoding: .utf8) else {
            return nil
        }
        guard let requestLine = request.components(separatedBy: "\r\n").first else {
            return nil
        }
        let parts = requestLine.split(separator: " ")
        guard parts.count >= 2, parts[0] == "GET" else { return nil }
        return String(parts[1])
    }

    private static func reasonPhrase(_ code: Int) -> String {
        switch code {
        case 200:
            return "OK"
        case 404:
            return "Not Found"
        default:
            return "Error"
        }
    }
}
