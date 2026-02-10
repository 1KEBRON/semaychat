import BitLogger
import Foundation
import SQLite3

@MainActor
final class SemayDataStore: ObservableObject {
    static let shared = SemayDataStore()

    struct HubIngestMetrics: Decodable {
        struct IngestCategoryCounts: Decodable {
            let protocolInvalid: Int
            let policyRejected: Int

            enum CodingKeys: String, CodingKey {
                case protocolInvalid = "protocol_invalid"
                case policyRejected = "policy_rejected"
            }
        }

        struct IngestReasonCount: Decodable {
            let errorCode: String
            let count: Int

            enum CodingKeys: String, CodingKey {
                case errorCode = "error_code"
                case count
            }
        }

        struct Ingest: Decodable {
            let rejectedTotal: Int
            let rejectedByCategory: IngestCategoryCounts
            let rejectedByReason: [IngestReasonCount]

            enum CodingKeys: String, CodingKey {
                case rejectedTotal = "rejected_total"
                case rejectedByCategory = "rejected_by_category"
                case rejectedByReason = "rejected_by_reason"
            }
        }

        struct Spool: Decodable {
            let pendingTotal: Int
            let retryTotal: Int
            let failedTotal: Int
            let deliveredTotal: Int
            let deliveryLatencyMs: DeliveryLatency

            enum CodingKeys: String, CodingKey {
                case pendingTotal = "pending_total"
                case retryTotal = "retry_total"
                case failedTotal = "failed_total"
                case deliveredTotal = "delivered_total"
                case deliveryLatencyMs = "delivery_latency_ms"
            }
        }

        struct DeliveryLatency: Decodable {
            let avg: Int
            let min: Int
            let max: Int
        }

        let generatedAt: Int
        let windowSeconds: Int
        let ingest: Ingest
        let spool: Spool

        enum CodingKeys: String, CodingKey {
            case generatedAt = "generated_at"
            case windowSeconds = "window_seconds"
            case ingest
            case spool
        }
    }

    struct OutboxSyncReport {
        var attempted: Int = 0
        var delivered: Int = 0
        var retried: Int = 0
        var failed: Int = 0
        var skipped: Int = 0
        var errorMessage: String?

        var summary: String {
            "attempted \(attempted), delivered \(delivered), retried \(retried), failed \(failed), skipped \(skipped)"
        }
    }

    @Published private(set) var pins: [SemayMapPin] = []
    @Published private(set) var businesses: [BusinessProfile] = []
    @Published private(set) var promises: [PromiseNote] = []

    private let queue = DispatchQueue(label: "semay.data.store")
    private let idBridge = NostrIdentityBridge()
    private let lamportKey = "semay.lamport.clock"
    private let hubBaseURLKey = "semay.hub.base_url"
    private let hubIngestTokenKey = "semay.hub.ingest_token"
    private let maxOutboxAttempts = 10

    private var db: OpaquePointer?

    private init() {
        openDatabase()
        migrate()
        refreshAll()
    }

    deinit {
        if let db {
            sqlite3_close(db)
        }
    }

    // MARK: - Map Pins

    func visiblePins() -> [SemayMapPin] {
        pins.filter { $0.isVisible }
    }

    @discardableResult
    func addPin(
        name: String,
        type: String,
        details: String,
        latitude: Double,
        longitude: Double,
        phone: String = ""
    ) -> SemayMapPin {
        let now = Int(Date().timeIntervalSince1970)
        let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
        let plusCode = address.plusCode
        let eAddress = address.eAddress
        let pinID = UUID().uuidString.lowercased()
        let author = currentAuthorPubkey()

        let insertSQL = """
        INSERT INTO pins (
            pin_id, name, type, details, latitude, longitude, plus_code, e_address, phone,
            author_pubkey, approval_count, is_visible, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
        """

        _ = execute(
            insertSQL,
            binds: [
                .text(pinID), .text(name), .text(type), .text(details),
                .double(latitude), .double(longitude), .text(plusCode), .text(eAddress), .text(phone),
                .text(author), .int(now), .int(now)
            ]
        )

        let payload = [
            "pin_id": pinID,
            "name": name,
            "type": type,
            "latitude": String(latitude),
            "longitude": String(longitude),
            "plus_code": plusCode,
            "e_address": eAddress,
            "phone": phone
        ]
        enqueueEvent(.pinCreate, entityID: "pin:\(pinID)", payload: payload)

        refreshPins()

        return SemayMapPin(
            pinID: pinID,
            name: name,
            type: type,
            details: details,
            latitude: latitude,
            longitude: longitude,
            plusCode: plusCode,
            eAddress: eAddress,
            phone: phone,
            authorPubkey: author,
            approvalCount: 0,
            isVisible: false,
            createdAt: now,
            updatedAt: now
        )
    }

    @discardableResult
    func approvePin(pinID: String, approverPubkey: String? = nil, distanceMeters: Double = 250.0) -> Int {
        let now = Int(Date().timeIntervalSince1970)
        let approver = approverPubkey ?? currentAuthorPubkey()

        let insert = """
        INSERT OR IGNORE INTO pin_approvals (pin_id, approver_pubkey, distance_meters, created_at)
        VALUES (?, ?, ?, ?)
        """
        _ = execute(insert, binds: [.text(pinID), .text(approver), .double(distanceMeters), .int(now)])

        let count = queryInt(
            "SELECT COUNT(1) FROM pin_approvals WHERE pin_id = ?",
            binds: [.text(pinID)]
        )

        let visible = count >= 2 ? 1 : 0
        let update = """
        UPDATE pins
        SET approval_count = ?, is_visible = ?, updated_at = ?
        WHERE pin_id = ?
        """
        _ = execute(update, binds: [.int(count), .int(visible), .int(now), .text(pinID)])

        enqueueEvent(
            .pinApproval,
            entityID: "pin:\(pinID)",
            payload: [
                "pin_id": pinID,
                "approval_count": String(count),
                "approver_pubkey": approver
            ]
        )

        refreshPins()
        return count
    }

    // MARK: - Business

    @discardableResult
    func registerBusiness(
        name: String,
        category: String,
        details: String,
        latitude: Double,
        longitude: Double,
        phone: String = ""
    ) -> BusinessProfile {
        let businessID = UUID().uuidString.lowercased()
        let now = Int(Date().timeIntervalSince1970)
        let owner = currentAuthorPubkey()

        let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
        let plusCode = address.plusCode
        let eAddress = address.eAddress
        let qrPayload = "semay://business/\(businessID)"

        let sql = """
        INSERT INTO business_profiles (
            business_id, name, category, details, latitude, longitude, plus_code, e_address, phone,
            owner_pubkey, qr_payload, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        _ = execute(
            sql,
            binds: [
                .text(businessID), .text(name), .text(category), .text(details),
                .double(latitude), .double(longitude), .text(plusCode), .text(eAddress), .text(phone),
                .text(owner), .text(qrPayload), .int(now), .int(now)
            ]
        )

        enqueueEvent(
            .businessRegister,
            entityID: "business:\(businessID)",
            payload: [
                "business_id": businessID,
                "name": name,
                "latitude": String(latitude),
                "longitude": String(longitude),
                "plus_code": plusCode,
                "e_address": eAddress,
                "phone": phone
            ]
        )

        refreshBusinesses()

        return BusinessProfile(
            businessID: businessID,
            name: name,
            category: category,
            details: details,
            latitude: latitude,
            longitude: longitude,
            plusCode: plusCode,
            eAddress: eAddress,
            phone: phone,
            ownerPubkey: owner,
            qrPayload: qrPayload,
            createdAt: now,
            updatedAt: now
        )
    }

    // MARK: - Promise Ledger

    @discardableResult
    func createPromise(merchantID: String, amountMsat: UInt64, fiatQuote: String? = nil) -> PromiseNote {
        let now = Int(Date().timeIntervalSince1970)
        let expiry = PromiseNote.defaultExpiry()
        let payer = currentAuthorPubkey()

        let nonce = Data((0..<32).map { _ in UInt8.random(in: 0...255) }).hexEncodedString()
        let pseudoSig = SemayEventEnvelope.pseudoSign(payloadHash: nonce, authorPubkey: payer)

        let note = PromiseNote(
            merchantID: merchantID,
            payerPubkey: payer,
            amountMsat: amountMsat,
            fiatQuote: fiatQuote,
            expiresAt: expiry,
            nonce: nonce,
            payerSignature: pseudoSig,
            status: .pending,
            createdAt: now,
            updatedAt: now
        )

        upsertPromise(note)
        enqueueEvent(
            .promiseCreate,
            entityID: "promise:\(note.promiseID)",
            payload: [
                "promise_id": note.promiseID,
                "merchant_id": note.merchantID,
                "amount_msat": String(note.amountMsat),
                "status": note.status.rawValue
            ],
            expiresAt: note.expiresAt
        )

        refreshPromises()
        return note
    }

    @discardableResult
    func updatePromiseStatus(_ promiseID: String, status: PromiseStatus) -> PromiseNote? {
        var promise = promises.first(where: { $0.promiseID == promiseID })
        if promise == nil {
            refreshPromises()
            promise = promises.first(where: { $0.promiseID == promiseID })
        }
        guard let resolvedPromise = promise else { return nil }

        var updated = resolvedPromise
        updated.status = status
        updated.updatedAt = Int(Date().timeIntervalSince1970)
        upsertPromise(updated)

        let eventType: SemayEventEnvelope.EventType
        var shouldEnqueue = true
        switch status {
        case .accepted: eventType = .promiseAccept
        case .rejected: eventType = .promiseReject
        case .settled: eventType = .promiseSettle
        case .pending, .expired:
            eventType = .promiseSettle
            shouldEnqueue = false
        }

        if shouldEnqueue {
            enqueueEvent(
                eventType,
                entityID: "promise:\(updated.promiseID)",
                payload: [
                    "promise_id": updated.promiseID,
                    "status": updated.status.rawValue
                ]
            )
        }

        refreshPromises()
        return updated
    }

    @discardableResult
    func submitSettlement(
        promiseID: String,
        proofType: SettlementProofType,
        proofValue: String,
        submittedBy: SettlementSubmitter
    ) -> SettlementReceipt? {
        var promise = promises.first(where: { $0.promiseID == promiseID })
        if promise == nil {
            refreshPromises()
            promise = promises.first(where: { $0.promiseID == promiseID })
        }
        guard let resolvedPromise = promise else { return nil }

        let author = currentAuthorPubkey()
        let proofHash = Data(proofValue.utf8).sha256Hash().hexEncodedString()
        let sig = SemayEventEnvelope.pseudoSign(payloadHash: proofHash, authorPubkey: author)

        let receipt = SettlementReceipt(
            promiseID: promiseID,
            proofType: proofType,
            proofValue: proofValue,
            submittedBy: submittedBy,
            submitterSignature: sig
        )

        let now = Int(Date().timeIntervalSince1970)
        let receiptData = (try? JSONEncoder().encode(receipt)) ?? Data()
        let receiptJSON = String(data: receiptData, encoding: .utf8) ?? "{}"

        _ = execute(
            "INSERT OR REPLACE INTO settlement_receipts (receipt_id, promise_id, receipt_json, submitted_at) VALUES (?, ?, ?, ?)",
            binds: [.text(receipt.receiptID), .text(receipt.promiseID), .text(receiptJSON), .int(now)]
        )

        var settled = resolvedPromise
        settled.status = .settled
        settled.updatedAt = now
        upsertPromise(settled)

        enqueueEvent(
            .promiseSettle,
            entityID: "promise:\(promiseID)",
            payload: [
                "promise_id": promiseID,
                "proof_type": proofType.rawValue,
                "submitted_by": submittedBy.rawValue
            ]
        )

        refreshPromises()
        return receipt
    }

    func expireDuePromises() {
        let now = Int(Date().timeIntervalSince1970)
        _ = execute(
            "UPDATE promise_notes SET status = 'expired', updated_at = ? WHERE status IN ('pending','accepted') AND expires_at < ?",
            binds: [.int(now), .int(now)]
        )
        refreshPromises()
    }

    // MARK: - Sync/Outbox

    /// Clears Semay local state (pins/businesses/promises/outbox) on this device.
    /// Offline packs are stored separately and are not removed.
    func wipeLocalDatabaseForRestore() {
        // Close DB on the serialized queue to avoid races with in-flight statements.
        queue.sync {
            if let db {
                sqlite3_close(db)
                self.db = nil
            }
        }

        let path = Self.databasePath()
        try? FileManager.default.removeItem(atPath: path)

        openDatabase()
        migrate()
        refreshAll()
    }

    func pendingOutboxCount() -> Int {
        queryInt("SELECT COUNT(1) FROM event_outbox WHERE status IN ('pending','retry')")
    }

    func failedOutboxCount() -> Int {
        queryInt("SELECT COUNT(1) FROM event_outbox WHERE status = 'failed'")
    }

    func hubBaseURLString() -> String {
        UserDefaults.standard.string(forKey: hubBaseURLKey)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    }

    func hubIngestToken() -> String {
        UserDefaults.standard.string(forKey: hubIngestTokenKey) ?? ""
    }

    func saveHubConfig(baseURL: String, token: String) {
        let trimmedURL = baseURL.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedToken = token.trimmingCharacters(in: .whitespacesAndNewlines)

        UserDefaults.standard.set(trimmedURL, forKey: hubBaseURLKey)
        UserDefaults.standard.set(trimmedToken, forKey: hubIngestTokenKey)
    }

    func syncOutboxToHub(limit: Int = 50) async -> OutboxSyncReport {
        var report = OutboxSyncReport()

        var endpointURL = makeHubBatchEndpointURL()
        if endpointURL == nil {
            // Auto-discover a reachable node when the user creates events but never touched Advanced settings.
            do {
                _ = try await OfflineTileStore.shared.discoverMapSourceURL()
            } catch {
                report.errorMessage = error.localizedDescription
                return report
            }
            endpointURL = makeHubBatchEndpointURL()
        }
        guard let endpointURL else {
            report.errorMessage = "node-url-missing-or-invalid"
            return report
        }

        var rows = loadDueOutboxRows(limit: limit)
        guard !rows.isEmpty else { return report }

        // Upgrade legacy pseudo-signed outbox rows to real Schnorr signatures before sending.
        if let identity = try? idBridge.getCurrentNostrIdentity() {
            var upgraded: [OutboxEnvelopeRow] = []
            upgraded.reserveCapacity(rows.count)
            for row in rows {
                if row.envelope.signature.count == 64 {
                    if let upgradedEnvelope = try? SemayEventEnvelope.signed(
                        eventType: row.envelope.eventType,
                        entityID: row.envelope.entityID,
                        identity: identity,
                        lamportClock: row.envelope.lamportClock,
                        expiresAt: row.envelope.expiresAt,
                        payload: row.envelope.payload,
                        eventID: row.envelope.eventID,
                        createdAt: row.envelope.createdAt
                    ) {
                        updateOutboxEnvelopeJSON(eventID: row.eventID, envelope: upgradedEnvelope)
                        upgraded.append(
                            OutboxEnvelopeRow(
                                eventID: row.eventID,
                                envelope: upgradedEnvelope,
                                attempts: row.attempts
                            )
                        )
                        continue
                    }
                }
                upgraded.append(row)
            }
            rows = upgraded
        }

        var validRows: [OutboxEnvelopeRow] = []
        for row in rows {
            if let failure = row.envelope.validate() {
                markOutboxFailed(
                    eventID: row.eventID,
                    attempts: row.attempts + 1,
                    error: "local-validation-\(failure.category.rawValue):\(failure.reason)"
                )
                report.failed += 1
            } else {
                validRows.append(row)
            }
        }

        rows = validRows
        guard !rows.isEmpty else { return report }
        report.attempted = rows.count

        do {
            let response = try await postEnvelopeBatch(rows.map(\.envelope), to: endpointURL)

            let acceptedIDs = Set(response.accepted.map(\.eventID))
            let rejectedByID = Dictionary(uniqueKeysWithValues: response.rejected.map { ($0.eventID, $0.error) })

            for row in rows {
                if acceptedIDs.contains(row.eventID) {
                    markOutboxDelivered(eventID: row.eventID)
                    report.delivered += 1
                    continue
                }

                if let rejectError = rejectedByID[row.eventID] {
                    markOutboxFailed(
                        eventID: row.eventID,
                        attempts: row.attempts + 1,
                        error: "hub-rejected-\(rejectError)"
                    )
                    report.failed += 1
                    continue
                }

                markOutboxRetry(
                    eventID: row.eventID,
                    attempts: row.attempts + 1,
                    error: "hub-no-ack"
                )
                report.retried += 1
            }
        } catch {
            let errorDescription = String(describing: error)
            report.errorMessage = errorDescription
            for row in rows {
                markOutboxRetry(
                    eventID: row.eventID,
                    attempts: row.attempts + 1,
                    error: "hub-network-\(errorDescription)"
                )
                report.retried += 1
            }
        }

        refreshAll()
        return report
    }

    func fetchHubMetrics(windowSeconds: Int = 24 * 60 * 60) async throws -> HubIngestMetrics {
        var endpointURL = makeHubMetricsEndpointURL(windowSeconds: windowSeconds)
        if endpointURL == nil {
            // Best-effort discovery for operators who hit "Load Node Metrics" first.
            _ = try? await OfflineTileStore.shared.discoverMapSourceURL()
            endpointURL = makeHubMetricsEndpointURL(windowSeconds: windowSeconds)
        }
        guard let endpointURL else {
            throw NSError(
                domain: "semay.node",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "node-url-missing-or-invalid"]
            )
        }

        var request = URLRequest(url: endpointURL)
        request.httpMethod = "GET"
        request.timeoutInterval = 20

        let token = hubIngestToken()
        if !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }
        guard (200...299).contains(httpResponse.statusCode) else {
            let bodyString = String(data: data, encoding: .utf8) ?? ""
            throw NSError(
                domain: "semay.hub",
                code: httpResponse.statusCode,
                userInfo: [NSLocalizedDescriptionKey: "hub-http-\(httpResponse.statusCode):\(bodyString)"]
            )
        }

        return try JSONDecoder().decode(HubIngestMetrics.self, from: data)
    }

    // MARK: - Refresh

    func refreshAll() {
        refreshPins()
        refreshBusinesses()
        refreshPromises()
        expireDuePromises()
    }

    private func refreshPins() {
        let rows = query(
            """
            SELECT pin_id, name, type, details, latitude, longitude, plus_code, e_address, phone,
                   author_pubkey, approval_count, is_visible, created_at, updated_at
            FROM pins
            ORDER BY updated_at DESC
            """
        )

        let mapped = rows.compactMap { row -> SemayMapPin? in
            guard let pinID = row["pin_id"] as? String,
                  let name = row["name"] as? String,
                  let type = row["type"] as? String,
                  let details = row["details"] as? String,
                  let latitude = row["latitude"] as? Double,
                  let longitude = row["longitude"] as? Double,
                  let plusCode = row["plus_code"] as? String,
                  let eAddress = row["e_address"] as? String,
                  let phone = row["phone"] as? String,
                  let authorPubkey = row["author_pubkey"] as? String,
                  let approvalCount = row["approval_count"] as? Int,
                  let isVisible = row["is_visible"] as? Int,
                  let createdAt = row["created_at"] as? Int,
                  let updatedAt = row["updated_at"] as? Int else {
                return nil
            }

            return SemayMapPin(
                pinID: pinID,
                name: name,
                type: type,
                details: details,
                latitude: latitude,
                longitude: longitude,
                plusCode: plusCode,
                eAddress: eAddress,
                phone: phone,
                authorPubkey: authorPubkey,
                approvalCount: approvalCount,
                isVisible: isVisible == 1,
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
        pins = mapped
    }

    private func refreshBusinesses() {
        let rows = query(
            """
            SELECT business_id, name, category, details, latitude, longitude, plus_code, e_address, phone,
                   owner_pubkey, qr_payload, created_at, updated_at
            FROM business_profiles
            ORDER BY updated_at DESC
            """
        )

        let mapped = rows.compactMap { row -> BusinessProfile? in
            guard let businessID = row["business_id"] as? String,
                  let name = row["name"] as? String,
                  let category = row["category"] as? String,
                  let details = row["details"] as? String,
                  let plusCode = row["plus_code"] as? String,
                  let eAddress = row["e_address"] as? String,
                  let phone = row["phone"] as? String,
                  let ownerPubkey = row["owner_pubkey"] as? String,
                  let qrPayload = row["qr_payload"] as? String,
                  let createdAt = row["created_at"] as? Int,
                  let updatedAt = row["updated_at"] as? Int else {
                return nil
            }

            let latitude = (row["latitude"] as? Double) ?? Double(row["latitude"] as? Int ?? 0)
            let longitude = (row["longitude"] as? Double) ?? Double(row["longitude"] as? Int ?? 0)

            return BusinessProfile(
                businessID: businessID,
                name: name,
                category: category,
                details: details,
                latitude: latitude,
                longitude: longitude,
                plusCode: plusCode,
                eAddress: eAddress,
                phone: phone,
                ownerPubkey: ownerPubkey,
                qrPayload: qrPayload,
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
        businesses = mapped
    }

    private func refreshPromises() {
        let rows = query(
            """
            SELECT promise_id, note_json
            FROM promise_notes
            ORDER BY updated_at DESC
            """
        )

        let decoder = JSONDecoder()
        let mapped = rows.compactMap { row -> PromiseNote? in
            guard let noteJSON = row["note_json"] as? String,
                  let data = noteJSON.data(using: .utf8) else {
                return nil
            }
            return try? decoder.decode(PromiseNote.self, from: data)
        }
        promises = mapped
    }

    private struct OutboxEnvelopeRow {
        let eventID: String
        let envelope: SemayEventEnvelope
        let attempts: Int
    }

    private struct HubBatchRequest: Encodable {
        let envelopes: [SemayEventEnvelope]
    }

    private struct HubBatchResponse: Decodable {
        struct Accepted: Decodable {
            let eventID: String

            enum CodingKeys: String, CodingKey {
                case eventID = "event_id"
            }
        }

        struct Rejected: Decodable {
            let eventID: String
            let error: String

            enum CodingKeys: String, CodingKey {
                case eventID = "event_id"
                case error
            }
        }

        let success: Bool
        let accepted: [Accepted]
        let rejected: [Rejected]
    }

    // MARK: - Private helpers

    private enum SQLiteBind {
        case text(String)
        case int(Int)
        case int64(Int64)
        case double(Double)
        case null
    }

    private func openDatabase() {
        let path = Self.databasePath()
        if sqlite3_open(path, &db) != SQLITE_OK {
            SecureLogger.error("SemayDataStore: failed to open database at \(path)", category: .session)
            db = nil
        }
    }

    private static func databasePath() -> String {
        let fileManager = FileManager.default
        let appSupport = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask).first!
        let directory = appSupport.appendingPathComponent("bitchat", isDirectory: true)
        try? fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.appendingPathComponent("semay.sqlite").path
    }

    private func migrate() {
        let statements = [
            """
            CREATE TABLE IF NOT EXISTS pins (
                pin_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                details TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                plus_code TEXT NOT NULL DEFAULT '',
                e_address TEXT NOT NULL,
                phone TEXT NOT NULL DEFAULT '',
                author_pubkey TEXT NOT NULL,
                approval_count INTEGER NOT NULL DEFAULT 0,
                is_visible INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pin_approvals (
                pin_id TEXT NOT NULL,
                approver_pubkey TEXT NOT NULL,
                distance_meters REAL NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (pin_id, approver_pubkey)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS business_profiles (
                business_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                details TEXT NOT NULL,
                latitude REAL NOT NULL DEFAULT 0,
                longitude REAL NOT NULL DEFAULT 0,
                plus_code TEXT NOT NULL DEFAULT '',
                e_address TEXT NOT NULL,
                phone TEXT NOT NULL DEFAULT '',
                owner_pubkey TEXT NOT NULL,
                qr_payload TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sync_cursor (
                key TEXT PRIMARY KEY,
                cursor_value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS promise_notes (
                promise_id TEXT PRIMARY KEY,
                merchant_id TEXT NOT NULL,
                payer_pubkey TEXT NOT NULL,
                amount_msat INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                status TEXT NOT NULL,
                note_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS settlement_receipts (
                receipt_id TEXT PRIMARY KEY,
                promise_id TEXT NOT NULL,
                receipt_json TEXT NOT NULL,
                submitted_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS event_outbox (
                event_id TEXT PRIMARY KEY,
                envelope_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_at INTEGER,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        ]

        for sql in statements {
            _ = execute(sql)
        }

        // Column-level migrations for existing installs.
        ensureColumn(table: "pins", column: "plus_code", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "pins", column: "phone", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "business_profiles", column: "latitude", definition: "REAL NOT NULL DEFAULT 0")
        ensureColumn(table: "business_profiles", column: "longitude", definition: "REAL NOT NULL DEFAULT 0")
        ensureColumn(table: "business_profiles", column: "plus_code", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "business_profiles", column: "phone", definition: "TEXT NOT NULL DEFAULT ''")

        backfillAddressesIfNeeded()
    }

    private func ensureColumn(table: String, column: String, definition: String) {
        guard !columnExists(table: table, column: column) else { return }
        _ = execute("ALTER TABLE \(table) ADD COLUMN \(column) \(definition)")
    }

    private func columnExists(table: String, column: String) -> Bool {
        let rows = query("PRAGMA table_info(\(table))")
        return rows.contains(where: { ($0["name"] as? String) == column })
    }

    private func backfillAddressesIfNeeded() {
        // Pins always have coordinates, so we can deterministically backfill plus_code and stable E-address.
        let pinRows = query("SELECT pin_id, latitude, longitude, plus_code, e_address FROM pins")
        for row in pinRows {
            guard let pinID = row["pin_id"] as? String,
                  let latitude = row["latitude"] as? Double,
                  let longitude = row["longitude"] as? Double else { continue }
            let currentPlus = (row["plus_code"] as? String) ?? ""
            let currentE = (row["e_address"] as? String) ?? ""

            let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
            if currentPlus == address.plusCode, currentE == address.eAddress {
                continue
            }

            _ = execute(
                "UPDATE pins SET plus_code = ?, e_address = ?, updated_at = ? WHERE pin_id = ?",
                binds: [
                    .text(address.plusCode),
                    .text(address.eAddress),
                    .int(Int(Date().timeIntervalSince1970)),
                    .text(pinID)
                ]
            )
        }

        // Businesses: coordinates may not exist in older rows (pre-migration). Keep existing values if coordinates are 0.
        let bizRows = query("SELECT business_id, latitude, longitude, plus_code, e_address FROM business_profiles")
        for row in bizRows {
            guard let businessID = row["business_id"] as? String,
                  let latitudeAny = row["latitude"],
                  let longitudeAny = row["longitude"] else { continue }
            let latitude = (latitudeAny as? Double) ?? Double(latitudeAny as? Int ?? 0)
            let longitude = (longitudeAny as? Double) ?? Double(longitudeAny as? Int ?? 0)
            guard latitude != 0, longitude != 0 else { continue }

            let currentPlus = (row["plus_code"] as? String) ?? ""
            let currentE = (row["e_address"] as? String) ?? ""
            let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
            if currentPlus == address.plusCode, currentE == address.eAddress {
                continue
            }

            _ = execute(
                "UPDATE business_profiles SET plus_code = ?, e_address = ?, updated_at = ? WHERE business_id = ?",
                binds: [
                    .text(address.plusCode),
                    .text(address.eAddress),
                    .int(Int(Date().timeIntervalSince1970)),
                    .text(businessID)
                ]
            )
        }
    }

    private func execute(_ sql: String, binds: [SQLiteBind] = []) -> Bool {
        queue.sync {
            guard let db else { return false }

            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }

            guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK else {
                logSQLiteError(prefix: "prepare", db: db)
                return false
            }

            bindValues(binds, statement: statement)

            guard sqlite3_step(statement) == SQLITE_DONE else {
                logSQLiteError(prefix: "step", db: db)
                return false
            }

            return true
        }
    }

    private func query(_ sql: String, binds: [SQLiteBind] = []) -> [[String: Any]] {
        queue.sync {
            guard let db else { return [] }

            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }

            guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK else {
                logSQLiteError(prefix: "prepare", db: db)
                return []
            }

            bindValues(binds, statement: statement)

            var rows: [[String: Any]] = []
            while sqlite3_step(statement) == SQLITE_ROW {
                var row: [String: Any] = [:]
                for index in 0..<sqlite3_column_count(statement) {
                    let key = String(cString: sqlite3_column_name(statement, index))
                    let type = sqlite3_column_type(statement, index)
                    switch type {
                    case SQLITE_INTEGER:
                        row[key] = Int(sqlite3_column_int64(statement, index))
                    case SQLITE_FLOAT:
                        row[key] = sqlite3_column_double(statement, index)
                    case SQLITE_TEXT:
                        if let cString = sqlite3_column_text(statement, index) {
                            row[key] = String(cString: cString)
                        }
                    case SQLITE_NULL:
                        row[key] = NSNull()
                    default:
                        break
                    }
                }
                rows.append(row)
            }
            return rows
        }
    }

    private func queryInt(_ sql: String, binds: [SQLiteBind] = []) -> Int {
        let rows = query(sql, binds: binds)
        guard let first = rows.first,
              let value = first.values.first as? Int else {
            return 0
        }
        return value
    }

    private func bindValues(_ values: [SQLiteBind], statement: OpaquePointer?) {
        for (idx, bind) in values.enumerated() {
            let index = Int32(idx + 1)
            switch bind {
            case .text(let value):
                sqlite3_bind_text(statement, index, value, -1, SQLITE_TRANSIENT)
            case .int(let value):
                sqlite3_bind_int64(statement, index, sqlite3_int64(value))
            case .int64(let value):
                sqlite3_bind_int64(statement, index, value)
            case .double(let value):
                sqlite3_bind_double(statement, index, value)
            case .null:
                sqlite3_bind_null(statement, index)
            }
        }
    }

    private func logSQLiteError(prefix: String, db: OpaquePointer?) {
        guard let db, let error = sqlite3_errmsg(db) else { return }
        SecureLogger.error("SemayDataStore \(prefix) error: \(String(cString: error))", category: .session)
    }

    private func upsertPromise(_ note: PromiseNote) {
        let now = Int(Date().timeIntervalSince1970)
        let encoded = (try? JSONEncoder().encode(note)) ?? Data()
        let jsonString = String(data: encoded, encoding: .utf8) ?? "{}"

        let sql = """
        INSERT INTO promise_notes (
            promise_id, merchant_id, payer_pubkey, amount_msat,
            expires_at, status, note_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(promise_id) DO UPDATE SET
            status = excluded.status,
            note_json = excluded.note_json,
            updated_at = excluded.updated_at
        """

        _ = execute(
            sql,
            binds: [
                .text(note.promiseID),
                .text(note.merchantID),
                .text(note.payerPubkey),
                .int64(Int64(note.amountMsat)),
                .int(note.expiresAt),
                .text(note.status.rawValue),
                .text(jsonString),
                .int(note.createdAt),
                .int(now)
            ]
        )
    }

    private func enqueueEvent(
        _ type: SemayEventEnvelope.EventType,
        entityID: String,
        payload: [String: String],
        expiresAt: Int? = nil
    ) {
        let lamport = nextLamportClock()
        let envelope: SemayEventEnvelope
        if let identity = try? idBridge.getCurrentNostrIdentity() {
            // Preferred: cryptographic Schnorr signature (BIP-340) over a stable signing hash.
            envelope = (try? SemayEventEnvelope.signed(
                eventType: type,
                entityID: entityID,
                identity: identity,
                lamportClock: lamport,
                expiresAt: expiresAt,
                payload: payload
            )) ?? SemayEventEnvelope(
                eventType: type,
                entityID: entityID,
                authorPubkey: identity.publicKeyHex.lowercased(),
                lamportClock: lamport,
                expiresAt: expiresAt,
                payload: payload,
                signature: SemayEventEnvelope.pseudoSign(
                    payloadHash: SemayEventEnvelope.canonicalPayloadHash(payload),
                    authorPubkey: identity.publicKeyHex.lowercased()
                )
            )
        } else {
            // Fallback: legacy pseudo signature (unsafe). Used only when identity is unavailable.
            let author = currentAuthorPubkey()
            let payloadHash = SemayEventEnvelope.canonicalPayloadHash(payload)
            let signature = SemayEventEnvelope.pseudoSign(payloadHash: payloadHash, authorPubkey: author)
            envelope = SemayEventEnvelope(
                eventType: type,
                entityID: entityID,
                authorPubkey: author,
                lamportClock: lamport,
                expiresAt: expiresAt,
                payload: payload,
                payloadHash: payloadHash,
                signature: signature
            )
        }

        let data = (try? JSONEncoder().encode(envelope)) ?? Data()
        let json = String(data: data, encoding: .utf8) ?? "{}"
        let now = Int(Date().timeIntervalSince1970)

        let sql = """
        INSERT OR REPLACE INTO event_outbox (
            event_id, envelope_json, status, attempts,
            next_retry_at, last_error, created_at, updated_at
        ) VALUES (?, ?, 'pending', 0, NULL, NULL, ?, ?)
        """

        _ = execute(sql, binds: [.text(envelope.eventID), .text(json), .int(now), .int(now)])
    }

    private func makeHubBatchEndpointURL() -> URL? {
        let baseURL = hubBaseURLString()
        guard !baseURL.isEmpty else { return nil }
        guard let root = URL(string: baseURL) else { return nil }
        return root.appendingPathComponent("chat").appendingPathComponent("api").appendingPathComponent("envelopes").appendingPathComponent("batch")
    }

    private func makeHubMetricsEndpointURL(windowSeconds: Int) -> URL? {
        let safeWindow = max(60, min(windowSeconds, 30 * 24 * 60 * 60))
        guard let base = makeHubBaseEnvelopesURL() else { return nil }
        var components = URLComponents(url: base.appendingPathComponent("metrics"), resolvingAgainstBaseURL: false)
        components?.queryItems = [URLQueryItem(name: "window_seconds", value: String(safeWindow))]
        return components?.url
    }

    private func makeHubBaseEnvelopesURL() -> URL? {
        let baseURL = hubBaseURLString()
        guard !baseURL.isEmpty else { return nil }
        guard let root = URL(string: baseURL) else { return nil }
        return root
            .appendingPathComponent("chat")
            .appendingPathComponent("api")
            .appendingPathComponent("envelopes")
    }

    private func loadDueOutboxRows(limit: Int) -> [OutboxEnvelopeRow] {
        let safeLimit = max(1, min(limit, 200))
        let now = Int(Date().timeIntervalSince1970)
        let rows = query(
            """
            SELECT event_id, envelope_json, attempts
            FROM event_outbox
            WHERE status IN ('pending', 'retry')
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            binds: [.int(now), .int(safeLimit)]
        )

        var output: [OutboxEnvelopeRow] = []
        for row in rows {
            guard let eventID = row["event_id"] as? String,
                  let envelopeJSON = row["envelope_json"] as? String else {
                continue
            }
            let attempts = (row["attempts"] as? Int) ?? 0
            guard let data = envelopeJSON.data(using: .utf8),
                  let envelope = try? JSONDecoder().decode(SemayEventEnvelope.self, from: data) else {
                markOutboxFailed(eventID: eventID, attempts: attempts + 1, error: "invalid-stored-envelope-json")
                continue
            }
            output.append(OutboxEnvelopeRow(eventID: eventID, envelope: envelope, attempts: attempts))
        }
        return output
    }

    private func postEnvelopeBatch(_ envelopes: [SemayEventEnvelope], to url: URL) async throws -> HubBatchResponse {
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.timeoutInterval = 20
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        let token = hubIngestToken()
        if !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        request.httpBody = try JSONEncoder().encode(HubBatchRequest(envelopes: envelopes))

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }
        guard (200...299).contains(httpResponse.statusCode) else {
            let bodyString = String(data: data, encoding: .utf8) ?? ""
            throw NSError(
                domain: "semay.hub",
                code: httpResponse.statusCode,
                userInfo: [NSLocalizedDescriptionKey: "hub-http-\(httpResponse.statusCode):\(bodyString)"]
            )
        }

        let decoded = try JSONDecoder().decode(HubBatchResponse.self, from: data)
        return decoded
    }

    private func markOutboxDelivered(eventID: String) {
        let now = Int(Date().timeIntervalSince1970)
        _ = execute(
            """
            UPDATE event_outbox
            SET status = 'delivered',
                last_error = NULL,
                next_retry_at = NULL,
                updated_at = ?
            WHERE event_id = ?
            """,
            binds: [.int(now), .text(eventID)]
        )
    }

    private func markOutboxRetry(eventID: String, attempts: Int, error: String) {
        if attempts >= maxOutboxAttempts {
            markOutboxFailed(eventID: eventID, attempts: attempts, error: error)
            return
        }

        let now = Int(Date().timeIntervalSince1970)
        let backoffSeconds = min(300, Int(pow(2.0, Double(min(attempts, 8)))))
        let nextRetry = now + backoffSeconds
        _ = execute(
            """
            UPDATE event_outbox
            SET status = 'retry',
                attempts = ?,
                next_retry_at = ?,
                last_error = ?,
                updated_at = ?
            WHERE event_id = ?
            """,
            binds: [.int(attempts), .int(nextRetry), .text(error), .int(now), .text(eventID)]
        )
    }

    private func markOutboxFailed(eventID: String, attempts: Int, error: String) {
        let now = Int(Date().timeIntervalSince1970)
        _ = execute(
            """
            UPDATE event_outbox
            SET status = 'failed',
                attempts = ?,
                next_retry_at = NULL,
                last_error = ?,
                updated_at = ?
            WHERE event_id = ?
            """,
            binds: [.int(attempts), .text(error), .int(now), .text(eventID)]
        )
    }

    private func updateOutboxEnvelopeJSON(eventID: String, envelope: SemayEventEnvelope) {
        let now = Int(Date().timeIntervalSince1970)
        let data = (try? JSONEncoder().encode(envelope)) ?? Data()
        let json = String(data: data, encoding: .utf8) ?? "{}"
        _ = execute(
            "UPDATE event_outbox SET envelope_json = ?, updated_at = ? WHERE event_id = ?",
            binds: [.text(json), .int(now), .text(eventID)]
        )
    }

    private func nextLamportClock() -> UInt64 {
        let current = UInt64(UserDefaults.standard.integer(forKey: lamportKey))
        let next = current + 1
        UserDefaults.standard.set(Int(next), forKey: lamportKey)
        return next
    }

    private func currentAuthorPubkey() -> String {
        if let pub = try? idBridge.getCurrentNostrIdentity()?.publicKeyHex,
           !pub.isEmpty {
            return pub.lowercased()
        }

        if let seed = SeedPhraseService.shared.derivedSeedMaterial() {
            return seed.sha256Fingerprint().lowercased()
        }

        return "0000000000000000000000000000000000000000000000000000000000000000"
    }

}

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
