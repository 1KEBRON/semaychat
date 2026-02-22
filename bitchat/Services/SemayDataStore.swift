import BitLogger
import CryptoKit
import Foundation
import ImageIO
import P256K
import SQLite3
import UniformTypeIdentifiers

@MainActor
final class SemayDataStore: ObservableObject {
    static let shared = SemayDataStore()
    nonisolated private static let servicePhotoMaxRefs = 3
    nonisolated private static let servicePhotoDefaultQuotaBytes: Int64 = 300 * 1024 * 1024
    nonisolated private static let contributionReviewVersion = 1
    nonisolated private static let contributionDailyPublishLimit = 20

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

    struct FeedSyncReport {
        var fetched: Int = 0
        var applied: Int = 0
        var skipped: Int = 0
        var errorMessage: String?

        var summary: String {
            "fetched \(fetched), applied \(applied), skipped \(skipped)"
        }
    }

    struct OfflinePackInstall: Equatable {
        let packID: String
        let regionCode: String
        let packVersion: String
        let tileFormat: String
        let installedPath: String
        let sha256: String
        let signature: String
        let sigAlg: String
        let minZoom: Int
        let maxZoom: Int
        let bounds: String
        let sizeBytes: Int64
        let lifecycleState: String
        let isActive: Bool
        let dependsOn: String
        let styleURL: String
        let languageCode: String
    }

    struct ContributionPublicationQueueItem: Equatable, Identifiable {
        let queueID: String
        let entityType: String
        let entityID: String
        let authorPubkey: String
        let shareScope: String
        let publishState: String
        let qualityScore: Int
        let qualityFlags: String
        let reviewVersion: Int
        let payloadJSON: String
        let createdAt: Int
        let updatedAt: Int

        var id: String { queueID }
    }

    private struct InboundApplyResult {
        let applied: Bool
        let reason: String?

        init(applied: Bool, reason: String? = nil) {
            self.applied = applied
            self.reason = reason
        }

        static let unsupportedEventType = InboundApplyResult(applied: false, reason: "unsupported-event-type")
    }

    private enum SemayEnvelopeDecodeResult {
        case success(SemayEventEnvelope)
        case failure(reason: String)
    }

    @Published private(set) var pins: [SemayMapPin] = []
    @Published private(set) var businesses: [BusinessProfile] = []
    @Published private(set) var bulletins: [BulletinPost] = []
    @Published private(set) var curatedRoutes: [SemayCuratedRoute] = []
    @Published private(set) var directoryServices: [SemayServiceDirectoryEntry] = []
    @Published private(set) var promises: [PromiseNote] = []
    @Published private(set) var mutedBulletinAuthors: Set<String> = []

    private let queue = DispatchQueue(label: "semay.data.store")
    private let idBridge = NostrIdentityBridge()
    private let lamportKey = "semay.lamport.clock"
    private let hubBaseURLKey = "semay.hub.base_url"
    private let hubIngestTokenKey = "semay.hub.ingest_token"
    private let semayNostrContentPrefix = "semay1:"
    private let semayNostrTag = "semay-v1"
    private let maxOutboxAttempts = 10
    private let routeAndServiceMaxOutboxAttempts = 4

    private var db: OpaquePointer?

    private init() {
        openDatabase()
        migrate()
        refreshAll()
        enforceServiceMediaQuota()
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

    func currentUserPubkey() -> String {
        currentAuthorPubkey()
    }

    // MARK: - Curated Routes (Diaspora transit + safety)

    @discardableResult
    func publishCuratedRoute(
        title: String,
        summary: String,
        city: String,
        fromLabel: String,
        toLabel: String,
        transportType: String,
        waypoints: [SemayRouteWaypoint] = [],
        reliabilityScore: Int = 50,
        sourceTrustTier: Int = 0
    ) -> SemayCuratedRoute {
        let routeID = UUID().uuidString.lowercased()
        let now = Int(Date().timeIntervalSince1970)
        let author = currentAuthorPubkey()
        let normalizedTransport = SemayRouteTransport(rawValue: transportType.lowercased())?.rawValue ?? SemayRouteTransport.unknown.rawValue

        let route = SemayCuratedRoute(
            routeID: routeID,
            title: title,
            summary: summary,
            city: city,
            fromLabel: fromLabel,
            toLabel: toLabel,
            transportType: normalizedTransport,
            waypoints: waypoints,
            reliabilityScore: max(0, min(100, reliabilityScore)),
            trustScore: max(0, min(100, reliabilityScore)),
            sourceTrustTier: max(0, min(4, sourceTrustTier)),
            status: "active",
            authorPubkey: author,
            createdAt: now,
            updatedAt: now
        )

        let payload = serializedPayload(for: route, eventType: .routeCuratedCreate)
        enqueueEvent(.routeCuratedCreate, entityID: "route:\(routeID)", payload: payload)

        persistCuratedRoute(route, sourceEventType: .routeCuratedCreate)
        refreshRoutes()
        refreshTrustSummariesForRoute(route.routeID)
        return route
    }

    func updateCuratedRoute(_ route: SemayCuratedRoute) {
        guard curatedRoutes.contains(where: { $0.routeID == route.routeID }) else { return }
        let now = Int(Date().timeIntervalSince1970)
        let merged = SemayCuratedRoute(
            routeID: route.routeID,
            title: route.title,
            summary: route.summary,
            city: route.city,
            fromLabel: route.fromLabel,
            toLabel: route.toLabel,
            transportType: route.transportType,
            waypoints: route.waypoints,
            reliabilityScore: route.reliabilityScore,
            trustScore: max(route.trustScore, route.reliabilityScore),
            sourceTrustTier: route.sourceTrustTier,
            status: route.status,
            authorPubkey: route.authorPubkey,
            createdAt: route.createdAt,
            updatedAt: now
        )

        enqueueEvent(
            .routeCuratedUpdate,
            entityID: "route:\(route.routeID)",
            payload: serializedPayload(for: merged, eventType: .routeCuratedUpdate)
        )
        persistCuratedRoute(merged, sourceEventType: .routeCuratedUpdate)
        refreshRoutes()
        refreshTrustSummariesForRoute(route.routeID)
    }

    func retractCuratedRoute(routeID: String) {
        guard let existing = curatedRoutes.first(where: { $0.routeID == routeID }) else { return }
        var retired = existing
        retired.status = "retracted"
        retired.updatedAt = Int(Date().timeIntervalSince1970)
        enqueueEvent(
            .routeCuratedRetract,
            entityID: "route:\(routeID)",
            payload: [
                "route_id": routeID,
                "status": "retracted",
                "updated_at": String(retired.updatedAt),
                "author_pubkey": retired.authorPubkey,
            ]
        )
        persistCuratedRoute(retired, sourceEventType: .routeCuratedRetract)
        refreshRoutes()
        refreshTrustSummariesForRoute(routeID)
    }

    @discardableResult
    func endorseCuratedRoute(routeID: String, score: Int = 1, reason: String = "verified") -> Bool {
        let now = Int(Date().timeIntervalSince1970)
        let actor = currentAuthorPubkey()
        let clampedScore = max(0, min(5, score))
        let safe = execute(
            """
            INSERT INTO route_endorsements (route_id, actor_pubkey, score, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(route_id, actor_pubkey) DO UPDATE SET
                score = excluded.score,
                reason = excluded.reason,
                created_at = excluded.created_at
            """,
            binds: [.text(routeID), .text(actor), .int(clampedScore), .text(reason), .int(now)]
        )
        guard safe else { return false }
        enqueueEvent(
            .routeCuratedEndorse,
            entityID: "route:\(routeID)",
            payload: [
                "route_id": routeID,
                "actor_pubkey": actor,
                "score": String(clampedScore),
                "reason": reason,
                "created_at": String(now),
            ]
        )
        refreshTrustSummariesForRoute(routeID)
        refreshRoutes()
        return true
    }

    func reportCuratedRoute(routeID: String, reason: String = "mismatch") {
        let now = Int(Date().timeIntervalSince1970)
        let actor = currentAuthorPubkey()
        let _ = execute(
            """
            INSERT OR REPLACE INTO route_reports (route_id, reporter_pubkey, reason, created_at)
            VALUES (?, ?, ?, ?)
            """,
            binds: [.text(routeID), .text(actor), .text(reason), .int(now)]
        )
        refreshTrustSummariesForRoute(routeID)
        refreshRoutes()
        enqueueEvent(
            .routeCuratedReport,
            entityID: "route:\(routeID)",
            payload: [
                "route_id": routeID,
                "reason": reason,
            ]
        )
    }

    var activeCuratedRoutes: [SemayCuratedRoute] {
        curatedRoutes.filter { $0.isActive }.sorted {
            if $0.trustScore != $1.trustScore {
                return $0.trustScore > $1.trustScore
            }
            return $0.updatedAt > $1.updatedAt
        }
    }

    // MARK: - Service Directory

    @discardableResult
    func publishServiceDirectoryEntry(
        name: String,
        serviceType: String,
        category: String,
        details: String,
        city: String = "",
        country: String = "",
        latitude: Double = 0,
        longitude: Double = 0,
        phone: String = "",
        website: String = "",
        emergencyContact: String = "",
        urgency: String = SemayServiceUrgency.medium.rawValue,
        tags: [String] = [],
        verified: Bool = false
    ) -> SemayServiceDirectoryEntry? {
        guard !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return nil }
        let serviceID = UUID().uuidString.lowercased()
        let now = Int(Date().timeIntervalSince1970)
        let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
        let sanitizedTags = tags
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        let tagsJSON = (try? String(data: JSONEncoder().encode(sanitizedTags), encoding: .utf8)) ?? "[]"
        let normalizedServiceType = serviceType.isEmpty ? "community-service" : serviceType
        let normalizedUrgency = ["low", "medium", "high", "critical"].contains(urgency) ? urgency : SemayServiceUrgency.medium.rawValue
        let author = currentAuthorPubkey()
        let display = SemayAddressDisplayBuilder.build(
            nameHint: name,
            latitude: latitude,
            longitude: longitude,
            plusCode: address.plusCode,
            eAddress: address.eAddress
        )
        let resolvedCity = city.isEmpty ? display.locality : city
        let resolvedCountry = country.isEmpty ? display.countryName : country
        let entry = SemayServiceDirectoryEntry(
            serviceID: serviceID,
            name: name,
            serviceType: normalizedServiceType,
            category: category,
            details: details,
            city: resolvedCity,
            country: resolvedCountry,
            latitude: latitude,
            longitude: longitude,
            plusCode: address.plusCode,
            eAddress: address.eAddress,
            addressLabel: display.addressLabel,
            locality: display.locality,
            adminArea: display.adminArea,
            countryCode: display.countryCode,
            phone: phone,
            website: website,
            emergencyContact: emergencyContact,
            urgency: normalizedUrgency,
            verified: verified,
            trustScore: verified ? 65 : 40,
            sourceTrustTier: 0,
            status: "active",
            tagsJSON: tagsJSON,
            authorPubkey: author,
            createdAt: now,
            updatedAt: now
        )

        persistServiceDirectoryEntry(entry, sourceEventType: .serviceDirectoryCreate)
        if shouldEmitServiceDirectoryNetworkEvent(for: entry) {
            enqueueEvent(
                .serviceDirectoryCreate,
                entityID: "service:\(serviceID)",
                payload: serializedPayload(for: entry)
            )
        }
        refreshServiceDirectory()
        refreshTrustSummariesForService(serviceID)
        return entry
    }

    func updateServiceDirectoryEntry(_ entry: SemayServiceDirectoryEntry) {
        guard directoryServices.contains(where: { $0.serviceID == entry.serviceID }) else { return }
        let now = Int(Date().timeIntervalSince1970)
        let merged = SemayServiceDirectoryEntry(
            serviceID: entry.serviceID,
            name: entry.name,
            serviceType: entry.serviceType,
            category: entry.category,
            details: entry.details,
            city: entry.city,
            country: entry.country,
            latitude: entry.latitude,
            longitude: entry.longitude,
            plusCode: entry.plusCode,
            eAddress: entry.eAddress,
            addressLabel: entry.addressLabel,
            locality: entry.locality,
            adminArea: entry.adminArea,
            countryCode: entry.countryCode,
            phone: entry.phone,
            website: entry.website,
            emergencyContact: entry.emergencyContact,
            urgency: entry.urgency,
            verified: entry.verified,
            trustScore: entry.trustScore,
            sourceTrustTier: entry.sourceTrustTier,
            status: entry.status,
            tagsJSON: entry.tagsJSON,
            primaryPhotoID: entry.primaryPhotoID,
            photoCount: entry.photoCount,
            shareScope: entry.shareScope,
            publishState: entry.publishState,
            qualityScore: entry.qualityScore,
            qualityReasonsJSON: entry.qualityReasonsJSON,
            reviewVersion: entry.reviewVersion,
            lastQualityCheckedAt: entry.lastQualityCheckedAt,
            authorPubkey: entry.authorPubkey,
            createdAt: entry.createdAt,
            updatedAt: now
        )
        persistServiceDirectoryEntry(merged, sourceEventType: .serviceDirectoryUpdate)
        if shouldEmitServiceDirectoryNetworkEvent(for: merged) {
            enqueueEvent(
                .serviceDirectoryUpdate,
                entityID: "service:\(entry.serviceID)",
                payload: serializedPayload(for: merged)
            )
        } else {
            removeContributionQueueEntries(entityType: "service", entityID: entry.serviceID)
            removePendingServiceOutboxEvents(serviceID: entry.serviceID)
        }
        refreshServiceDirectory()
        refreshTrustSummariesForService(entry.serviceID)
    }

    func retractServiceDirectoryEntry(serviceID: String) {
        guard let existing = directoryServices.first(where: { $0.serviceID == serviceID }) else { return }
        var retired = existing
        retired.status = "retracted"
        retired.updatedAt = Int(Date().timeIntervalSince1970)
        persistServiceDirectoryEntry(retired, sourceEventType: .serviceDirectoryRetract)
        if shouldEmitServiceDirectoryNetworkEvent(for: existing) {
            enqueueEvent(
                .serviceDirectoryRetract,
                entityID: "service:\(serviceID)",
                payload: [
                    "service_id": serviceID,
                    "status": "retracted",
                    "updated_at": String(retired.updatedAt),
                    "author_pubkey": retired.authorPubkey
                ]
            )
        }
        removeContributionQueueEntries(entityType: "service", entityID: serviceID)
        refreshServiceDirectory()
        refreshTrustSummariesForService(serviceID)
    }

    @discardableResult
    func endorseServiceDirectoryEntry(serviceID: String, score: Int = 1, reason: String = "verified") -> Bool {
        let now = Int(Date().timeIntervalSince1970)
        let actor = currentAuthorPubkey()
        let clamped = max(0, min(5, score))
        let ok = execute(
            """
            INSERT INTO service_endorsements (service_id, actor_pubkey, score, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(service_id, actor_pubkey) DO UPDATE SET
                score = excluded.score,
                reason = excluded.reason,
                created_at = excluded.created_at
            """,
            binds: [.text(serviceID), .text(actor), .int(clamped), .text(reason), .int(now)]
        )
        guard ok else { return false }
        refreshTrustSummariesForService(serviceID)
        refreshServiceDirectory()
        return true
    }

    func reportServiceDirectoryEntry(serviceID: String, reason: String = "mismatch") {
        let now = Int(Date().timeIntervalSince1970)
        let actor = currentAuthorPubkey()
        _ = execute(
            """
            INSERT OR REPLACE INTO service_reports (service_id, reporter_pubkey, reason, created_at)
            VALUES (?, ?, ?, ?)
            """,
            binds: [.text(serviceID), .text(actor), .text(reason), .int(now)]
        )
        refreshTrustSummariesForService(serviceID)
        refreshServiceDirectory()
        enqueueEvent(
            .serviceDirectoryReport,
            entityID: "service:\(serviceID)",
            payload: [
                "service_id": serviceID,
                "reason": reason,
            ]
        )
    }

    @discardableResult
    func requestNetworkShareForService(serviceID: String) -> (accepted: Bool, reasons: [String]) {
        guard let existing = serviceDirectoryEntry(for: serviceID) else {
            return (false, ["Listing not found."])
        }

        let now = Int(Date().timeIntervalSince1970)
        let quality = evaluateServiceContributionQuality(existing)
        let accepted = quality.reasons.isEmpty
        let updated = SemayServiceDirectoryEntry(
            serviceID: existing.serviceID,
            name: existing.name,
            serviceType: existing.serviceType,
            category: existing.category,
            details: existing.details,
            city: existing.city,
            country: existing.country,
            latitude: existing.latitude,
            longitude: existing.longitude,
            plusCode: existing.plusCode,
            eAddress: existing.eAddress,
            addressLabel: existing.addressLabel,
            locality: existing.locality,
            adminArea: existing.adminArea,
            countryCode: existing.countryCode,
            phone: existing.phone,
            website: existing.website,
            emergencyContact: existing.emergencyContact,
            urgency: existing.urgency,
            verified: existing.verified,
            trustScore: existing.trustScore,
            sourceTrustTier: existing.sourceTrustTier,
            status: existing.status,
            tagsJSON: existing.tagsJSON,
            primaryPhotoID: existing.primaryPhotoID,
            photoCount: existing.photoCount,
            shareScope: accepted ? .network : .personal,
            publishState: accepted ? .pendingReview : .rejected,
            qualityScore: quality.score,
            qualityReasonsJSON: quality.reasonsJSON,
            reviewVersion: SemayDataStore.contributionReviewVersion,
            lastQualityCheckedAt: now,
            authorPubkey: existing.authorPubkey,
            createdAt: existing.createdAt,
            updatedAt: max(existing.updatedAt, now)
        )

        persistServiceDirectoryEntry(updated, sourceEventType: .serviceDirectoryUpdate)
        refreshServiceDirectory()

        if accepted {
            let shareEventType: SemayEventEnvelope.EventType = existing.shareScope == .network
                ? .serviceDirectoryUpdate
                : .serviceDirectoryCreate
            enqueueEvent(
                shareEventType,
                entityID: "service:\(updated.serviceID)",
                payload: serializedPayload(for: updated)
            )
            enqueueContributionPublication(
                entityType: "service",
                entityID: updated.serviceID,
                authorPubkey: updated.authorPubkey,
                shareScope: updated.shareScope.rawValue,
                publishState: updated.publishState.rawValue,
                qualityScore: updated.qualityScore,
                qualityFlags: updated.qualityReasonsJSON,
                reviewVersion: updated.reviewVersion,
                payloadJSON: (try? String(data: JSONEncoder().encode(serializedPayload(for: updated)), encoding: .utf8)) ?? "{}",
                timestamp: now
            )
        } else {
            removeContributionQueueEntries(entityType: "service", entityID: updated.serviceID)
            removePendingServiceOutboxEvents(serviceID: updated.serviceID)
        }

        return (accepted, quality.reasons)
    }

    func setServiceContributionScope(serviceID: String, scope: SemayContributionScope) {
        switch scope {
        case .network:
            _ = requestNetworkShareForService(serviceID: serviceID)
        case .personal:
            guard let existing = serviceDirectoryEntry(for: serviceID) else { return }
            let now = Int(Date().timeIntervalSince1970)
            let updated = SemayServiceDirectoryEntry(
                serviceID: existing.serviceID,
                name: existing.name,
                serviceType: existing.serviceType,
                category: existing.category,
                details: existing.details,
                city: existing.city,
                country: existing.country,
                latitude: existing.latitude,
                longitude: existing.longitude,
                plusCode: existing.plusCode,
                eAddress: existing.eAddress,
                addressLabel: existing.addressLabel,
                locality: existing.locality,
                adminArea: existing.adminArea,
                countryCode: existing.countryCode,
                phone: existing.phone,
                website: existing.website,
                emergencyContact: existing.emergencyContact,
                urgency: existing.urgency,
                verified: existing.verified,
                trustScore: existing.trustScore,
                sourceTrustTier: existing.sourceTrustTier,
                status: existing.status,
                tagsJSON: existing.tagsJSON,
                primaryPhotoID: existing.primaryPhotoID,
                photoCount: existing.photoCount,
                shareScope: .personal,
                publishState: .localOnly,
                qualityScore: existing.qualityScore,
                qualityReasonsJSON: existing.qualityReasonsJSON,
                reviewVersion: existing.reviewVersion,
                lastQualityCheckedAt: existing.lastQualityCheckedAt,
                authorPubkey: existing.authorPubkey,
                createdAt: existing.createdAt,
                updatedAt: max(existing.updatedAt, now)
            )
            persistServiceDirectoryEntry(updated, sourceEventType: .serviceDirectoryUpdate)
            removeContributionQueueEntries(entityType: "service", entityID: serviceID)
            removePendingServiceOutboxEvents(serviceID: serviceID)
            refreshServiceDirectory()
        }
    }

    private func shouldEmitServiceDirectoryNetworkEvent(for entry: SemayServiceDirectoryEntry) -> Bool {
        entry.shareScope == .network && entry.publishState != .rejected
    }

    func pendingContributionPublications(limit: Int = 50) -> [ContributionPublicationQueueItem] {
        let safeLimit = max(1, min(limit, 200))
        let rows = query(
            """
            SELECT queue_id, entity_type, entity_id, author_pubkey, share_scope, publish_state,
                   quality_score, quality_flags, review_version, payload_json, created_at, updated_at
            FROM contribution_publication_queue
            ORDER BY created_at ASC
            LIMIT ?
            """,
            binds: [.int(safeLimit)]
        )

        return rows.compactMap { row in
            guard let queueID = row["queue_id"] as? String,
                  let entityType = row["entity_type"] as? String,
                  let entityID = row["entity_id"] as? String,
                  let authorPubkey = row["author_pubkey"] as? String,
                  let shareScope = row["share_scope"] as? String,
                  let publishState = row["publish_state"] as? String,
                  let qualityFlags = row["quality_flags"] as? String,
                  let payloadJSON = row["payload_json"] as? String,
                  let createdAt = row["created_at"] as? Int,
                  let updatedAt = row["updated_at"] as? Int else {
                return nil
            }
            let qualityScore = (row["quality_score"] as? Int) ?? 0
            let reviewVersion = (row["review_version"] as? Int) ?? 1
            return ContributionPublicationQueueItem(
                queueID: queueID,
                entityType: entityType,
                entityID: entityID,
                authorPubkey: authorPubkey,
                shareScope: shareScope,
                publishState: publishState,
                qualityScore: qualityScore,
                qualityFlags: qualityFlags,
                reviewVersion: reviewVersion,
                payloadJSON: payloadJSON,
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
    }

    var activeDirectoryServices: [SemayServiceDirectoryEntry] {
        directoryServices.filter { $0.isActive }.sorted {
            if $0.trustScore != $1.trustScore {
                return $0.trustScore > $1.trustScore
            }
            return $0.updatedAt > $1.updatedAt
        }
    }

    func servicePhotoRefs(serviceID: String) -> [SemayServicePhotoRef] {
        let rows = query(
            """
            SELECT photo_id, service_id, sha256, mime_type, width, height, bytes_full, bytes_thumb,
                   taken_at, exif_latitude, exif_longitude, geo_source, is_primary, remote_url,
                   created_at, updated_at
            FROM service_photo_refs
            WHERE service_id = ?
            ORDER BY is_primary DESC, updated_at DESC
            """,
            binds: [.text(serviceID)]
        )

        return rows.compactMap { row in
            guard let photoID = row["photo_id"] as? String,
                  let resolvedServiceID = row["service_id"] as? String,
                  let sha256 = row["sha256"] as? String,
                  let mimeType = row["mime_type"] as? String,
                  let geoSource = row["geo_source"] as? String,
                  let createdAt = row["created_at"] as? Int,
                  let updatedAt = row["updated_at"] as? Int else {
                return nil
            }

            let width = (row["width"] as? Int) ?? Int(row["width"] as? Int64 ?? 0)
            let height = (row["height"] as? Int) ?? Int(row["height"] as? Int64 ?? 0)
            let bytesFull = (row["bytes_full"] as? Int) ?? Int(row["bytes_full"] as? Int64 ?? 0)
            let bytesThumb = (row["bytes_thumb"] as? Int) ?? Int(row["bytes_thumb"] as? Int64 ?? 0)
            let takenAt = row["taken_at"] as? Int
            let exifLatitude = row["exif_latitude"] as? Double
            let exifLongitude = row["exif_longitude"] as? Double
            let remoteURLRaw = (row["remote_url"] as? String) ?? ""
            let remoteURL = remoteURLRaw.isEmpty ? nil : remoteURLRaw
            let primaryRaw = row["is_primary"]
            let primary = String(describing: primaryRaw ?? 0) == "1" || String(describing: primaryRaw ?? false).lowercased() == "true"

            return SemayServicePhotoRef(
                photoID: photoID,
                serviceID: resolvedServiceID,
                sha256: sha256,
                mimeType: mimeType,
                width: width,
                height: height,
                bytesFull: bytesFull,
                bytesThumb: bytesThumb,
                takenAt: takenAt,
                exifLatitude: exifLatitude,
                exifLongitude: exifLongitude,
                geoSource: geoSource,
                primary: primary,
                remoteURL: remoteURL,
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
    }

    func upsertServicePhotoRefs(serviceID: String, refs: [SemayServicePhotoRef], emitServiceUpdate: Bool) {
        let normalized = normalizeServicePhotoRefs(serviceID: serviceID, refs: refs)
        let now = Int(Date().timeIntervalSince1970)

        let existingRows = query(
            "SELECT photo_id, local_full_path, local_thumb_path FROM service_photo_refs WHERE service_id = ?",
            binds: [.text(serviceID)]
        )
        let keep = Set(normalized.map(\.photoID))
        for row in existingRows {
            guard let photoID = row["photo_id"] as? String else { continue }
            if keep.contains(photoID) { continue }
            removePhotoFileIfPresent(path: row["local_full_path"] as? String)
            removePhotoFileIfPresent(path: row["local_thumb_path"] as? String)
        }
        if keep.isEmpty {
            _ = execute("DELETE FROM service_photo_refs WHERE service_id = ?", binds: [.text(serviceID)])
        } else {
            var binds: [SQLiteBind] = [.text(serviceID)]
            let placeholders = normalized.map { ref in
                binds.append(.text(ref.photoID))
                return "?"
            }.joined(separator: ", ")
            _ = execute(
                "DELETE FROM service_photo_refs WHERE service_id = ? AND photo_id NOT IN (\(placeholders))",
                binds: binds
            )
        }

        for ref in normalized {
            let fullURL = servicePhotoFullURL(serviceID: serviceID, photoID: ref.photoID)
            let thumbURL = servicePhotoThumbURL(serviceID: serviceID, photoID: ref.photoID)
            let hasFull = fullURL != nil && FileManager.default.fileExists(atPath: fullURL?.path ?? "")
            let hasThumb = thumbURL != nil && FileManager.default.fileExists(atPath: thumbURL?.path ?? "")

            _ = execute(
                """
                INSERT INTO service_photo_refs (
                    photo_id, service_id, sha256, mime_type, width, height, bytes_full, bytes_thumb,
                    local_full_path, local_thumb_path, taken_at, exif_latitude, exif_longitude,
                    geo_source, is_primary, remote_url, last_accessed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(photo_id) DO UPDATE SET
                    service_id = excluded.service_id,
                    sha256 = excluded.sha256,
                    mime_type = excluded.mime_type,
                    width = excluded.width,
                    height = excluded.height,
                    bytes_full = excluded.bytes_full,
                    bytes_thumb = excluded.bytes_thumb,
                    local_full_path = excluded.local_full_path,
                    local_thumb_path = excluded.local_thumb_path,
                    taken_at = excluded.taken_at,
                    exif_latitude = excluded.exif_latitude,
                    exif_longitude = excluded.exif_longitude,
                    geo_source = excluded.geo_source,
                    is_primary = excluded.is_primary,
                    remote_url = excluded.remote_url,
                    last_accessed_at = excluded.last_accessed_at,
                    updated_at = excluded.updated_at
                """,
                binds: [
                    .text(ref.photoID),
                    .text(serviceID),
                    .text(ref.sha256),
                    .text(ref.mimeType),
                    .int(ref.width),
                    .int(ref.height),
                    .int(ref.bytesFull),
                    .int(ref.bytesThumb),
                    .text(hasFull ? (fullURL?.path ?? "") : ""),
                    .text(hasThumb ? (thumbURL?.path ?? "") : ""),
                    ref.takenAt.map(SQLiteBind.int) ?? .null,
                    ref.exifLatitude.map(SQLiteBind.double) ?? .null,
                    ref.exifLongitude.map(SQLiteBind.double) ?? .null,
                    .text(normalizeGeoSource(ref.geoSource)),
                    .int(ref.primary ? 1 : 0),
                    .text(ref.remoteURL ?? ""),
                    .int(now),
                    .int(ref.createdAt),
                    .int(max(ref.updatedAt, now))
                ]
            )
        }

        updateServicePhotoSummary(serviceID: serviceID, emitServiceUpdate: emitServiceUpdate)
        enforceServiceMediaQuota()
    }

    func removeServicePhotoRef(serviceID: String, photoID: String, emitServiceUpdate: Bool) {
        let rows = query(
            """
            SELECT local_full_path, local_thumb_path
            FROM service_photo_refs
            WHERE service_id = ? AND photo_id = ?
            LIMIT 1
            """,
            binds: [.text(serviceID), .text(photoID)]
        )
        if let row = rows.first {
            removePhotoFileIfPresent(path: row["local_full_path"] as? String)
            removePhotoFileIfPresent(path: row["local_thumb_path"] as? String)
        }
        _ = execute(
            "DELETE FROM service_photo_refs WHERE service_id = ? AND photo_id = ?",
            binds: [.text(serviceID), .text(photoID)]
        )
        updateServicePhotoSummary(serviceID: serviceID, emitServiceUpdate: emitServiceUpdate)
    }

    func enforceServiceMediaQuota(maxBytes: Int64 = SemayDataStore.servicePhotoDefaultQuotaBytes) {
        if maxBytes <= 0 { return }
        let rows = query(
            """
            SELECT photo_id, local_full_path, local_thumb_path, bytes_full, bytes_thumb, last_accessed_at, updated_at
            FROM service_photo_refs
            ORDER BY last_accessed_at ASC, updated_at ASC
            """
        )
        var total: Int64 = 0
        struct Row {
            let photoID: String
            let fullPath: String
            let thumbPath: String
            let bytesFull: Int64
            let bytesThumb: Int64
        }
        var mapped: [Row] = []
        for row in rows {
            guard let photoID = row["photo_id"] as? String else { continue }
            let fullPath = (row["local_full_path"] as? String) ?? ""
            let thumbPath = (row["local_thumb_path"] as? String) ?? ""
            let bytesFull = Int64((row["bytes_full"] as? Int) ?? Int(row["bytes_full"] as? Int64 ?? 0))
            let bytesThumb = Int64((row["bytes_thumb"] as? Int) ?? Int(row["bytes_thumb"] as? Int64 ?? 0))
            if !fullPath.isEmpty { total += max(0, bytesFull) }
            if !thumbPath.isEmpty { total += max(0, bytesThumb) }
            mapped.append(Row(photoID: photoID, fullPath: fullPath, thumbPath: thumbPath, bytesFull: bytesFull, bytesThumb: bytesThumb))
        }

        guard total > maxBytes else { return }
        let now = Int(Date().timeIntervalSince1970)
        for row in mapped {
            if total <= maxBytes { break }

            if !row.fullPath.isEmpty, FileManager.default.fileExists(atPath: row.fullPath) {
                removePhotoFileIfPresent(path: row.fullPath)
                total -= max(0, row.bytesFull)
                _ = execute(
                    "UPDATE service_photo_refs SET local_full_path = '', bytes_full = 0, last_accessed_at = ? WHERE photo_id = ?",
                    binds: [.int(now), .text(row.photoID)]
                )
            }
            if total <= maxBytes { continue }

            if !row.thumbPath.isEmpty, FileManager.default.fileExists(atPath: row.thumbPath) {
                removePhotoFileIfPresent(path: row.thumbPath)
                total -= max(0, row.bytesThumb)
                _ = execute(
                    "UPDATE service_photo_refs SET local_thumb_path = '', bytes_thumb = 0, last_accessed_at = ? WHERE photo_id = ?",
                    binds: [.int(now), .text(row.photoID)]
                )
            }
        }
    }

    @discardableResult
    func addServicePhotoFromImageData(
        serviceID: String,
        imageData: Data,
        exifLatitude: Double?,
        exifLongitude: Double?,
        geoSource: String,
        isPrimary: Bool,
        preferredPhotoID: String? = nil
    ) -> SemayServicePhotoRef? {
        guard let prepared = prepareServicePhotoVariants(from: imageData) else {
            return nil
        }
        let preferredTrimmed = preferredPhotoID?
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased() ?? ""
        let resolvedPhotoID = preferredTrimmed.isEmpty ? UUID().uuidString.lowercased() : preferredTrimmed
        guard let fullURL = servicePhotoFullURL(serviceID: serviceID, photoID: resolvedPhotoID),
              let thumbURL = servicePhotoThumbURL(serviceID: serviceID, photoID: resolvedPhotoID) else {
            return nil
        }

        do {
            try prepared.fullData.write(to: fullURL, options: [.atomic])
            try prepared.thumbData.write(to: thumbURL, options: [.atomic])
        } catch {
            removePhotoFileIfPresent(path: fullURL.path)
            removePhotoFileIfPresent(path: thumbURL.path)
            return nil
        }

        let now = Int(Date().timeIntervalSince1970)
        var refs = servicePhotoRefs(serviceID: serviceID)
        if let duplicate = refs.first(where: { $0.sha256 == prepared.sha256 }) {
            removePhotoFileIfPresent(path: fullURL.path)
            removePhotoFileIfPresent(path: thumbURL.path)
            if isPrimary {
                refs = refs.map { ref in
                    SemayServicePhotoRef(
                        photoID: ref.photoID,
                        serviceID: ref.serviceID,
                        sha256: ref.sha256,
                        mimeType: ref.mimeType,
                        width: ref.width,
                        height: ref.height,
                        bytesFull: ref.bytesFull,
                        bytesThumb: ref.bytesThumb,
                        takenAt: ref.takenAt,
                        exifLatitude: ref.exifLatitude,
                        exifLongitude: ref.exifLongitude,
                        geoSource: ref.geoSource,
                        primary: ref.photoID == duplicate.photoID,
                        remoteURL: ref.remoteURL,
                        createdAt: ref.createdAt,
                        updatedAt: now
                    )
                }
                upsertServicePhotoRefs(serviceID: serviceID, refs: refs, emitServiceUpdate: true)
                return servicePhotoRefs(serviceID: serviceID).first(where: { $0.photoID == duplicate.photoID })
            }
            return duplicate
        }

        let newRef = SemayServicePhotoRef(
            photoID: resolvedPhotoID,
            serviceID: serviceID,
            sha256: prepared.sha256,
            mimeType: "image/jpeg",
            width: prepared.width,
            height: prepared.height,
            bytesFull: prepared.fullData.count,
            bytesThumb: prepared.thumbData.count,
            takenAt: nil,
            exifLatitude: exifLatitude,
            exifLongitude: exifLongitude,
            geoSource: normalizeGeoSource(geoSource),
            primary: isPrimary,
            remoteURL: nil,
            createdAt: now,
            updatedAt: now
        )
        refs.append(newRef)
        refs = normalizeServicePhotoRefs(serviceID: serviceID, refs: refs)
        upsertServicePhotoRefs(serviceID: serviceID, refs: refs, emitServiceUpdate: true)
        return servicePhotoRefs(serviceID: serviceID).first(where: { $0.photoID == newRef.photoID })
    }

    func servicePhotoThumbURL(serviceID: String, photoID: String) -> URL? {
        guard let base = servicePhotoDirectoryURL(serviceID: serviceID) else { return nil }
        return base.appendingPathComponent("\(photoID).thumb.jpg")
    }

    func servicePhotoFullURL(serviceID: String, photoID: String) -> URL? {
        guard let base = servicePhotoDirectoryURL(serviceID: serviceID) else { return nil }
        return base.appendingPathComponent("\(photoID).jpg")
    }

    func touchServicePhoto(serviceID: String, photoID: String) {
        let now = Int(Date().timeIntervalSince1970)
        _ = execute(
            "UPDATE service_photo_refs SET last_accessed_at = ? WHERE service_id = ? AND photo_id = ?",
            binds: [.int(now), .text(serviceID), .text(photoID)]
        )
    }

    private func normalizeServicePhotoRefs(serviceID: String, refs: [SemayServicePhotoRef]) -> [SemayServicePhotoRef] {
        let now = Int(Date().timeIntervalSince1970)
        var dedupByPhotoID: [String: SemayServicePhotoRef] = [:]
        for ref in refs {
            let normalized = SemayServicePhotoRef(
                photoID: ref.photoID,
                serviceID: serviceID,
                sha256: ref.sha256,
                mimeType: ref.mimeType,
                width: max(0, ref.width),
                height: max(0, ref.height),
                bytesFull: max(0, ref.bytesFull),
                bytesThumb: max(0, ref.bytesThumb),
                takenAt: ref.takenAt,
                exifLatitude: ref.exifLatitude,
                exifLongitude: ref.exifLongitude,
                geoSource: normalizeGeoSource(ref.geoSource),
                primary: ref.primary,
                remoteURL: ref.remoteURL?.trimmingCharacters(in: .whitespacesAndNewlines),
                createdAt: ref.createdAt,
                updatedAt: max(ref.updatedAt, now)
            )
            let existing = dedupByPhotoID[normalized.photoID]
            if existing == nil || normalized.updatedAt >= (existing?.updatedAt ?? 0) {
                dedupByPhotoID[normalized.photoID] = normalized
            }
        }

        var merged = Array(dedupByPhotoID.values).sorted {
            if $0.primary != $1.primary { return $0.primary && !$1.primary }
            return $0.updatedAt > $1.updatedAt
        }

        var dedupByHash = Set<String>()
        merged = merged.filter { ref in
            guard !ref.sha256.isEmpty else { return true }
            if dedupByHash.contains(ref.sha256) { return false }
            dedupByHash.insert(ref.sha256)
            return true
        }

        if merged.count > SemayDataStore.servicePhotoMaxRefs {
            merged = Array(merged.prefix(SemayDataStore.servicePhotoMaxRefs))
        }

        let desiredPrimary = merged.first(where: { $0.primary })?.photoID ?? merged.first?.photoID ?? ""
        merged = merged.map { ref in
            SemayServicePhotoRef(
                photoID: ref.photoID,
                serviceID: ref.serviceID,
                sha256: ref.sha256,
                mimeType: ref.mimeType,
                width: ref.width,
                height: ref.height,
                bytesFull: ref.bytesFull,
                bytesThumb: ref.bytesThumb,
                takenAt: ref.takenAt,
                exifLatitude: ref.exifLatitude,
                exifLongitude: ref.exifLongitude,
                geoSource: ref.geoSource,
                primary: !desiredPrimary.isEmpty && ref.photoID == desiredPrimary,
                remoteURL: ref.remoteURL,
                createdAt: ref.createdAt,
                updatedAt: ref.updatedAt
            )
        }
        return merged
    }

    private func updateServicePhotoSummary(serviceID: String, emitServiceUpdate: Bool) {
        let refs = servicePhotoRefs(serviceID: serviceID)
        let primaryID = refs.first(where: { $0.primary })?.photoID ?? refs.first?.photoID ?? ""
        let count = refs.count
        let now = Int(Date().timeIntervalSince1970)

        if emitServiceUpdate,
           let existing = directoryServices.first(where: { $0.serviceID == serviceID }) {
            var merged = existing
            merged.primaryPhotoID = primaryID
            merged.photoCount = count
            merged.updatedAt = now
            updateServiceDirectoryEntry(merged)
            return
        }

        _ = execute(
            "UPDATE service_directory_entries SET primary_photo_id = ?, photo_count = ? WHERE service_id = ?",
            binds: [.text(primaryID), .int(count), .text(serviceID)]
        )
        refreshServiceDirectory()
    }

    private func encodePhotoRefsForPayload(_ refs: [SemayServicePhotoRef]) -> String {
        let sanitized: [[String: Any]] = refs.prefix(SemayDataStore.servicePhotoMaxRefs).map { ref in
            var item: [String: Any] = [
                "photo_id": ref.photoID,
                "sha256": ref.sha256,
                "mime_type": ref.mimeType,
                "width": ref.width,
                "height": ref.height,
                "bytes_full": ref.bytesFull,
                "bytes_thumb": ref.bytesThumb,
                "geo_source": normalizeGeoSource(ref.geoSource),
                "primary": ref.primary,
                "remote_url": ref.remoteURL ?? "",
                "updated_at": ref.updatedAt,
            ]
            if let takenAt = ref.takenAt {
                item["taken_at"] = takenAt
            }
            if let exifLatitude = ref.exifLatitude {
                item["exif_latitude"] = exifLatitude
            }
            if let exifLongitude = ref.exifLongitude {
                item["exif_longitude"] = exifLongitude
            }
            return item
        }
        guard let data = try? JSONSerialization.data(withJSONObject: sanitized, options: []),
              let text = String(data: data, encoding: .utf8) else {
            return "[]"
        }
        return text
    }

    private func decodePhotoRefsFromPayload(
        _ raw: String,
        serviceID: String,
        createdAt: Int,
        updatedAt: Int
    ) -> [SemayServicePhotoRef]? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return [] }
        guard let data = trimmed.data(using: .utf8),
              let any = try? JSONSerialization.jsonObject(with: data, options: []),
              let objects = any as? [[String: Any]] else {
            return nil
        }

        var refs: [SemayServicePhotoRef] = []
        for object in objects.prefix(SemayDataStore.servicePhotoMaxRefs) {
            guard let photoID = (object["photo_id"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !photoID.isEmpty else {
                continue
            }
            let sha = (object["sha256"] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let mime = (object["mime_type"] as? String ?? "image/jpeg").trimmingCharacters(in: .whitespacesAndNewlines)
            let width = max(0, (object["width"] as? Int) ?? 0)
            let height = max(0, (object["height"] as? Int) ?? 0)
            let bytesFull = max(0, (object["bytes_full"] as? Int) ?? 0)
            let bytesThumb = max(0, (object["bytes_thumb"] as? Int) ?? 0)
            let takenAt = object["taken_at"] as? Int
            let exifLatitude = object["exif_latitude"] as? Double
            let exifLongitude = object["exif_longitude"] as? Double
            let geoSource = normalizeGeoSource((object["geo_source"] as? String) ?? "none")
            let primary = (object["primary"] as? Bool) ?? false
            let remoteURL = ((object["remote_url"] as? String) ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            let refUpdatedAt = max(updatedAt, (object["updated_at"] as? Int) ?? updatedAt)

            refs.append(
                SemayServicePhotoRef(
                    photoID: photoID,
                    serviceID: serviceID,
                    sha256: sha,
                    mimeType: mime,
                    width: width,
                    height: height,
                    bytesFull: bytesFull,
                    bytesThumb: bytesThumb,
                    takenAt: takenAt,
                    exifLatitude: exifLatitude,
                    exifLongitude: exifLongitude,
                    geoSource: geoSource,
                    primary: primary,
                    remoteURL: remoteURL.isEmpty ? nil : remoteURL,
                    createdAt: createdAt,
                    updatedAt: refUpdatedAt
                )
            )
        }
        return normalizeServicePhotoRefs(serviceID: serviceID, refs: refs)
    }

    private struct PreparedServicePhoto {
        let fullData: Data
        let thumbData: Data
        let width: Int
        let height: Int
        let sha256: String
    }

    private func prepareServicePhotoVariants(from sourceData: Data) -> PreparedServicePhoto? {
        guard let source = CGImageSourceCreateWithData(sourceData as CFData, nil),
              let fullData = jpegVariant(from: source, maxPixel: 1600, quality: 0.86),
              let thumbData = jpegVariant(from: source, maxPixel: 320, quality: 0.80) else {
            return nil
        }
        let properties = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any]
        let width = (properties?[kCGImagePropertyPixelWidth] as? Int) ?? 0
        let height = (properties?[kCGImagePropertyPixelHeight] as? Int) ?? 0
        let sha = SHA256.hash(data: fullData).map { String(format: "%02x", $0) }.joined()
        return PreparedServicePhoto(
            fullData: fullData,
            thumbData: thumbData,
            width: width,
            height: height,
            sha256: sha
        )
    }

    private func jpegVariant(from source: CGImageSource, maxPixel: CGFloat, quality: CGFloat) -> Data? {
        let options: [CFString: Any] = [
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceThumbnailMaxPixelSize: Int(maxPixel),
            kCGImageSourceShouldCacheImmediately: false,
        ]
        guard let cgImage = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary),
              let buffer = CFDataCreateMutable(nil, 0),
              let dest = CGImageDestinationCreateWithData(buffer, UTType.jpeg.identifier as CFString, 1, nil) else {
            return nil
        }
        let props: [CFString: Any] = [kCGImageDestinationLossyCompressionQuality: quality]
        CGImageDestinationAddImage(dest, cgImage, props as CFDictionary)
        guard CGImageDestinationFinalize(dest) else { return nil }
        return buffer as Data
    }

    private func servicePhotoDirectoryURL(serviceID: String) -> URL? {
        let fm = FileManager.default
        guard let appSupport = fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else {
            return nil
        }
        let directory = appSupport
            .appendingPathComponent("bitchat", isDirectory: true)
            .appendingPathComponent("media", isDirectory: true)
            .appendingPathComponent("services", isDirectory: true)
            .appendingPathComponent(serviceID, isDirectory: true)
        do {
            try fm.createDirectory(at: directory, withIntermediateDirectories: true)
            return directory
        } catch {
            return nil
        }
    }

    private func removePhotoFileIfPresent(path: String?) {
        guard let path = path?.trimmingCharacters(in: .whitespacesAndNewlines), !path.isEmpty else { return }
        try? FileManager.default.removeItem(atPath: path)
    }

    private func normalizeGeoSource(_ raw: String) -> String {
        let normalized = raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        switch normalized {
        case "exif_confirmed", "manual":
            return normalized
        default:
            return "none"
        }
    }

    private func serializedPayload(for route: SemayCuratedRoute, eventType: SemayEventEnvelope.EventType) -> [String: String] {
        let waypointsJSON = (try? JSONEncoder().encode(route.waypoints)).flatMap { String(data: $0, encoding: .utf8) } ?? "[]"
        return [
            "route_id": route.routeID,
            "title": route.title,
            "summary": route.summary,
            "city": route.city,
            "from_label": route.fromLabel,
            "to_label": route.toLabel,
            "transport_type": route.transportType,
            "waypoints": waypointsJSON,
            "reliability_score": String(route.reliabilityScore),
            "trust_score": String(route.trustScore),
            "source_trust_tier": String(route.sourceTrustTier),
            "status": route.status,
            "author_pubkey": route.authorPubkey,
            "created_at": String(route.createdAt),
            "updated_at": String(route.updatedAt),
            "source_event": eventType.rawValue,
        ]
    }

    private func decodedWaypoints(from waypoints: [SemayRouteWaypoint]) -> String {
        let sorted = waypoints.compactMap { waypoint -> [String: String]? in
            guard isLatitudeLongitudeWithinBounds(
                latitude: waypoint.latitude,
                longitude: waypoint.longitude
            ) else { return nil }
            return [
                "title": waypoint.title ?? "",
                "latitude": String(waypoint.latitude),
                "longitude": String(waypoint.longitude),
            ]
        }
        guard let data = try? JSONSerialization.data(withJSONObject: sorted, options: []) else {
            return "[]"
        }
        return String(data: data, encoding: .utf8) ?? "[]"
    }

    private func decodedWaypoints(from waypointJSONString: String) -> [SemayRouteWaypoint] {
        let trimmed = waypointJSONString.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return [] }
        guard let data = trimmed.data(using: .utf8) else { return [] }
        if let decoded = try? JSONDecoder().decode([SemayRouteWaypoint].self, from: data) {
            return decoded.compactMap {
                if isLatitudeLongitudeWithinBounds(latitude: $0.latitude, longitude: $0.longitude) { return $0 }
                return nil
            }
        }
        return []
    }

    private func serializedPayload(for entry: SemayServiceDirectoryEntry) -> [String: String] {
        var payload: [String: String] = [
            "service_id": entry.serviceID,
            "name": entry.name,
            "service_type": entry.serviceType,
            "category": entry.category,
            "details": entry.details,
            "city": entry.city,
            "country": entry.country,
            "latitude": String(entry.latitude),
            "longitude": String(entry.longitude),
            "plus_code": entry.plusCode,
            "e_address": entry.eAddress,
            "address_label": entry.addressLabel,
            "locality": entry.locality,
            "admin_area": entry.adminArea,
            "country_code": entry.countryCode,
            "phone": entry.phone,
            "website": entry.website,
            "emergency_contact": entry.emergencyContact,
            "urgency": entry.urgency,
            "verified": String(entry.verified),
            "trust_score": String(entry.trustScore),
            "source_trust_tier": String(entry.sourceTrustTier),
            "status": entry.status,
            "tags_json": entry.tagsJSON,
            "share_scope": entry.shareScope.rawValue,
            "publish_state": entry.publishState.rawValue,
            "quality_score": String(max(0, min(100, entry.qualityScore))),
            "quality_reasons": normalizedQualityReasonsJSON(from: entry.qualityReasonsJSON),
            "review_version": String(max(1, entry.reviewVersion)),
            "last_quality_checked_at": String(max(0, entry.lastQualityCheckedAt)),
            "author_pubkey": entry.authorPubkey,
            "created_at": String(entry.createdAt),
            "updated_at": String(entry.updatedAt),
        ]

        let refs = servicePhotoRefs(serviceID: entry.serviceID)
        if !refs.isEmpty {
            payload["photo_refs_json"] = encodePhotoRefsForPayload(refs)
            let primaryID = refs.first(where: { $0.primary })?.photoID ?? refs.first?.photoID ?? ""
            payload["primary_photo_id"] = primaryID
            payload["photo_count"] = String(refs.count)
        } else if !entry.primaryPhotoID.isEmpty || entry.photoCount > 0 {
            payload["primary_photo_id"] = entry.primaryPhotoID
            payload["photo_count"] = String(max(0, entry.photoCount))
        }
        return payload
    }

    private func persistCuratedRoute(_ route: SemayCuratedRoute, sourceEventType: SemayEventEnvelope.EventType) {
        let sql = """
        INSERT INTO curated_routes (
            route_id, title, summary, city, from_label, to_label,
            transport_type, waypoints_json, reliability_score, trust_score, source_trust_tier,
            status, author_pubkey, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(route_id) DO UPDATE SET
            title = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.title ELSE curated_routes.title END,
            summary = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.summary ELSE curated_routes.summary END,
            city = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.city ELSE curated_routes.city END,
            from_label = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.from_label ELSE curated_routes.from_label END,
            to_label = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.to_label ELSE curated_routes.to_label END,
            transport_type = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.transport_type ELSE curated_routes.transport_type END,
            waypoints_json = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.waypoints_json ELSE curated_routes.waypoints_json END,
            reliability_score = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.reliability_score ELSE curated_routes.reliability_score END,
            trust_score = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.trust_score ELSE curated_routes.trust_score END,
            source_trust_tier = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.source_trust_tier ELSE curated_routes.source_trust_tier END,
            status = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.status ELSE curated_routes.status END,
            author_pubkey = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.author_pubkey ELSE curated_routes.author_pubkey END,
            updated_at = CASE WHEN excluded.updated_at >= curated_routes.updated_at THEN excluded.updated_at ELSE curated_routes.updated_at END
        """
        let waypointsJSON = decodedWaypoints(from: route.waypoints)
        _ = execute(
            sql,
            binds: [
                .text(route.routeID), .text(route.title), .text(route.summary), .text(route.city),
                .text(route.fromLabel), .text(route.toLabel), .text(route.transportType),
                .text(waypointsJSON), .int(route.reliabilityScore), .int(route.trustScore), .int(route.sourceTrustTier),
                .text(route.status), .text(route.authorPubkey), .int(route.createdAt), .int(route.updatedAt)
            ]
        )

        if sourceEventType == .routeCuratedCreate || sourceEventType == .routeCuratedUpdate || sourceEventType == .routeCuratedRetract || sourceEventType == .routeCuratedReport {
            refreshTrustSummariesForRoute(route.routeID)
        }
        refreshRoutes()
    }

    private func persistServiceDirectoryEntry(_ entry: SemayServiceDirectoryEntry, sourceEventType: SemayEventEnvelope.EventType) {
        let sql = """
        INSERT INTO service_directory_entries (
            service_id, name, service_type, category, details, city, country,
            latitude, longitude, plus_code, e_address, address_label, locality, admin_area, country_code, phone, website,
            emergency_contact, urgency, verified, trust_score, source_trust_tier,
            status, tags_json, primary_photo_id, photo_count,
            share_scope, publish_state, quality_score, quality_flags, review_version, last_quality_checked_at,
            author_pubkey, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(service_id) DO UPDATE SET
            name = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.name ELSE service_directory_entries.name END,
            service_type = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.service_type ELSE service_directory_entries.service_type END,
            category = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.category ELSE service_directory_entries.category END,
            details = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.details ELSE service_directory_entries.details END,
            city = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.city ELSE service_directory_entries.city END,
            country = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.country ELSE service_directory_entries.country END,
            latitude = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.latitude ELSE service_directory_entries.latitude END,
            longitude = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.longitude ELSE service_directory_entries.longitude END,
            plus_code = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.plus_code ELSE service_directory_entries.plus_code END,
            e_address = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.e_address ELSE service_directory_entries.e_address END,
            address_label = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.address_label ELSE service_directory_entries.address_label END,
            locality = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.locality ELSE service_directory_entries.locality END,
            admin_area = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.admin_area ELSE service_directory_entries.admin_area END,
            country_code = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.country_code ELSE service_directory_entries.country_code END,
            phone = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.phone ELSE service_directory_entries.phone END,
            website = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.website ELSE service_directory_entries.website END,
            emergency_contact = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.emergency_contact ELSE service_directory_entries.emergency_contact END,
            urgency = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.urgency ELSE service_directory_entries.urgency END,
            verified = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.verified ELSE service_directory_entries.verified END,
            trust_score = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.trust_score ELSE service_directory_entries.trust_score END,
            source_trust_tier = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.source_trust_tier ELSE service_directory_entries.source_trust_tier END,
            status = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.status ELSE service_directory_entries.status END,
            tags_json = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.tags_json ELSE service_directory_entries.tags_json END,
            primary_photo_id = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.primary_photo_id ELSE service_directory_entries.primary_photo_id END,
            photo_count = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.photo_count ELSE service_directory_entries.photo_count END,
            share_scope = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.share_scope ELSE service_directory_entries.share_scope END,
            publish_state = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.publish_state ELSE service_directory_entries.publish_state END,
            quality_score = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.quality_score ELSE service_directory_entries.quality_score END,
            quality_flags = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.quality_flags ELSE service_directory_entries.quality_flags END,
            review_version = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.review_version ELSE service_directory_entries.review_version END,
            last_quality_checked_at = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.last_quality_checked_at ELSE service_directory_entries.last_quality_checked_at END,
            author_pubkey = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.author_pubkey ELSE service_directory_entries.author_pubkey END,
            updated_at = CASE WHEN excluded.updated_at >= service_directory_entries.updated_at THEN excluded.updated_at ELSE service_directory_entries.updated_at END
        """
        _ = execute(
            sql,
            binds: [
                .text(entry.serviceID), .text(entry.name), .text(entry.serviceType), .text(entry.category),
                .text(entry.details), .text(entry.city), .text(entry.country),
                .double(entry.latitude), .double(entry.longitude), .text(entry.plusCode), .text(entry.eAddress),
                .text(entry.addressLabel), .text(entry.locality), .text(entry.adminArea), .text(entry.countryCode),
                .text(entry.phone), .text(entry.website), .text(entry.emergencyContact), .text(entry.urgency),
                .text(entry.verified ? "1" : "0"), .int(entry.trustScore), .int(entry.sourceTrustTier),
                .text(entry.status), .text(entry.tagsJSON), .text(entry.primaryPhotoID), .int(max(0, entry.photoCount)),
                .text(entry.shareScope.rawValue), .text(entry.publishState.rawValue),
                .int(max(0, min(100, entry.qualityScore))), .text(normalizedQualityReasonsJSON(from: entry.qualityReasonsJSON)),
                .int(max(1, entry.reviewVersion)), .int(max(0, entry.lastQualityCheckedAt)),
                .text(entry.authorPubkey), .int(entry.createdAt), .int(entry.updatedAt)
            ]
        )

        if sourceEventType == .serviceDirectoryCreate || sourceEventType == .serviceDirectoryUpdate || sourceEventType == .serviceDirectoryRetract || sourceEventType == .serviceDirectoryReport {
            refreshTrustSummariesForService(entry.serviceID)
        }
        refreshServiceDirectory()
    }

    private func refreshRoutes() {
        let rows = query(
            """
            SELECT route_id, title, summary, city, from_label, to_label,
                   transport_type, waypoints_json, reliability_score, trust_score,
                   source_trust_tier, status, author_pubkey, created_at, updated_at
            FROM curated_routes
            ORDER BY updated_at DESC
            """
        )

        let mapped = rows.compactMap { row -> SemayCuratedRoute? in
            guard let routeID = row["route_id"] as? String,
                  let title = row["title"] as? String,
                  let summary = row["summary"] as? String,
                  let city = row["city"] as? String,
                  let fromLabel = row["from_label"] as? String,
                  let toLabel = row["to_label"] as? String,
                  let transportType = row["transport_type"] as? String,
                  let waypointsJSON = row["waypoints_json"] as? String,
                  let reliabilityScore = row["reliability_score"] as? Int,
                  let trustScore = row["trust_score"] as? Int,
                  let sourceTrustTier = row["source_trust_tier"] as? Int,
                  let status = row["status"] as? String,
                  let authorPubkey = row["author_pubkey"] as? String,
                  let createdAt = row["created_at"] as? Int,
                  let updatedAt = row["updated_at"] as? Int else {
                return nil
            }

            let waypoints = decodedWaypoints(from: waypointsJSON)

            return SemayCuratedRoute(
                routeID: routeID,
                title: title,
                summary: summary,
                city: city,
                fromLabel: fromLabel,
                toLabel: toLabel,
                transportType: transportType,
                waypoints: waypoints,
                reliabilityScore: reliabilityScore,
                trustScore: trustScore,
                sourceTrustTier: sourceTrustTier,
                status: status,
                authorPubkey: authorPubkey,
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
        curatedRoutes = mapped
    }

    private func refreshServiceDirectory() {
        let rows = query(
            """
            SELECT service_id, name, service_type, category, details, city, country,
                   latitude, longitude, plus_code, e_address, address_label, locality, admin_area, country_code, phone, website,
                   emergency_contact, urgency, verified, trust_score, source_trust_tier,
                   status, tags_json, primary_photo_id, photo_count,
                   share_scope, publish_state, quality_score, quality_flags, review_version, last_quality_checked_at,
                   author_pubkey, created_at, updated_at
            FROM service_directory_entries
            ORDER BY updated_at DESC
            """
        )
        let mapped = rows.compactMap(decodeServiceDirectoryEntryRow)
        directoryServices = mapped
    }

    private func serviceDirectoryEntry(for serviceID: String) -> SemayServiceDirectoryEntry? {
        if let cached = directoryServices.first(where: { $0.serviceID == serviceID }) {
            return cached
        }
        let rows = query(
            """
            SELECT service_id, name, service_type, category, details, city, country,
                   latitude, longitude, plus_code, e_address, address_label, locality, admin_area, country_code, phone, website,
                   emergency_contact, urgency, verified, trust_score, source_trust_tier,
                   status, tags_json, primary_photo_id, photo_count,
                   share_scope, publish_state, quality_score, quality_flags, review_version, last_quality_checked_at,
                   author_pubkey, created_at, updated_at
            FROM service_directory_entries
            WHERE service_id = ?
            LIMIT 1
            """,
            binds: [.text(serviceID)]
        )
        guard let row = rows.first,
              let decoded = decodeServiceDirectoryEntryRow(row) else {
            return nil
        }
        if !directoryServices.contains(where: { $0.serviceID == decoded.serviceID }) {
            directoryServices.insert(decoded, at: 0)
        }
        return decoded
    }

    private func decodeServiceDirectoryEntryRow(_ row: [String: Any]) -> SemayServiceDirectoryEntry? {
        guard let serviceID = row["service_id"] as? String,
              let name = row["name"] as? String,
              !serviceID.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return nil
        }

        let serviceType = (row["service_type"] as? String) ?? "community-service"
        let category = (row["category"] as? String) ?? "community"
        let details = (row["details"] as? String) ?? ""
        let city = (row["city"] as? String) ?? ""
        let country = (row["country"] as? String) ?? ""
        let plusCode = (row["plus_code"] as? String) ?? ""
        let eAddress = (row["e_address"] as? String) ?? ""
        let addressLabel = (row["address_label"] as? String) ?? ""
        let locality = (row["locality"] as? String) ?? ""
        let adminArea = (row["admin_area"] as? String) ?? ""
        let countryCode = (row["country_code"] as? String) ?? ""
        let phone = (row["phone"] as? String) ?? ""
        let website = (row["website"] as? String) ?? ""
        let emergencyContact = (row["emergency_contact"] as? String) ?? ""
        let urgency = (row["urgency"] as? String) ?? SemayServiceUrgency.medium.rawValue
        let status = (row["status"] as? String) ?? "active"
        let tagsJSON = (row["tags_json"] as? String) ?? "[]"
        let primaryPhotoID = (row["primary_photo_id"] as? String) ?? ""
        let qualityFlags = (row["quality_flags"] as? String) ?? "[]"
        let authorPubkey = (row["author_pubkey"] as? String) ?? currentAuthorPubkey()
        let createdAt = (row["created_at"] as? Int) ?? Int(Date().timeIntervalSince1970)
        let updatedAt = (row["updated_at"] as? Int) ?? createdAt

        let latitude = (row["latitude"] as? Double) ?? Double(row["latitude"] as? Int ?? 0)
        let longitude = (row["longitude"] as? Double) ?? Double(row["longitude"] as? Int ?? 0)

        let verifiedRaw = row["verified"]
        let verified: Bool
        if let boolValue = verifiedRaw as? Bool {
            verified = boolValue
        } else {
            verified = String(describing: verifiedRaw ?? 0) == "1" || String(describing: verifiedRaw ?? false).lowercased() == "true"
        }

        let trustScore = (row["trust_score"] as? Int) ?? Int(row["trust_score"] as? Int64 ?? 0)
        let sourceTrustTier = (row["source_trust_tier"] as? Int) ?? Int(row["source_trust_tier"] as? Int64 ?? 0)
        let photoCount = (row["photo_count"] as? Int) ?? Int(row["photo_count"] as? Int64 ?? 0)
        let qualityScore = (row["quality_score"] as? Int) ?? Int(row["quality_score"] as? Int64 ?? 0)
        let reviewVersion = (row["review_version"] as? Int) ?? Int(row["review_version"] as? Int64 ?? 1)
        let lastQualityCheckedAt = (row["last_quality_checked_at"] as? Int) ?? Int(row["last_quality_checked_at"] as? Int64 ?? 0)
        let shareScopeRaw = (row["share_scope"] as? String) ?? SemayContributionScope.personal.rawValue
        let publishStateRaw = (row["publish_state"] as? String) ?? SemayContributionPublishState.localOnly.rawValue
        let shareScope = SemayContributionScope(rawValue: shareScopeRaw) ?? .personal
        let publishState = SemayContributionPublishState(rawValue: publishStateRaw) ?? .localOnly

        return SemayServiceDirectoryEntry(
            serviceID: serviceID,
            name: name,
            serviceType: serviceType,
            category: category,
            details: details,
            city: city,
            country: country,
            latitude: latitude,
            longitude: longitude,
            plusCode: plusCode,
            eAddress: eAddress,
            addressLabel: addressLabel,
            locality: locality,
            adminArea: adminArea,
            countryCode: countryCode,
            phone: phone,
            website: website,
            emergencyContact: emergencyContact,
            urgency: urgency,
            verified: verified,
            trustScore: trustScore,
            sourceTrustTier: sourceTrustTier,
            status: status,
            tagsJSON: tagsJSON,
            primaryPhotoID: primaryPhotoID,
            photoCount: max(0, photoCount),
            shareScope: shareScope,
            publishState: publishState,
            qualityScore: max(0, min(100, qualityScore)),
            qualityReasonsJSON: normalizedQualityReasonsJSON(from: qualityFlags),
            reviewVersion: max(1, reviewVersion),
            lastQualityCheckedAt: max(0, lastQualityCheckedAt),
            authorPubkey: authorPubkey,
            createdAt: createdAt,
            updatedAt: updatedAt
        )
    }

    private func refreshTrustSummariesForRoute(_ routeID: String) {
        let endorsementRows = query("SELECT score FROM route_endorsements WHERE route_id = ?", binds: [.text(routeID)])
        let reportCount = queryInt("SELECT COUNT(1) AS count FROM route_reports WHERE route_id = ?", binds: [.text(routeID)])
        let row = query(
            "SELECT trust_score, reliability_score, source_trust_tier, updated_at FROM curated_routes WHERE route_id = ? LIMIT 1",
            binds: [.text(routeID)]
        ).first
        guard let row,
              let baseTrust = row["trust_score"] as? Int,
              let reliability = row["reliability_score"] as? Int,
              let updatedAt = row["updated_at"] as? Int else {
            return
        }

        let endorsements = endorsementRows.compactMap { Int($0["score"] as? Int ?? 0) }
        let endorsementCount = endorsements.count
        let average = endorsementCount > 0 ? endorsements.reduce(0, +) / max(1, endorsementCount) : 0
        let ageDays = max(0, (Int(Date().timeIntervalSince1970) - updatedAt) / 86_400)

        let endorsementBonus = min(35, average * 4 + endorsementCount * 3)
        let reportPenalty = min(45, reportCount * 12)
        let recencyPenalty = min(25, max(0, ageDays - 7))
        let computed = max(0, min(100, reliability + endorsementBonus - reportPenalty - recencyPenalty))
        let trustScore = max(0, max(computed, baseTrust))

        let sourceTier: Int
        switch true {
        case endorsementCount >= 4 && reportCount == 0:
            sourceTier = 3
        case endorsementCount >= 2:
            sourceTier = 2
        case endorsementCount == 1:
            sourceTier = 1
        default:
            sourceTier = 0
        }

        _ = execute(
            "UPDATE curated_routes SET trust_score = ?, source_trust_tier = ? WHERE route_id = ?",
            binds: [.int(trustScore), .int(sourceTier), .text(routeID)]
        )
    }

    private func refreshTrustSummariesForService(_ serviceID: String) {
        let endorsementRows = query("SELECT score FROM service_endorsements WHERE service_id = ?", binds: [.text(serviceID)])
        let reportCount = queryInt("SELECT COUNT(1) AS count FROM service_reports WHERE service_id = ?", binds: [.text(serviceID)])
        let row = query(
            "SELECT trust_score, source_trust_tier, verified, updated_at FROM service_directory_entries WHERE service_id = ? LIMIT 1",
            binds: [.text(serviceID)]
        ).first
        guard let row,
              let baseTrust = row["trust_score"] as? Int,
              let updatedAt = row["updated_at"] as? Int else {
            return
        }

        let verified = {
            if let boolValue = row["verified"] as? Bool { return boolValue }
            return String(describing: row["verified"] ?? "false").lowercased() == "1" || String(describing: row["verified"] ?? "false").lowercased() == "true"
        }()

        let endorsements = endorsementRows.compactMap { Int($0["score"] as? Int ?? 0) }
        let endorsementCount = endorsements.count
        let average = endorsementCount > 0 ? endorsements.reduce(0, +) / max(1, endorsementCount) : 0
        let ageDays = max(0, (Int(Date().timeIntervalSince1970) - updatedAt) / 86_400)

        let verifiedBoost = verified ? 15 : 0
        let endorsementBonus = min(35, average * 4 + endorsementCount * 3)
        let reportPenalty = min(45, reportCount * 10)
        let recencyPenalty = min(25, max(0, ageDays - 10))
        let computed = max(0, min(100, baseTrust + verifiedBoost + endorsementBonus - reportPenalty - recencyPenalty))

        let sourceTier: Int
        switch true {
        case endorsementCount >= 5 && reportCount == 0:
            sourceTier = 3
        case endorsementCount >= 2:
            sourceTier = 2
        case endorsementCount == 1:
            sourceTier = 1
        default:
            sourceTier = 0
        }

        _ = execute(
            "UPDATE service_directory_entries SET trust_score = ?, source_trust_tier = ? WHERE service_id = ?",
            binds: [.int(computed), .int(sourceTier), .text(serviceID)]
        )
    }

    @discardableResult
    private func ensureLinkedServiceForBusiness(_ business: BusinessProfile, emitEvents: Bool) -> SemayServiceDirectoryEntry? {
        let now = Int(Date().timeIntervalSince1970)
        let linkedService = linkedServiceID(entityType: "business", entityID: business.businessID)
            .flatMap { id in directoryServices.first(where: { $0.serviceID == id }) }
        let display = SemayAddressDisplayBuilder.build(
            nameHint: business.name,
            latitude: business.latitude,
            longitude: business.longitude,
            plusCode: business.plusCode,
            eAddress: business.eAddress
        )

        if let linkedService {
            let merged = SemayServiceDirectoryEntry(
                serviceID: linkedService.serviceID,
                name: business.name,
                serviceType: "business",
                category: business.category,
                details: business.details,
                city: display.locality,
                country: display.countryName,
                latitude: business.latitude,
                longitude: business.longitude,
                plusCode: business.plusCode,
                eAddress: business.eAddress,
                addressLabel: display.addressLabel,
                locality: display.locality,
                adminArea: display.adminArea,
                countryCode: display.countryCode,
                phone: business.phone,
                website: "",
                emergencyContact: "",
                urgency: SemayServiceUrgency.medium.rawValue,
                verified: linkedService.verified,
                trustScore: max(40, linkedService.trustScore),
                sourceTrustTier: linkedService.sourceTrustTier,
                status: linkedService.status,
                tagsJSON: linkedService.tagsJSON,
                primaryPhotoID: linkedService.primaryPhotoID,
                photoCount: linkedService.photoCount,
                shareScope: linkedService.shareScope,
                publishState: linkedService.publishState,
                qualityScore: linkedService.qualityScore,
                qualityReasonsJSON: linkedService.qualityReasonsJSON,
                reviewVersion: linkedService.reviewVersion,
                lastQualityCheckedAt: linkedService.lastQualityCheckedAt,
                authorPubkey: business.ownerPubkey,
                createdAt: linkedService.createdAt,
                updatedAt: now
            )
            if emitEvents {
                updateServiceDirectoryEntry(merged)
            } else {
                persistServiceDirectoryEntry(merged, sourceEventType: .serviceDirectoryUpdate)
            }
            ensureDirectoryEntityLink(serviceID: merged.serviceID, entityType: "business", entityID: business.businessID, createdAt: now)
            return merged
        }

        let tags = ["business", "yellow-pages"]
        if emitEvents {
            let created = publishServiceDirectoryEntry(
                name: business.name,
                serviceType: "business",
                category: business.category,
                details: business.details,
                city: display.locality,
                country: display.countryName,
                latitude: business.latitude,
                longitude: business.longitude,
                phone: business.phone,
                website: "",
                urgency: SemayServiceUrgency.medium.rawValue,
                tags: tags,
                verified: false
            )
            if let created {
                ensureDirectoryEntityLink(serviceID: created.serviceID, entityType: "business", entityID: business.businessID, createdAt: now)
            }
            return created
        }

        let serviceID = UUID().uuidString.lowercased()
        let created = SemayServiceDirectoryEntry(
            serviceID: serviceID,
            name: business.name,
            serviceType: "business",
            category: business.category,
            details: business.details,
            city: display.locality,
            country: display.countryName,
            latitude: business.latitude,
            longitude: business.longitude,
            plusCode: business.plusCode,
            eAddress: business.eAddress,
            addressLabel: display.addressLabel,
            locality: display.locality,
            adminArea: display.adminArea,
            countryCode: display.countryCode,
            phone: business.phone,
            website: "",
            emergencyContact: "",
            urgency: SemayServiceUrgency.medium.rawValue,
            verified: false,
            trustScore: 45,
            sourceTrustTier: 0,
            status: "active",
            tagsJSON: (try? String(data: JSONEncoder().encode(tags), encoding: .utf8)) ?? "[]",
            authorPubkey: business.ownerPubkey,
            createdAt: now,
            updatedAt: now
        )
        persistServiceDirectoryEntry(created, sourceEventType: .serviceDirectoryCreate)
        ensureDirectoryEntityLink(serviceID: created.serviceID, entityType: "business", entityID: business.businessID, createdAt: now)
        return created
    }

    @discardableResult
    private func ensureLinkedServiceForPin(_ pin: SemayMapPin, emitEvents: Bool) -> SemayServiceDirectoryEntry? {
        guard isHighValuePinType(pin.type) else { return nil }
        let now = Int(Date().timeIntervalSince1970)
        let linkedService = linkedServiceID(entityType: "pin", entityID: pin.pinID)
            .flatMap { id in directoryServices.first(where: { $0.serviceID == id }) }
        let display = SemayAddressDisplayBuilder.build(
            nameHint: pin.name,
            latitude: pin.latitude,
            longitude: pin.longitude,
            plusCode: pin.plusCode,
            eAddress: pin.eAddress
        )

        if let linkedService {
            let merged = SemayServiceDirectoryEntry(
                serviceID: linkedService.serviceID,
                name: pin.name,
                serviceType: pin.type.lowercased(),
                category: "place",
                details: pin.details,
                city: display.locality,
                country: display.countryName,
                latitude: pin.latitude,
                longitude: pin.longitude,
                plusCode: pin.plusCode,
                eAddress: pin.eAddress,
                addressLabel: display.addressLabel,
                locality: display.locality,
                adminArea: display.adminArea,
                countryCode: display.countryCode,
                phone: pin.phone,
                website: "",
                emergencyContact: "",
                urgency: SemayServiceUrgency.medium.rawValue,
                verified: pin.isVisible || linkedService.verified,
                trustScore: max(35, linkedService.trustScore),
                sourceTrustTier: linkedService.sourceTrustTier,
                status: linkedService.status,
                tagsJSON: linkedService.tagsJSON,
                primaryPhotoID: linkedService.primaryPhotoID,
                photoCount: linkedService.photoCount,
                shareScope: linkedService.shareScope,
                publishState: linkedService.publishState,
                qualityScore: linkedService.qualityScore,
                qualityReasonsJSON: linkedService.qualityReasonsJSON,
                reviewVersion: linkedService.reviewVersion,
                lastQualityCheckedAt: linkedService.lastQualityCheckedAt,
                authorPubkey: pin.authorPubkey,
                createdAt: linkedService.createdAt,
                updatedAt: now
            )
            if emitEvents {
                updateServiceDirectoryEntry(merged)
            } else {
                persistServiceDirectoryEntry(merged, sourceEventType: .serviceDirectoryUpdate)
            }
            ensureDirectoryEntityLink(serviceID: merged.serviceID, entityType: "pin", entityID: pin.pinID, createdAt: now)
            return merged
        }

        let tags = ["place", "yellow-pages"]
        if emitEvents {
            let created = publishServiceDirectoryEntry(
                name: pin.name,
                serviceType: pin.type.lowercased(),
                category: "place",
                details: pin.details,
                city: display.locality,
                country: display.countryName,
                latitude: pin.latitude,
                longitude: pin.longitude,
                phone: pin.phone,
                website: "",
                urgency: SemayServiceUrgency.medium.rawValue,
                tags: tags,
                verified: pin.isVisible
            )
            if let created {
                ensureDirectoryEntityLink(serviceID: created.serviceID, entityType: "pin", entityID: pin.pinID, createdAt: now)
            }
            return created
        }

        let serviceID = UUID().uuidString.lowercased()
        let created = SemayServiceDirectoryEntry(
            serviceID: serviceID,
            name: pin.name,
            serviceType: pin.type.lowercased(),
            category: "place",
            details: pin.details,
            city: display.locality,
            country: display.countryName,
            latitude: pin.latitude,
            longitude: pin.longitude,
            plusCode: pin.plusCode,
            eAddress: pin.eAddress,
            addressLabel: display.addressLabel,
            locality: display.locality,
            adminArea: display.adminArea,
            countryCode: display.countryCode,
            phone: pin.phone,
            website: "",
            emergencyContact: "",
            urgency: SemayServiceUrgency.medium.rawValue,
            verified: pin.isVisible,
            trustScore: 35,
            sourceTrustTier: 0,
            status: "active",
            tagsJSON: (try? String(data: JSONEncoder().encode(tags), encoding: .utf8)) ?? "[]",
            authorPubkey: pin.authorPubkey,
            createdAt: now,
            updatedAt: now
        )
        persistServiceDirectoryEntry(created, sourceEventType: .serviceDirectoryCreate)
        ensureDirectoryEntityLink(serviceID: created.serviceID, entityType: "pin", entityID: pin.pinID, createdAt: now)
        return created
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
            "details": details,
            "latitude": String(latitude),
            "longitude": String(longitude),
            "plus_code": plusCode,
            "e_address": eAddress,
            "phone": phone
        ]
        enqueueEvent(.pinCreate, entityID: "pin:\(pinID)", payload: payload)

        refreshPins()

        let created = SemayMapPin(
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
        _ = ensureLinkedServiceForPin(created, emitEvents: true)
        return created
    }

    @discardableResult
    func updatePin(
        pinID: String,
        name: String,
        type: String,
        details: String,
        latitude: Double,
        longitude: Double,
        phone: String = ""
    ) -> SemayMapPin? {
        let rows = query(
            "SELECT latitude, longitude, approval_count, is_visible, created_at FROM pins WHERE pin_id = ? LIMIT 1",
            binds: [.text(pinID)]
        )
        guard let row = rows.first,
              let oldLat = row["latitude"] as? Double,
              let oldLon = row["longitude"] as? Double
        else {
            return nil
        }

        let oldApproval = (row["approval_count"] as? Int) ?? 0
        let oldVisible = (row["is_visible"] as? Int) ?? 0
        let createdAt = (row["created_at"] as? Int) ?? Int(Date().timeIntervalSince1970)

        let now = Int(Date().timeIntervalSince1970)
        let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
        let plusCode = address.plusCode
        let eAddress = address.eAddress
        let actor = currentAuthorPubkey()

        let moved = abs(oldLat - latitude) > 0.0005 || abs(oldLon - longitude) > 0.0005
        if moved {
            _ = execute("DELETE FROM pin_approvals WHERE pin_id = ?", binds: [.text(pinID)])
        }

        if moved {
            _ = execute(
                """
                UPDATE pins
                SET name = ?, type = ?, details = ?, latitude = ?, longitude = ?,
                    plus_code = ?, e_address = ?, phone = ?,
                    author_pubkey = ?,
                    approval_count = 0, is_visible = 0,
                    updated_at = ?
                WHERE pin_id = ?
                """,
                binds: [
                    .text(name), .text(type), .text(details),
                    .double(latitude), .double(longitude),
                    .text(plusCode), .text(eAddress), .text(phone),
                    .text(actor),
                    .int(now), .text(pinID)
                ]
            )
        } else {
            _ = execute(
                """
                UPDATE pins
                SET name = ?, type = ?, details = ?, latitude = ?, longitude = ?,
                    plus_code = ?, e_address = ?, phone = ?,
                    author_pubkey = ?,
                    updated_at = ?
                WHERE pin_id = ?
                """,
                binds: [
                    .text(name), .text(type), .text(details),
                    .double(latitude), .double(longitude),
                    .text(plusCode), .text(eAddress), .text(phone),
                    .text(actor),
                    .int(now), .text(pinID)
                ]
            )
        }

        let payload = [
            "pin_id": pinID,
            "name": name,
            "type": type,
            "details": details,
            "latitude": String(latitude),
            "longitude": String(longitude),
            "plus_code": plusCode,
            "e_address": eAddress,
            "phone": phone
        ]
        enqueueEvent(.pinUpdate, entityID: "pin:\(pinID)", payload: payload)

        refreshPins()

        let updatedPin = SemayMapPin(
            pinID: pinID,
            name: name,
            type: type,
            details: details,
            latitude: latitude,
            longitude: longitude,
            plusCode: plusCode,
            eAddress: eAddress,
            phone: phone,
            authorPubkey: actor,
            approvalCount: moved ? 0 : oldApproval,
            isVisible: moved ? false : (oldVisible != 0),
            createdAt: createdAt,
            updatedAt: now
        )
        _ = ensureLinkedServiceForPin(updatedPin, emitEvents: true)
        return updatedPin
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
        if let pin = pins.first(where: { $0.pinID == pinID }) {
            _ = ensureLinkedServiceForPin(pin, emitEvents: true)
        }
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
        phone: String = "",
        lightningLink: String = "",
        cashuLink: String = ""
    ) -> BusinessProfile {
        let businessID = UUID().uuidString.lowercased()
        let now = Int(Date().timeIntervalSince1970)
        let owner = currentAuthorPubkey()

        let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
        let plusCode = address.plusCode
        let eAddress = address.eAddress
        let qrPayload = "semay://business/\(businessID)"
        let normalizedLightning = normalizeLightningLink(lightningLink)
        let normalizedCashu = cashuLink.trimmingCharacters(in: .whitespacesAndNewlines)

        let sql = """
        INSERT INTO business_profiles (
            business_id, name, category, details, latitude, longitude, plus_code, e_address, phone,
            lightning_link, cashu_link,
            owner_pubkey, qr_payload, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        _ = execute(
            sql,
            binds: [
                .text(businessID), .text(name), .text(category), .text(details),
                .double(latitude), .double(longitude), .text(plusCode), .text(eAddress), .text(phone),
                .text(normalizedLightning), .text(normalizedCashu),
                .text(owner), .text(qrPayload), .int(now), .int(now)
            ]
        )

        enqueueEvent(
            .businessRegister,
            entityID: "business:\(businessID)",
            payload: [
                "business_id": businessID,
                "name": name,
                "category": category,
                "details": details,
                "latitude": String(latitude),
                "longitude": String(longitude),
                "plus_code": plusCode,
                "e_address": eAddress,
                "phone": phone,
                "lightning_link": normalizedLightning,
                "cashu_link": normalizedCashu
            ]
        )

        refreshBusinesses()

        let createdBusiness = BusinessProfile(
            businessID: businessID,
            name: name,
            category: category,
            details: details,
            latitude: latitude,
            longitude: longitude,
            plusCode: plusCode,
            eAddress: eAddress,
            phone: phone,
            lightningLink: normalizedLightning,
            cashuLink: normalizedCashu,
            ownerPubkey: owner,
            qrPayload: qrPayload,
            createdAt: now,
            updatedAt: now
        )
        _ = ensureLinkedServiceForBusiness(createdBusiness, emitEvents: true)
        return createdBusiness
    }

    @discardableResult
    func updateBusiness(
        businessID: String,
        name: String,
        category: String,
        details: String,
        latitude: Double,
        longitude: Double,
        phone: String = "",
        lightningLink: String = "",
        cashuLink: String = ""
    ) -> BusinessProfile? {
        let rows = query(
            "SELECT owner_pubkey, created_at FROM business_profiles WHERE business_id = ? LIMIT 1",
            binds: [.text(businessID)]
        )
        guard let row = rows.first,
              let ownerPubkey = row["owner_pubkey"] as? String else {
            return nil
        }

        let actor = currentAuthorPubkey()
        guard actor.lowercased() == ownerPubkey.lowercased() else {
            // Owner-only edits for MVP.
            return nil
        }

        let createdAt = (row["created_at"] as? Int) ?? Int(Date().timeIntervalSince1970)
        let now = Int(Date().timeIntervalSince1970)
        let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
        let plusCode = address.plusCode
        let eAddress = address.eAddress
        let qrPayload = "semay://business/\(businessID)"
        let normalizedLightning = normalizeLightningLink(lightningLink)
        let normalizedCashu = cashuLink.trimmingCharacters(in: .whitespacesAndNewlines)

        _ = execute(
            """
            UPDATE business_profiles
            SET name = ?, category = ?, details = ?, latitude = ?, longitude = ?,
                plus_code = ?, e_address = ?, phone = ?,
                lightning_link = ?, cashu_link = ?,
                qr_payload = ?,
                updated_at = ?
            WHERE business_id = ?
            """,
            binds: [
                .text(name), .text(category), .text(details),
                .double(latitude), .double(longitude),
                .text(plusCode), .text(eAddress), .text(phone),
                .text(normalizedLightning), .text(normalizedCashu),
                .text(qrPayload),
                .int(now), .text(businessID)
            ]
        )

        enqueueEvent(
            .businessUpdate,
            entityID: "business:\(businessID)",
            payload: [
                "business_id": businessID,
                "name": name,
                "category": category,
                "details": details,
                "latitude": String(latitude),
                "longitude": String(longitude),
                "plus_code": plusCode,
                "e_address": eAddress,
                "phone": phone,
                "lightning_link": normalizedLightning,
                "cashu_link": normalizedCashu
            ]
        )

        refreshBusinesses()

        let updatedBusiness = BusinessProfile(
            businessID: businessID,
            name: name,
            category: category,
            details: details,
            latitude: latitude,
            longitude: longitude,
            plusCode: plusCode,
            eAddress: eAddress,
            phone: phone,
            lightningLink: normalizedLightning,
            cashuLink: normalizedCashu,
            ownerPubkey: ownerPubkey.lowercased(),
            qrPayload: qrPayload,
            createdAt: createdAt,
            updatedAt: now
        )
        _ = ensureLinkedServiceForBusiness(updatedBusiness, emitEvents: true)
        return updatedBusiness
    }

    // MARK: - Bulletins

    func visibleBulletins() -> [BulletinPost] {
        bulletins.filter { !mutedBulletinAuthors.contains($0.authorPubkey.lowercased()) }
    }

    @discardableResult
    func postBulletin(
        title: String,
        category: BulletinCategory,
        body: String,
        phone: String = "",
        latitude: Double,
        longitude: Double
    ) -> BulletinPost {
        let bulletinID = UUID().uuidString.lowercased()
        let now = Int(Date().timeIntervalSince1970)
        let author = currentAuthorPubkey()
        let address = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
        let plusCode = address.plusCode
        let eAddress = address.eAddress

        let sql = """
        INSERT INTO bulletins (
            bulletin_id, title, category, body, phone, latitude, longitude,
            plus_code, e_address, author_pubkey, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        _ = execute(
            sql,
            binds: [
                .text(bulletinID), .text(title), .text(category.rawValue), .text(body), .text(phone),
                .double(latitude), .double(longitude),
                .text(plusCode), .text(eAddress), .text(author),
                .int(now), .int(now)
            ]
        )

        enqueueEvent(
            .bulletinPost,
            entityID: "bulletin:\(bulletinID)",
            payload: [
                "bulletin_id": bulletinID,
                "title": title,
                "category": category.rawValue,
                "body": body,
                "phone": phone,
                "latitude": String(latitude),
                "longitude": String(longitude),
                "plus_code": plusCode,
                "e_address": eAddress
            ]
        )

        refreshBulletins()
        refreshBulletinModeration()

        return BulletinPost(
            bulletinID: bulletinID,
            title: title,
            category: category,
            body: body,
            phone: phone,
            latitude: latitude,
            longitude: longitude,
            plusCode: plusCode,
            eAddress: eAddress,
            authorPubkey: author,
            createdAt: now,
            updatedAt: now
        )
    }

    func reportBulletin(bulletinID: String, reason: String = "inaccurate") {
        let now = Int(Date().timeIntervalSince1970)
        let reporter = currentAuthorPubkey()
        _ = execute(
            """
            INSERT OR REPLACE INTO bulletin_reports (
                bulletin_id, reporter_pubkey, reason, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            binds: [.text(bulletinID), .text(reporter), .text(reason), .int(now)]
        )
    }

    func setBulletinAuthorMuted(_ authorPubkey: String, muted: Bool) {
        let author = authorPubkey.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !author.isEmpty else { return }
        let now = Int(Date().timeIntervalSince1970)
        if muted {
            _ = execute(
                "INSERT OR REPLACE INTO bulletin_mutes (author_pubkey, created_at) VALUES (?, ?)",
                binds: [.text(author), .int(now)]
            )
        } else {
            _ = execute(
                "DELETE FROM bulletin_mutes WHERE author_pubkey = ?",
                binds: [.text(author)]
            )
        }
        refreshBulletinModeration()
    }

    func isBulletinAuthorMuted(_ authorPubkey: String) -> Bool {
        mutedBulletinAuthors.contains(authorPubkey.lowercased())
    }

    // MARK: - Promise Ledger

    private struct PromiseNoteSigningPayload: Codable {
        let promiseID: String
        let merchantID: String
        let payerPubkey: String
        let amountMsat: UInt64
        let currency: String
        let expiresAt: Int
        let nonce: String

        enum CodingKeys: String, CodingKey {
            case promiseID = "promise_id"
            case merchantID = "merchant_id"
            case payerPubkey = "payer_pubkey"
            case amountMsat = "amount_msat"
            case currency
            case expiresAt = "expires_at"
            case nonce
        }
    }

    private struct SettlementReceiptSigningPayload: Codable {
        let receiptID: String
        let promiseID: String
        let proofType: String
        let proofValue: String
        let submittedBy: String
        let submittedAt: Int

        enum CodingKeys: String, CodingKey {
            case receiptID = "receipt_id"
            case promiseID = "promise_id"
            case proofType = "proof_type"
            case proofValue = "proof_value"
            case submittedBy = "submitted_by"
            case submittedAt = "submitted_at"
        }
    }

    private func schnorrSignHex(messageHash: Data, identity: NostrIdentity) -> String? {
        guard messageHash.count == 32 else { return nil }
        do {
            let key = try identity.schnorrSigningKey()
            var messageBytes = [UInt8](messageHash)
            var auxRand = [UInt8](repeating: 0, count: 32)
            _ = auxRand.withUnsafeMutableBytes { ptr in
                SecRandomCopyBytes(kSecRandomDefault, 32, ptr.baseAddress!)
            }
            let sig = try key.signature(message: &messageBytes, auxiliaryRand: &auxRand)
            return sig.dataRepresentation.hexEncodedString()
        } catch {
            return nil
        }
    }

    private func canonicalJSONHash<T: Encodable>(_ value: T) -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let data = (try? encoder.encode(value)) ?? Data()
        return data.sha256Hash()
    }

    @discardableResult
    func createPromise(merchantID: String, amountMsat: UInt64, fiatQuote: String? = nil) -> PromiseNote {
        let now = Int(Date().timeIntervalSince1970)
        let expiry = PromiseNote.defaultExpiry()
        let payerIdentity = try? idBridge.getCurrentNostrIdentity()
        let payer = payerIdentity?.publicKeyHex.lowercased() ?? currentAuthorPubkey()

        let nonce = Data((0..<32).map { _ in UInt8.random(in: 0...255) }).hexEncodedString()
        let signaturePayload = PromiseNoteSigningPayload(
            promiseID: UUID().uuidString.lowercased(),
            merchantID: merchantID,
            payerPubkey: payer,
            amountMsat: amountMsat,
            currency: "BTC_LN",
            expiresAt: expiry,
            nonce: nonce
        )
        let payerSignature = payerIdentity.flatMap { schnorrSignHex(messageHash: canonicalJSONHash(signaturePayload), identity: $0) }
            ?? SemayEventEnvelope.pseudoSign(payloadHash: nonce, authorPubkey: payer)

        // Use the same promise_id for the note and its signature payload.
        let promiseID = signaturePayload.promiseID

        let note = PromiseNote(
            promiseID: promiseID,
            merchantID: merchantID,
            payerPubkey: payer,
            amountMsat: amountMsat,
            fiatQuote: fiatQuote,
            expiresAt: expiry,
            nonce: nonce,
            payerSignature: payerSignature,
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

    /// Build a signed promise.create envelope suitable for QR / peer-to-peer transfer.
    /// Promise events are intentionally not uploaded to nodes/hubs by default (hostile-default privacy).
    func makePromiseCreateEnvelope(for note: PromiseNote) -> SemayEventEnvelope? {
        let lamport = nextLamportClock()
        let payload: [String: String] = [
            "promise_id": note.promiseID,
            "merchant_id": note.merchantID,
            "payer_pubkey": note.payerPubkey,
            "amount_msat": String(note.amountMsat),
            "fiat_quote": note.fiatQuote ?? "",
            "currency": note.currency,
            "expires_at": String(note.expiresAt),
            "nonce": note.nonce,
            "payer_signature": note.payerSignature,
            "status": note.status.rawValue
        ]

        if let identity = try? idBridge.getCurrentNostrIdentity() {
            return (try? SemayEventEnvelope.signed(
                eventType: .promiseCreate,
                entityID: "promise:\(note.promiseID)",
                identity: identity,
                lamportClock: lamport,
                expiresAt: note.expiresAt,
                payload: payload,
                eventID: UUID().uuidString.lowercased(),
                createdAt: note.createdAt
            ))
        }

        let author = currentAuthorPubkey()
        let payloadHash = SemayEventEnvelope.canonicalPayloadHash(payload)
        let signature = SemayEventEnvelope.pseudoSign(payloadHash: payloadHash, authorPubkey: author)
        return SemayEventEnvelope(
            eventType: .promiseCreate,
            entityID: "promise:\(note.promiseID)",
            authorPubkey: author,
            eventID: UUID().uuidString.lowercased(),
            createdAt: note.createdAt,
            lamportClock: lamport,
            expiresAt: note.expiresAt,
            payload: payload,
            payloadHash: payloadHash,
            signature: signature
        )
    }

    /// Build a signed promise.accept / promise.reject envelope in response to a promise.create.
    func makePromiseResponseEnvelope(
        promiseID: String,
        merchantID: String,
        status: PromiseStatus
    ) -> SemayEventEnvelope? {
        let eventType: SemayEventEnvelope.EventType
        switch status {
        case .accepted: eventType = .promiseAccept
        case .rejected: eventType = .promiseReject
        default:
            return nil
        }

        let lamport = nextLamportClock()
        let now = Int(Date().timeIntervalSince1970)
        let payload: [String: String] = [
            "promise_id": promiseID,
            "merchant_id": merchantID,
            "status": status.rawValue,
            "responded_at": String(now)
        ]

        if let identity = try? idBridge.getCurrentNostrIdentity() {
            return (try? SemayEventEnvelope.signed(
                eventType: eventType,
                entityID: "promise:\(promiseID)",
                identity: identity,
                lamportClock: lamport,
                expiresAt: nil,
                payload: payload
            ))
        }

        let author = currentAuthorPubkey()
        let payloadHash = SemayEventEnvelope.canonicalPayloadHash(payload)
        let signature = SemayEventEnvelope.pseudoSign(payloadHash: payloadHash, authorPubkey: author)
        return SemayEventEnvelope(
            eventType: eventType,
            entityID: "promise:\(promiseID)",
            authorPubkey: author,
            lamportClock: lamport,
            expiresAt: nil,
            payload: payload,
            payloadHash: payloadHash,
            signature: signature
        )
    }

    /// Import a promise.create envelope into the local ledger (e.g., when a merchant scans a payer QR).
    @discardableResult
    func importPromiseCreateEnvelope(_ envelope: SemayEventEnvelope) -> PromiseNote? {
        guard envelope.eventType == .promiseCreate else { return nil }
        if envelope.validate() != nil { return nil }

        let p = envelope.payload
        guard let promiseID = p["promise_id"], !promiseID.isEmpty else { return nil }
        guard let merchantID = p["merchant_id"], !merchantID.isEmpty else { return nil }

        if let claimedPayer = p["payer_pubkey"], !claimedPayer.isEmpty,
           claimedPayer.lowercased() != envelope.authorPubkey.lowercased() {
            return nil
        }
        let payer = (p["payer_pubkey"] ?? envelope.authorPubkey).lowercased()
        guard let amount = UInt64(p["amount_msat"] ?? "") else { return nil }

        let expiresAt = Int(p["expires_at"] ?? "") ?? envelope.expiresAt ?? PromiseNote.defaultExpiry()
        let nonce = p["nonce"] ?? ""
        let payerSig = p["payer_signature"] ?? envelope.signature

        let note = PromiseNote(
            promiseID: promiseID,
            merchantID: merchantID,
            payerPubkey: payer,
            amountMsat: amount,
            fiatQuote: (p["fiat_quote"] ?? "").isEmpty ? nil : (p["fiat_quote"] ?? ""),
            currency: (p["currency"] ?? "").isEmpty ? "BTC_LN" : (p["currency"] ?? "BTC_LN"),
            expiresAt: expiresAt,
            nonce: nonce,
            payerSignature: payerSig,
            status: .pending,
            createdAt: envelope.createdAt,
            updatedAt: Int(Date().timeIntervalSince1970)
        )

        upsertPromise(note)
        refreshPromises()
        return note
    }

    /// Apply a promise.accept / promise.reject envelope to the local ledger (e.g., payer scans merchant response QR).
    @discardableResult
    func applyPromiseResponseEnvelope(_ envelope: SemayEventEnvelope) -> PromiseNote? {
        guard envelope.eventType == .promiseAccept || envelope.eventType == .promiseReject else { return nil }
        if envelope.validate() != nil { return nil }

        let p = envelope.payload
        guard let promiseID = p["promise_id"], !promiseID.isEmpty else { return nil }

        let status: PromiseStatus = (envelope.eventType == .promiseAccept) ? .accepted : .rejected

        let now = Int(Date().timeIntervalSince1970)
        _ = execute(
            "UPDATE promise_notes SET status = ?, updated_at = ? WHERE promise_id = ?",
            binds: [.text(status.rawValue), .int(now), .text(promiseID)]
        )
        refreshPromises()
        return promises.first(where: { $0.promiseID == promiseID })
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

        let identity = try? idBridge.getCurrentNostrIdentity()
        let author = identity?.publicKeyHex.lowercased() ?? currentAuthorPubkey()

        let receiptID = UUID().uuidString.lowercased()
        let payload = SettlementReceiptSigningPayload(
            receiptID: receiptID,
            promiseID: promiseID,
            proofType: proofType.rawValue,
            proofValue: proofValue,
            submittedBy: submittedBy.rawValue,
            submittedAt: Int(Date().timeIntervalSince1970)
        )
        let sig = identity.flatMap { schnorrSignHex(messageHash: canonicalJSONHash(payload), identity: $0) }
            ?? SemayEventEnvelope.pseudoSign(payloadHash: Data(proofValue.utf8).sha256Hash().hexEncodedString(), authorPubkey: author)

        let receipt = SettlementReceipt(
            receiptID: receiptID,
            promiseID: promiseID,
            proofType: proofType,
            proofValue: proofValue,
            submittedBy: submittedBy,
            submittedAt: payload.submittedAt,
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

    func upsertOfflinePackInstall(_ install: OfflinePackInstall) {
        let now = Int(Date().timeIntervalSince1970)
        _ = execute(
            """
            INSERT INTO offline_pack_installs (
                pack_id, region_code, pack_version, tile_format, installed_path,
                sha256, signature, sig_alg, min_zoom, max_zoom, bounds, size_bytes,
                installed_at, is_active, lifecycle_state, depends_on, style_url, lang
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(installed_path) DO UPDATE SET
                pack_id = excluded.pack_id,
                region_code = excluded.region_code,
                pack_version = excluded.pack_version,
                tile_format = excluded.tile_format,
                sha256 = CASE WHEN excluded.sha256 != '' THEN excluded.sha256 ELSE offline_pack_installs.sha256 END,
                signature = CASE WHEN excluded.signature != '' THEN excluded.signature ELSE offline_pack_installs.signature END,
                sig_alg = CASE WHEN excluded.sig_alg != '' THEN excluded.sig_alg ELSE offline_pack_installs.sig_alg END,
                min_zoom = excluded.min_zoom,
                max_zoom = excluded.max_zoom,
                bounds = excluded.bounds,
                size_bytes = excluded.size_bytes,
                installed_at = excluded.installed_at,
                is_active = excluded.is_active,
                lifecycle_state = excluded.lifecycle_state,
                depends_on = excluded.depends_on,
                style_url = excluded.style_url,
                lang = excluded.lang
            """,
            binds: [
                .text(install.packID),
                .text(install.regionCode),
                .text(install.packVersion),
                .text(install.tileFormat),
                .text(install.installedPath),
                .text(install.sha256),
                .text(install.signature),
                .text(install.sigAlg),
                .int(install.minZoom),
                .int(install.maxZoom),
                .text(install.bounds),
                .int64(install.sizeBytes),
                .int(now),
                .int(install.isActive ? 1 : 0),
                .text(install.lifecycleState),
                .text(install.dependsOn),
                .text(install.styleURL),
                .text(install.languageCode),
            ]
        )
    }

    func offlinePackInstall(path: String) -> OfflinePackInstall? {
        let rows = query(
            """
            SELECT pack_id, region_code, pack_version, tile_format, installed_path,
                   sha256, signature, sig_alg, min_zoom, max_zoom, bounds, size_bytes,
                   is_active, lifecycle_state, depends_on, style_url, lang
            FROM offline_pack_installs
            WHERE installed_path = ?
            LIMIT 1
            """,
            binds: [.text(path)]
        )
        guard let row = rows.first else { return nil }
        return OfflinePackInstall(
            packID: (row["pack_id"] as? String) ?? "",
            regionCode: (row["region_code"] as? String) ?? "",
            packVersion: (row["pack_version"] as? String) ?? "",
            tileFormat: (row["tile_format"] as? String) ?? "",
            installedPath: (row["installed_path"] as? String) ?? "",
            sha256: (row["sha256"] as? String) ?? "",
            signature: (row["signature"] as? String) ?? "",
            sigAlg: (row["sig_alg"] as? String) ?? "",
            minZoom: (row["min_zoom"] as? Int) ?? 0,
            maxZoom: (row["max_zoom"] as? Int) ?? 0,
            bounds: (row["bounds"] as? String) ?? "",
            sizeBytes: Int64((row["size_bytes"] as? Int) ?? 0),
            lifecycleState: (row["lifecycle_state"] as? String) ?? "discovered",
            isActive: ((row["is_active"] as? Int) ?? 0) == 1,
            dependsOn: (row["depends_on"] as? String) ?? "",
            styleURL: (row["style_url"] as? String) ?? "",
            languageCode: (row["lang"] as? String) ?? ""
        )
    }

    func setActiveOfflinePack(path: String?) {
        if let path {
            setActiveOfflinePacks(paths: [path])
        } else {
            setActiveOfflinePacks(paths: [])
        }
    }

    func setActiveOfflinePacks(paths: [String]) {
        _ = execute("UPDATE offline_pack_installs SET is_active = 0")
        guard !paths.isEmpty else { return }

        let uniquePaths = Array(Set(paths)).sorted()
        let placeholders = Array(repeating: "?", count: uniquePaths.count).joined(separator: ",")
        let sql = "UPDATE offline_pack_installs SET is_active = 1, lifecycle_state = 'active' WHERE installed_path IN (\(placeholders))"
        let binds = uniquePaths.map { SQLiteBind.text($0) }
        _ = execute(sql, binds: binds)
    }

    func removeOfflinePackInstall(path: String) {
        _ = execute("DELETE FROM offline_pack_installs WHERE installed_path = ?", binds: [.text(path)])
    }

    func linkedServiceID(entityType: String, entityID: String) -> String? {
        let rows = query(
            """
            SELECT service_id FROM directory_entity_links
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            binds: [.text(entityType.lowercased()), .text(entityID)]
        )
        return rows.first?["service_id"] as? String
    }

    func hasConfiguredNode() -> Bool {
        !hubBaseURLString().isEmpty
    }

    func syncOutboxToNostr(limit: Int = 50) async -> OutboxSyncReport {
        var report = OutboxSyncReport()

        var rows = upgradeOutboxRowsWithSchnorr(loadDueOutboxRows(limit: limit))
        rows = deduplicatedRowsForTransportSync(rows)
        rows = locallyValidatedRows(rows, report: &report)
        guard !rows.isEmpty else { return report }
        report.attempted = rows.count

        for row in rows {
            do {
                let event = try makeNostrSemayEvent(from: row.envelope)
                let relays = semayRelayTargets(for: row.envelope)
                guard !relays.isEmpty else {
                    markOutboxRetry(
                        eventID: row.eventID,
                        eventType: row.envelope.eventType,
                        attempts: row.attempts + 1,
                        error: "nostr-no-relays"
                    )
                    report.retried += 1
                    continue
                }
                NostrRelayManager.shared.sendEvent(event, to: relays)
                markOutboxDelivered(eventID: row.eventID)
                report.delivered += 1
            } catch {
                let description = String(describing: error)
                markOutboxRetry(
                    eventID: row.eventID,
                    eventType: row.envelope.eventType,
                    attempts: row.attempts + 1,
                    error: "nostr-send-\(description)"
                )
                report.retried += 1
            }
        }

        refreshAll()
        return report
    }

    func syncFeedFromNostr(limit: Int = 200) async -> FeedSyncReport {
        var report = FeedSyncReport()
        let cursorKey = "nostr.feed.created_at.v1"
        let currentCursor = Int(loadSyncCursor(key: cursorKey) ?? "0") ?? 0
        let geohashes = semaySyncGeohashes()
        guard !geohashes.isEmpty else { return report }

        let relays = semayRelayTargets(forGeohashes: geohashes)
        guard !relays.isEmpty else { return report }

        let safeLimit = max(20, min(limit, 500))
        let sinceDate: Date? = currentCursor > 0
            ? Date(timeIntervalSince1970: TimeInterval(max(0, currentCursor - 120)))
            : nil
        let filter = NostrFilter.geohashNotes(Array(geohashes), since: sinceDate, limit: safeLimit)
        let events = await collectNostrEvents(filter: filter, relays: relays, timeoutSeconds: 8)
        report.fetched = events.count

        var maxCreatedAt = currentCursor
        var seenEnvelopeIDs: Set<String> = []

        for event in events {
            guard event.tags.contains(where: { tag in
                tag.count >= 2 && tag[0].lowercased() == "t" && tag[1].lowercased() == semayNostrTag
            }) else {
                report.skipped += 1
                continue
            }
            let decodeResult = decodeSemayEnvelope(from: event, source: "nostr")
            guard case .success(let envelope) = decodeResult else {
                report.skipped += 1
                if case .failure(let reason) = decodeResult {
                    SecureLogger.warning(
                        "Nostr feed envelope decode failed event=\(event.id): \(reason)",
                        category: .session
                    )
                }
                continue
            }
            guard seenEnvelopeIDs.insert(envelope.eventID).inserted else {
                report.skipped += 1
                SecureLogger.warning(
                    "Nostr feed envelope duplicate skipped id=\(envelope.eventID): ingest-duplicate",
                    category: .session
                )
                continue
            }
            maxCreatedAt = max(maxCreatedAt, envelope.createdAt)
            let applyResult = applyInboundEnvelopeWithReason(envelope)
            if applyResult.applied {
                report.applied += 1
            } else {
                report.skipped += 1
                let reason = applyResult.reason ?? "reason-unknown"
                SecureLogger.warning(
                    "Nostr feed envelope rejected id=\(envelope.eventID): \(reason)",
                    category: .session
                )
            }
        }

        saveSyncCursor(key: cursorKey, value: String(maxCreatedAt))
        refreshAll()
        return report
    }

    func syncOutboxToHub(limit: Int = 50, allowDiscovery: Bool = false) async -> OutboxSyncReport {
        var report = OutboxSyncReport()

        var endpointURL = makeHubBatchEndpointURL()
        if endpointURL == nil, allowDiscovery {
            do {
                _ = try await SemayNodeDiscoveryService.shared.resolveBaseURL(forceDiscovery: true)
            } catch {
                report.errorMessage = error.localizedDescription
                return report
            }
            endpointURL = makeHubBatchEndpointURL()
        }
        guard let endpointURL else {
            return report
        }

        var rows = upgradeOutboxRowsWithSchnorr(loadDueOutboxRows(limit: limit))
        rows = deduplicatedRowsForTransportSync(rows)
        rows = locallyValidatedRows(rows, report: &report)
        guard !rows.isEmpty else { return report }
        report.attempted = rows.count

        do {
            let batchRows = deduplicatedRowsForHubSync(rows)
            let response = try await postEnvelopeBatch(batchRows.map(\.envelope), to: endpointURL)

            let acceptedIDs = Set(response.accepted.compactMap { normalizeEventID($0.eventID) })
            let rejectedByID: [String: HubRejectInfo] = Dictionary(uniqueKeysWithValues: response.rejected.compactMap {
                guard let eventID = normalizeEventID($0.eventID) else { return nil }
                return (eventID, HubRejectInfo(from: $0))
            })
            for row in batchRows {
                guard let normalizedRowEventID = normalizeEventID(row.eventID) else {
                    markOutboxRetry(
                        eventID: row.eventID,
                        eventType: row.envelope.eventType,
                        attempts: row.attempts + 1,
                        error: "hub-invalid-event-id"
                    )
                    report.retried += 1
                    continue
                }

                if acceptedIDs.contains(normalizedRowEventID) {
                    markOutboxDelivered(eventID: row.eventID)
                    report.delivered += 1
                    continue
                }

                let eventID = row.eventID
                if let reject = rejectedByID[normalizedRowEventID] {
                    if isHubRejectionRetryable(type: row.envelope.eventType, errorCategory: reject.errorCategory, errorCode: reject.errorCode) {
                        markOutboxRetry(
                            eventID: eventID,
                            eventType: row.envelope.eventType,
                            attempts: row.attempts + 1,
                            error: "hub-rejected-\(reject.errorCode)"
                        )
                        report.retried += 1
                    } else {
                        markOutboxFailed(
                            eventID: eventID,
                            attempts: row.attempts + 1,
                            error: "hub-rejected-\(reject.errorCode)"
                        )
                        report.failed += 1
                    }
                    continue
                }

                markOutboxRetry(
                    eventID: row.eventID,
                    eventType: row.envelope.eventType,
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
                    eventType: row.envelope.eventType,
                    attempts: row.attempts + 1,
                    error: "hub-network-\(errorDescription)"
                )
                report.retried += 1
            }
        }

        refreshAll()
        return report
    }

    func syncFeedFromHub(limit: Int = 200, allowDiscovery: Bool = false) async -> FeedSyncReport {
        var report = FeedSyncReport()

        var baseURL = hubBaseURLString()
        if baseURL.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty, allowDiscovery {
            do {
                _ = try await SemayNodeDiscoveryService.shared.resolveBaseURL(forceDiscovery: true)
            } catch {
                report.errorMessage = error.localizedDescription
                return report
            }
            baseURL = hubBaseURLString()
        }

        guard let root = URL(string: baseURL), !baseURL.isEmpty else {
            return report
        }

        let cursorKey = "hub.feed.seq.v1"
        let currentCursor = Int(loadSyncCursor(key: cursorKey) ?? "0") ?? 0
        let safeLimit = max(1, min(limit, 500))

        var components = URLComponents(
            url: root
                .appendingPathComponent("chat")
                .appendingPathComponent("api")
                .appendingPathComponent("envelopes")
                .appendingPathComponent("feed"),
            resolvingAgainstBaseURL: false
        )
        components?.queryItems = [
            URLQueryItem(name: "cursor", value: String(currentCursor)),
            URLQueryItem(name: "limit", value: String(safeLimit)),
        ]
        guard let url = components?.url else {
            report.errorMessage = "invalid-feed-url"
            return report
        }

        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.timeoutInterval = 20

        let token = hubIngestToken().trimmingCharacters(in: .whitespacesAndNewlines)
        if !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                report.errorMessage = "hub-feed-bad-response"
                return report
            }
            guard (200...299).contains(http.statusCode) else {
                let body = String(data: data, encoding: .utf8) ?? ""
                report.errorMessage = "hub-feed-http-\(http.statusCode):\(body)"
                return report
            }

            let decoded = try JSONDecoder().decode(HubFeedResponse.self, from: data)
            report.fetched = decoded.events.count

            var maxSeq = currentCursor
            var seenEnvelopeIDs: Set<String> = []
            for item in decoded.events {
                maxSeq = max(maxSeq, item.seq)
                let envelope = item.envelope
                if let failure = envelope.validate() {
                    report.skipped += 1
                    SecureLogger.warning(
                        "Hub feed envelope invalid id=\(envelope.eventID): \(failure.category.rawValue):\(failure.reason)",
                        category: .session
                    )
                    continue
                }
                guard seenEnvelopeIDs.insert(envelope.eventID).inserted else {
                    report.skipped += 1
                    SecureLogger.warning(
                        "Hub feed envelope duplicate skipped id=\(envelope.eventID): ingest-duplicate",
                        category: .session
                    )
                    continue
                }
                let applyResult = applyInboundEnvelopeWithReason(envelope)
                if applyResult.applied {
                    report.applied += 1
                } else {
                    report.skipped += 1
                    let reason = applyResult.reason ?? "reason-unknown"
                    SecureLogger.warning(
                        "Hub feed envelope rejected id=\(envelope.eventID): \(reason)",
                        category: .session
                    )
                }
            }

            let nextCursor = max(maxSeq, decoded.nextCursor)
            saveSyncCursor(key: cursorKey, value: String(nextCursor))

            refreshAll()
            return report
        } catch {
            report.errorMessage = String(describing: error)
            return report
        }
    }

    func fetchHubMetrics(windowSeconds: Int = 24 * 60 * 60) async throws -> HubIngestMetrics {
        var endpointURL = makeHubMetricsEndpointURL(windowSeconds: windowSeconds)
        if endpointURL == nil {
            // Best-effort discovery for operators who hit "Load Node Metrics" first.
            _ = try? await SemayNodeDiscoveryService.shared.resolveBaseURL(forceDiscovery: true)
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
        refreshRoutes()
        refreshServiceDirectory()
        refreshBulletins()
        refreshBulletinModeration()
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
                   lightning_link, cashu_link,
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
            let lightningLink = (row["lightning_link"] as? String) ?? ""
            let cashuLink = (row["cashu_link"] as? String) ?? ""

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
                lightningLink: lightningLink,
                cashuLink: cashuLink,
                ownerPubkey: ownerPubkey,
                qrPayload: qrPayload,
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
        businesses = mapped
    }

    private func refreshBulletins() {
        let rows = query(
            """
            SELECT bulletin_id, title, category, body, phone, latitude, longitude, plus_code, e_address,
                   author_pubkey, created_at, updated_at
            FROM bulletins
            ORDER BY updated_at DESC
            """
        )

        let mapped = rows.compactMap { row -> BulletinPost? in
            guard let bulletinID = row["bulletin_id"] as? String,
                  let title = row["title"] as? String,
                  let categoryRaw = row["category"] as? String,
                  let body = row["body"] as? String,
                  let plusCode = row["plus_code"] as? String,
                  let eAddress = row["e_address"] as? String,
                  let authorPubkey = row["author_pubkey"] as? String,
                  let createdAt = row["created_at"] as? Int,
                  let updatedAt = row["updated_at"] as? Int else {
                return nil
            }

            let category = BulletinCategory(rawValue: categoryRaw) ?? .general
            let phone = (row["phone"] as? String) ?? ""
            let latitude = (row["latitude"] as? Double) ?? Double(row["latitude"] as? Int ?? 0)
            let longitude = (row["longitude"] as? Double) ?? Double(row["longitude"] as? Int ?? 0)

            return BulletinPost(
                bulletinID: bulletinID,
                title: title,
                category: category,
                body: body,
                phone: phone,
                latitude: latitude,
                longitude: longitude,
                plusCode: plusCode,
                eAddress: eAddress,
                authorPubkey: authorPubkey.lowercased(),
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
        bulletins = mapped
    }

    private func refreshBulletinModeration() {
        let rows = query("SELECT author_pubkey FROM bulletin_mutes")
        var muted: Set<String> = []
        for row in rows {
            if let author = row["author_pubkey"] as? String, !author.isEmpty {
                muted.insert(author.lowercased())
            }
        }
        mutedBulletinAuthors = muted
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
        let createdAt: Int
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
            let error: String?
            let errorCode: String?
            let errorCategory: String?

            enum CodingKeys: String, CodingKey {
                case eventID = "event_id"
                case error
                case errorCode = "error_code"
                case errorCategory = "error_category"
            }
        }

        let success: Bool
        let accepted: [Accepted]
        let rejected: [Rejected]
    }

    private struct HubRejectInfo {
        let eventID: String?
        let error: String
        let errorCode: String
        let errorCategory: String

        init(from item: HubBatchResponse.Rejected) {
            eventID = item.eventID
            error = item.error?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            errorCode = item.errorCode?.trimmingCharacters(in: .whitespacesAndNewlines)
                ?? item.error?.trimmingCharacters(in: .whitespacesAndNewlines)
                ?? "hub-rejected"
            errorCategory = item.errorCategory?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "protocol_invalid"
        }

        var isRetryable: Bool {
            switch errorCategory {
            case "transport_failed":
                return true
            default:
                return false
            }
        }
    }

    private struct HubFeedResponse: Decodable {
        struct Item: Decodable {
            let seq: Int
            let envelope: SemayEventEnvelope
        }

        let success: Bool
        let nextCursor: Int
        let events: [Item]

        enum CodingKeys: String, CodingKey {
            case success
            case nextCursor = "next_cursor"
            case events
        }
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
                share_scope TEXT NOT NULL DEFAULT 'personal',
                publish_state TEXT NOT NULL DEFAULT 'local_only',
                quality_score INTEGER NOT NULL DEFAULT 0,
                quality_flags TEXT NOT NULL DEFAULT '[]',
                review_version INTEGER NOT NULL DEFAULT 1,
                last_quality_checked_at INTEGER NOT NULL DEFAULT 0,
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
                lightning_link TEXT NOT NULL DEFAULT '',
                cashu_link TEXT NOT NULL DEFAULT '',
                owner_pubkey TEXT NOT NULL,
                qr_payload TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bulletins (
                bulletin_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                body TEXT NOT NULL,
                phone TEXT NOT NULL DEFAULT '',
                latitude REAL NOT NULL DEFAULT 0,
                longitude REAL NOT NULL DEFAULT 0,
                plus_code TEXT NOT NULL DEFAULT '',
                e_address TEXT NOT NULL,
                author_pubkey TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bulletin_reports (
                bulletin_id TEXT NOT NULL,
                reporter_pubkey TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (bulletin_id, reporter_pubkey)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bulletin_mutes (
                author_pubkey TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
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
            """,
            """
            CREATE TABLE IF NOT EXISTS curated_routes (
                route_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                summary TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                from_label TEXT NOT NULL DEFAULT '',
                to_label TEXT NOT NULL DEFAULT '',
                transport_type TEXT NOT NULL DEFAULT 'unknown',
                waypoints_json TEXT NOT NULL DEFAULT '[]',
                reliability_score INTEGER NOT NULL DEFAULT 0,
                trust_score INTEGER NOT NULL DEFAULT 0,
                source_trust_tier INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                author_pubkey TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS route_endorsements (
                route_id TEXT NOT NULL,
                actor_pubkey TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                PRIMARY KEY (route_id, actor_pubkey),
                FOREIGN KEY (route_id) REFERENCES curated_routes(route_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS route_reports (
                route_id TEXT NOT NULL,
                reporter_pubkey TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                PRIMARY KEY (route_id, reporter_pubkey),
                FOREIGN KEY (route_id) REFERENCES curated_routes(route_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS service_directory_entries (
                service_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                service_type TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                country TEXT NOT NULL DEFAULT '',
                latitude REAL NOT NULL DEFAULT 0,
                longitude REAL NOT NULL DEFAULT 0,
                plus_code TEXT NOT NULL DEFAULT '',
                e_address TEXT NOT NULL DEFAULT '',
                address_label TEXT NOT NULL DEFAULT '',
                locality TEXT NOT NULL DEFAULT '',
                admin_area TEXT NOT NULL DEFAULT '',
                country_code TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                website TEXT NOT NULL DEFAULT '',
                emergency_contact TEXT NOT NULL DEFAULT '',
                urgency TEXT NOT NULL DEFAULT 'medium',
                verified INTEGER NOT NULL DEFAULT 0,
                trust_score INTEGER NOT NULL DEFAULT 0,
                source_trust_tier INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                tags_json TEXT NOT NULL DEFAULT '[]',
                primary_photo_id TEXT NOT NULL DEFAULT '',
                photo_count INTEGER NOT NULL DEFAULT 0,
                share_scope TEXT NOT NULL DEFAULT 'personal',
                publish_state TEXT NOT NULL DEFAULT 'local_only',
                quality_score INTEGER NOT NULL DEFAULT 0,
                quality_flags TEXT NOT NULL DEFAULT '[]',
                review_version INTEGER NOT NULL DEFAULT 1,
                last_quality_checked_at INTEGER NOT NULL DEFAULT 0,
                author_pubkey TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS service_endorsements (
                service_id TEXT NOT NULL,
                actor_pubkey TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                PRIMARY KEY (service_id, actor_pubkey),
                FOREIGN KEY (service_id) REFERENCES service_directory_entries(service_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS service_reports (
                service_id TEXT NOT NULL,
                reporter_pubkey TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                PRIMARY KEY (service_id, reporter_pubkey),
                FOREIGN KEY (service_id) REFERENCES service_directory_entries(service_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS offline_pack_installs (
                installed_path TEXT PRIMARY KEY,
                pack_id TEXT NOT NULL DEFAULT '',
                region_code TEXT NOT NULL DEFAULT '',
                pack_version TEXT NOT NULL DEFAULT '',
                tile_format TEXT NOT NULL DEFAULT '',
                sha256 TEXT NOT NULL DEFAULT '',
                signature TEXT NOT NULL DEFAULT '',
                sig_alg TEXT NOT NULL DEFAULT '',
                min_zoom INTEGER NOT NULL DEFAULT 0,
                max_zoom INTEGER NOT NULL DEFAULT 0,
                bounds TEXT NOT NULL DEFAULT '',
                size_bytes INTEGER NOT NULL DEFAULT 0,
                installed_at INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 0,
                lifecycle_state TEXT NOT NULL DEFAULT 'discovered',
                depends_on TEXT NOT NULL DEFAULT '',
                style_url TEXT NOT NULL DEFAULT '',
                lang TEXT NOT NULL DEFAULT ''
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS directory_entity_links (
                service_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (entity_type, entity_id),
                FOREIGN KEY (service_id) REFERENCES service_directory_entries(service_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS service_photo_refs (
                photo_id TEXT PRIMARY KEY,
                service_id TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                mime_type TEXT NOT NULL DEFAULT 'image/jpeg',
                width INTEGER NOT NULL DEFAULT 0,
                height INTEGER NOT NULL DEFAULT 0,
                bytes_full INTEGER NOT NULL DEFAULT 0,
                bytes_thumb INTEGER NOT NULL DEFAULT 0,
                local_full_path TEXT NOT NULL DEFAULT '',
                local_thumb_path TEXT NOT NULL DEFAULT '',
                taken_at INTEGER,
                exif_latitude REAL,
                exif_longitude REAL,
                geo_source TEXT NOT NULL DEFAULT 'none',
                is_primary INTEGER NOT NULL DEFAULT 0,
                remote_url TEXT NOT NULL DEFAULT '',
                last_accessed_at INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY (service_id) REFERENCES service_directory_entries(service_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS contribution_publication_queue (
                queue_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                author_pubkey TEXT NOT NULL,
                share_scope TEXT NOT NULL DEFAULT 'personal',
                publish_state TEXT NOT NULL DEFAULT 'local_only',
                quality_score INTEGER NOT NULL DEFAULT 0,
                quality_flags TEXT NOT NULL DEFAULT '[]',
                review_version INTEGER NOT NULL DEFAULT 1,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_service_photo_refs_service ON service_photo_refs(service_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_service_photo_refs_lru ON service_photo_refs(last_accessed_at)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_contribution_publication_queue_entity ON contribution_publication_queue(entity_type, entity_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_contribution_publication_queue_author_time ON contribution_publication_queue(author_pubkey, created_at)
            """
        ]

        for sql in statements {
            _ = execute(sql)
        }

        // Column-level migrations for existing installs.
        ensureColumn(table: "pins", column: "plus_code", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "pins", column: "phone", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "pins", column: "share_scope", definition: "TEXT NOT NULL DEFAULT 'personal'")
        ensureColumn(table: "pins", column: "publish_state", definition: "TEXT NOT NULL DEFAULT 'local_only'")
        ensureColumn(table: "pins", column: "quality_score", definition: "INTEGER NOT NULL DEFAULT 0")
        ensureColumn(table: "pins", column: "quality_flags", definition: "TEXT NOT NULL DEFAULT '[]'")
        ensureColumn(table: "pins", column: "review_version", definition: "INTEGER NOT NULL DEFAULT 1")
        ensureColumn(table: "pins", column: "last_quality_checked_at", definition: "INTEGER NOT NULL DEFAULT 0")
        ensureColumn(table: "business_profiles", column: "latitude", definition: "REAL NOT NULL DEFAULT 0")
        ensureColumn(table: "business_profiles", column: "longitude", definition: "REAL NOT NULL DEFAULT 0")
        ensureColumn(table: "business_profiles", column: "plus_code", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "business_profiles", column: "phone", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "business_profiles", column: "lightning_link", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "business_profiles", column: "cashu_link", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "bulletins", column: "phone", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "bulletins", column: "latitude", definition: "REAL NOT NULL DEFAULT 0")
        ensureColumn(table: "bulletins", column: "longitude", definition: "REAL NOT NULL DEFAULT 0")
        ensureColumn(table: "bulletins", column: "plus_code", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "service_directory_entries", column: "address_label", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "service_directory_entries", column: "locality", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "service_directory_entries", column: "admin_area", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "service_directory_entries", column: "country_code", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "service_directory_entries", column: "primary_photo_id", definition: "TEXT NOT NULL DEFAULT ''")
        ensureColumn(table: "service_directory_entries", column: "photo_count", definition: "INTEGER NOT NULL DEFAULT 0")
        ensureColumn(table: "service_directory_entries", column: "share_scope", definition: "TEXT NOT NULL DEFAULT 'personal'")
        ensureColumn(table: "service_directory_entries", column: "publish_state", definition: "TEXT NOT NULL DEFAULT 'local_only'")
        ensureColumn(table: "service_directory_entries", column: "quality_score", definition: "INTEGER NOT NULL DEFAULT 0")
        ensureColumn(table: "service_directory_entries", column: "quality_flags", definition: "TEXT NOT NULL DEFAULT '[]'")
        ensureColumn(table: "service_directory_entries", column: "review_version", definition: "INTEGER NOT NULL DEFAULT 1")
        ensureColumn(table: "service_directory_entries", column: "last_quality_checked_at", definition: "INTEGER NOT NULL DEFAULT 0")

        backfillAddressesIfNeeded()
        backfillDirectoryEntityLinksIfNeeded()
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

        let bulletinRows = query("SELECT bulletin_id, latitude, longitude, plus_code, e_address FROM bulletins")
        for row in bulletinRows {
            guard let bulletinID = row["bulletin_id"] as? String,
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
                "UPDATE bulletins SET plus_code = ?, e_address = ?, updated_at = ? WHERE bulletin_id = ?",
                binds: [
                    .text(address.plusCode),
                    .text(address.eAddress),
                    .int(Int(Date().timeIntervalSince1970)),
                    .text(bulletinID)
                ]
            )
        }

        let serviceRows = query(
            """
            SELECT service_id, name, latitude, longitude, plus_code, e_address, address_label, locality, admin_area, country_code
            FROM service_directory_entries
            """
        )
        for row in serviceRows {
            guard let serviceID = row["service_id"] as? String,
                  let serviceName = row["name"] as? String else { continue }
            let latitude = (row["latitude"] as? Double) ?? Double(row["latitude"] as? Int ?? 0)
            let longitude = (row["longitude"] as? Double) ?? Double(row["longitude"] as? Int ?? 0)
            let currentPlus = (row["plus_code"] as? String) ?? ""
            let currentEAddress = (row["e_address"] as? String) ?? ""
            let currentAddressLabel = (row["address_label"] as? String) ?? ""
            let currentLocality = (row["locality"] as? String) ?? ""
            let currentAdminArea = (row["admin_area"] as? String) ?? ""
            let currentCountryCode = (row["country_code"] as? String) ?? ""

            let fallbackAddress = SemayAddress.eAddress(latitude: latitude, longitude: longitude)
            let plusCode = currentPlus.isEmpty ? fallbackAddress.plusCode : currentPlus
            let eAddress = currentEAddress.isEmpty ? fallbackAddress.eAddress : currentEAddress
            let display = SemayAddressDisplayBuilder.build(
                nameHint: serviceName,
                latitude: latitude,
                longitude: longitude,
                plusCode: plusCode,
                eAddress: eAddress
            )
            let addressLabel = currentAddressLabel.isEmpty ? display.addressLabel : currentAddressLabel
            let locality = currentLocality.isEmpty ? display.locality : currentLocality
            let adminArea = currentAdminArea.isEmpty ? display.adminArea : currentAdminArea
            let countryCode = currentCountryCode.isEmpty ? display.countryCode : currentCountryCode

            if currentPlus == plusCode,
               currentEAddress == eAddress,
               currentAddressLabel == addressLabel,
               currentLocality == locality,
               currentAdminArea == adminArea,
               currentCountryCode == countryCode {
                continue
            }

            _ = execute(
                """
                UPDATE service_directory_entries
                SET plus_code = ?, e_address = ?, address_label = ?, locality = ?, admin_area = ?, country_code = ?, updated_at = ?
                WHERE service_id = ?
                """,
                binds: [
                    .text(plusCode),
                    .text(eAddress),
                    .text(addressLabel),
                    .text(locality),
                    .text(adminArea),
                    .text(countryCode),
                    .int(Int(Date().timeIntervalSince1970)),
                    .text(serviceID)
                ]
            )
        }
    }

    private func backfillDirectoryEntityLinksIfNeeded() {
        let now = Int(Date().timeIntervalSince1970)
        let existingServiceIDs = Set(
            query("SELECT service_id FROM service_directory_entries").compactMap { $0["service_id"] as? String }
        )

        let businessRows = query(
            """
            SELECT business_id, name, category, details, latitude, longitude, plus_code, e_address, phone, owner_pubkey, created_at, updated_at
            FROM business_profiles
            """
        )
        for row in businessRows {
            guard let businessID = row["business_id"] as? String,
                  let name = row["name"] as? String,
                  let category = row["category"] as? String,
                  let details = row["details"] as? String,
                  let plusCode = row["plus_code"] as? String,
                  let eAddress = row["e_address"] as? String,
                  let phone = row["phone"] as? String,
                  let ownerPubkey = row["owner_pubkey"] as? String else {
                continue
            }
            if linkedServiceID(entityType: "business", entityID: businessID) != nil {
                continue
            }

            let latitude = (row["latitude"] as? Double) ?? Double(row["latitude"] as? Int ?? 0)
            let longitude = (row["longitude"] as? Double) ?? Double(row["longitude"] as? Int ?? 0)
            let createdAt = (row["created_at"] as? Int) ?? now
            let updatedAt = (row["updated_at"] as? Int) ?? now
            let serviceID = UUID().uuidString.lowercased()

            let display = SemayAddressDisplayBuilder.build(
                nameHint: name,
                latitude: latitude,
                longitude: longitude,
                plusCode: plusCode,
                eAddress: eAddress
            )

            _ = execute(
                """
                INSERT OR IGNORE INTO service_directory_entries (
                    service_id, name, service_type, category, details, city, country,
                    latitude, longitude, plus_code, e_address, address_label, locality, admin_area, country_code,
                    phone, website, emergency_contact, urgency, verified, trust_score, source_trust_tier,
                    status, tags_json, author_pubkey, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', 'medium', 0, 45, 0, 'active', ?, ?, ?, ?)
                """,
                binds: [
                    .text(serviceID), .text(name), .text("business"), .text(category), .text(details),
                    .text(display.locality), .text(display.countryName),
                    .double(latitude), .double(longitude), .text(plusCode), .text(eAddress),
                    .text(display.addressLabel), .text(display.locality), .text(display.adminArea), .text(display.countryCode),
                    .text(phone),
                    .text("[\"business\",\"yellow-pages\"]"),
                    .text(ownerPubkey.lowercased()),
                    .int(createdAt), .int(updatedAt)
                ]
            )

            if !existingServiceIDs.contains(serviceID) {
                ensureDirectoryEntityLink(serviceID: serviceID, entityType: "business", entityID: businessID, createdAt: updatedAt)
            }
        }

        let pinRows = query(
            """
            SELECT pin_id, name, type, details, latitude, longitude, plus_code, e_address, phone, author_pubkey, created_at, updated_at
            FROM pins
            """
        )
        for row in pinRows {
            guard let pinID = row["pin_id"] as? String,
                  let name = row["name"] as? String,
                  let type = row["type"] as? String,
                  let details = row["details"] as? String,
                  let plusCode = row["plus_code"] as? String,
                  let eAddress = row["e_address"] as? String,
                  let phone = row["phone"] as? String,
                  let authorPubkey = row["author_pubkey"] as? String else {
                continue
            }
            if linkedServiceID(entityType: "pin", entityID: pinID) != nil {
                continue
            }
            guard isHighValuePinType(type) else { continue }

            let latitude = (row["latitude"] as? Double) ?? Double(row["latitude"] as? Int ?? 0)
            let longitude = (row["longitude"] as? Double) ?? Double(row["longitude"] as? Int ?? 0)
            let createdAt = (row["created_at"] as? Int) ?? now
            let updatedAt = (row["updated_at"] as? Int) ?? now
            let serviceID = UUID().uuidString.lowercased()

            let display = SemayAddressDisplayBuilder.build(
                nameHint: name,
                latitude: latitude,
                longitude: longitude,
                plusCode: plusCode,
                eAddress: eAddress
            )

            _ = execute(
                """
                INSERT OR IGNORE INTO service_directory_entries (
                    service_id, name, service_type, category, details, city, country,
                    latitude, longitude, plus_code, e_address, address_label, locality, admin_area, country_code,
                    phone, website, emergency_contact, urgency, verified, trust_score, source_trust_tier,
                    status, tags_json, author_pubkey, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', 'medium', 0, 35, 0, 'active', ?, ?, ?, ?)
                """,
                binds: [
                    .text(serviceID), .text(name), .text(type.lowercased()), .text("place"), .text(details),
                    .text(display.locality), .text(display.countryName),
                    .double(latitude), .double(longitude), .text(plusCode), .text(eAddress),
                    .text(display.addressLabel), .text(display.locality), .text(display.adminArea), .text(display.countryCode),
                    .text(phone),
                    .text("[\"place\",\"yellow-pages\"]"),
                    .text(authorPubkey.lowercased()),
                    .int(createdAt), .int(updatedAt)
                ]
            )

            if !existingServiceIDs.contains(serviceID) {
                ensureDirectoryEntityLink(serviceID: serviceID, entityType: "pin", entityID: pinID, createdAt: updatedAt)
            }
        }
    }

    private func ensureDirectoryEntityLink(serviceID: String, entityType: String, entityID: String, createdAt: Int) {
        _ = execute(
            """
            INSERT OR REPLACE INTO directory_entity_links (
                service_id, entity_type, entity_id, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            binds: [.text(serviceID), .text(entityType.lowercased()), .text(entityID), .int(createdAt)]
        )
    }

    private func isHighValuePinType(_ rawType: String) -> Bool {
        let normalized = rawType.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !normalized.isEmpty else { return false }
        let highValue: [String] = ["store", "clinic", "pharmacy", "transport", "school"]
        return highValue.contains { normalized.contains($0) }
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
        // Hostile-default privacy: only sync public directory events by default.
        // Promise lifecycle events are peer-to-peer and not broadcast to shared transports.
        guard shouldSyncOutboxEvent(type) else { return }

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

    private func shouldSyncOutboxEvent(_ type: SemayEventEnvelope.EventType) -> Bool {
        switch type {
        case .pinCreate, .pinUpdate, .pinApproval, .businessRegister, .businessUpdate, .bulletinPost,
            .routeCurated, .routeCuratedCreate, .routeCuratedUpdate, .routeCuratedRetract, .routeCuratedReport, .routeCuratedEndorse,
            .serviceDirectoryCreate, .serviceDirectoryUpdate, .serviceDirectoryRetract, .serviceDirectoryEndorse, .serviceDirectoryReport:
            return true
        case .promiseCreate, .promiseAccept, .promiseReject, .promiseSettle, .chatMessage, .chatAck:
            return false
        }
    }

    private func normalizeLightningLink(_ raw: String) -> String {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return "" }
        if let url = URL(string: trimmed),
           let scheme = url.scheme,
           !scheme.isEmpty {
            return trimmed
        }
        // Accept bare invoices/lnurl/lightning-addresses by prefixing the common scheme.
        return "lightning:\(trimmed)"
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

    private func upgradeOutboxRowsWithSchnorr(_ rows: [OutboxEnvelopeRow]) -> [OutboxEnvelopeRow] {
        guard let identity = try? idBridge.getCurrentNostrIdentity() else {
            return rows
        }

        var upgraded: [OutboxEnvelopeRow] = []
        upgraded.reserveCapacity(rows.count)

        for row in rows {
            if row.envelope.signature.count == 64,
               let upgradedEnvelope = try? SemayEventEnvelope.signed(
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
                        attempts: row.attempts,
                        createdAt: row.createdAt
                    )
                )
            } else {
                upgraded.append(row)
            }
        }
        return upgraded
    }

    private func deduplicatedRowsForHubSync(_ rows: [OutboxEnvelopeRow]) -> [OutboxEnvelopeRow] {
        return deduplicatedRowsForTransportSync(rows)
    }

    private func isRouteOrServiceLifecycleEvent(_ type: SemayEventEnvelope.EventType) -> Bool {
        return SemayEventEnvelope.EventType.routeLifecycle.contains(type)
            || SemayEventEnvelope.EventType.serviceDirectoryLifecycle.contains(type)
    }

    private func outboxAttemptsLimit(for type: SemayEventEnvelope.EventType) -> Int {
        if isRouteOrServiceLifecycleEvent(type) {
            return routeAndServiceMaxOutboxAttempts
        }
        return maxOutboxAttempts
    }

    private func outboxBackoffSeconds(for type: SemayEventEnvelope.EventType, attempts: Int) -> Int {
        let bounded = max(0, min(attempts, 10))
        if isRouteOrServiceLifecycleEvent(type) {
            return min(600, 20 * (1 << bounded))
        }
        return min(300, Int(pow(2.0, Double(bounded))))
    }

    private func outboxDedupKey(for row: OutboxEnvelopeRow) -> String {
        let envelope = row.envelope
        let entityID = envelope.entityID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let actor = (envelope.payload["actor_pubkey"] ?? envelope.authorPubkey)
            .trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

        switch envelope.eventType {
        case .routeCuratedCreate, .routeCuratedUpdate, .routeCuratedRetract,
             .serviceDirectoryCreate, .serviceDirectoryUpdate, .serviceDirectoryRetract:
            return "\(envelope.eventType.rawValue)|\(entityID)"
        case .routeCuratedReport, .serviceDirectoryReport,
             .routeCuratedEndorse, .serviceDirectoryEndorse:
            return "\(envelope.eventType.rawValue)|\(entityID)|\(actor.isEmpty ? "unknown" : actor)"
        default:
            return row.eventID
        }
    }

    private func deduplicatedRowsForTransportSync(_ rows: [OutboxEnvelopeRow]) -> [OutboxEnvelopeRow] {
        guard !rows.isEmpty else { return [] }
        var output: [OutboxEnvelopeRow] = []
        var latestIndexByID: [String: Int] = [:]

        for row in rows {
            guard normalizeEventID(row.eventID) != nil else { continue }
            let key = outboxDedupKey(for: row)
            if let existingIndex = latestIndexByID[key] {
                if row.createdAt > output[existingIndex].createdAt {
                    output[existingIndex] = row
                }
                continue
            }
            latestIndexByID[key] = output.count
            output.append(row)
        }
        return output
    }

    private func normalizeEventID(_ eventID: String) -> String? {
        let normalized = eventID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !normalized.isEmpty else { return nil }
        return normalized
    }

    private func isHubRejectionRetryable(
        type: SemayEventEnvelope.EventType,
        errorCategory: String,
        errorCode: String
    ) -> Bool {
        let category = errorCategory.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
        let code = errorCode.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)

        switch category {
        case "transport_failed":
            return true
        case "protocol_invalid", "policy_rejected":
            return false
        default:
            break
        }

        // Conservative retry list for transient node-side conditions.
        if code == "node-unavailable"
            || code == "node-busy"
            || code == "queue-full"
            || code == "service-unavailable"
            || code == "rate-limited"
            || code.contains("timeout")
            || code.contains("retry")
            || code.contains("temporary") {
            return true
        }

        // For sensitive trust workflows, avoid retrying explicit policy signals.
        if type == .routeCuratedCreate || type == .routeCuratedUpdate || type == .routeCuratedRetract
            || type == .serviceDirectoryCreate || type == .serviceDirectoryUpdate || type == .serviceDirectoryRetract {
            return false
        }

        return false
    }

    private func locallyValidatedRows(_ rows: [OutboxEnvelopeRow], report: inout OutboxSyncReport) -> [OutboxEnvelopeRow] {
        var validRows: [OutboxEnvelopeRow] = []
        validRows.reserveCapacity(rows.count)
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
        return validRows
    }

    private func makeNostrSemayEvent(from envelope: SemayEventEnvelope) throws -> NostrEvent {
        guard let identity = try idBridge.getCurrentNostrIdentity() else {
            throw NSError(
                domain: "semay.nostr",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "nostr-identity-unavailable"]
            )
        }

        let data = try JSONEncoder().encode(envelope)
        let encoded = Base64URL.encode(data)
        let content = "\(semayNostrContentPrefix)\(encoded)"

        var tags: [[String]] = [
            ["t", semayNostrTag],
            ["event_id", envelope.eventID],
            ["event_type", envelope.eventType.rawValue],
        ]
        if let geohash = semayEventGeohash(envelope) {
            tags.append(["g", geohash])
        }

        let event = NostrEvent(
            pubkey: identity.publicKeyHex.lowercased(),
            createdAt: Date(timeIntervalSince1970: TimeInterval(max(0, envelope.createdAt))),
            kind: .textNote,
            tags: tags,
            content: content
        )
        return try event.sign(with: identity.schnorrSigningKey())
    }

    private func decodeSemayEnvelope(
        from event: NostrEvent,
        source: String
    ) -> SemayEnvelopeDecodeResult {
        let sourceLabel = "source=\(source)"

        guard event.kind == NostrProtocol.EventKind.textNote.rawValue else {
            SecureLogger.warning(
                "Semay envelope decode rejected (\(sourceLabel)): protocol-invalid:event-kind",
                category: .session
            )
            return .failure(reason: "protocol-invalid:event-kind")
        }

        guard event.content.hasPrefix(semayNostrContentPrefix) else {
            SecureLogger.warning(
                "Semay envelope decode rejected event=\(event.id) (\(sourceLabel)): protocol-invalid:missing-prefix",
                category: .session
            )
            return .failure(reason: "protocol-invalid:missing-prefix")
        }

        guard event.isValidSignature() else {
            SecureLogger.warning(
                "Semay envelope decode rejected event=\(event.id) (\(sourceLabel)): protocol-invalid:nostr-signature-invalid",
                category: .session
            )
            return .failure(reason: "protocol-invalid:nostr-signature-invalid")
        }

        let encoded = String(event.content.dropFirst(semayNostrContentPrefix.count))
        guard let data = Base64URL.decode(encoded) else {
            SecureLogger.warning(
                "Semay envelope decode rejected event=\(event.id) (\(sourceLabel)): protocol-invalid:base64-decode",
                category: .session
            )
            return .failure(reason: "protocol-invalid:base64-decode")
        }

        guard let envelope = try? JSONDecoder().decode(SemayEventEnvelope.self, from: data) else {
            SecureLogger.warning(
                "Semay envelope decode rejected event=\(event.id) (\(sourceLabel)): protocol-invalid:bad-envelope-json",
                category: .session
            )
            return .failure(reason: "protocol-invalid:bad-envelope-json")
        }

        guard envelope.authorPubkey.lowercased() == event.pubkey.lowercased() else {
            SecureLogger.warning(
                "Semay envelope decode rejected event=\(envelope.eventID) (\(sourceLabel)): protocol-invalid:pubkey-mismatch",
                category: .session
            )
            return .failure(reason: "protocol-invalid:pubkey-mismatch")
        }

        if let failure = envelope.validate() {
            SecureLogger.warning(
                "Semay envelope decode rejected event=\(envelope.eventID) (\(sourceLabel)): \(failure.category.rawValue):\(failure.reason)",
                category: .session
            )
            return .failure(reason: "\(failure.category.rawValue):\(failure.reason)")
        }

        return .success(envelope)
    }

    private func decodeSemayEnvelope(from event: NostrEvent) -> SemayEventEnvelope? {
        guard case .success(let envelope) = decodeSemayEnvelope(from: event, source: "legacy") else {
            return nil
        }
        return envelope
    }

#if DEBUG
    func debugDecodeSemayEnvelope(
        from event: NostrEvent,
        source: String = "test"
    ) -> (envelope: SemayEventEnvelope?, reason: String?) {
        let result = decodeSemayEnvelope(from: event, source: source)
        switch result {
        case let .success(envelope):
            return (envelope: envelope, reason: nil)
        case let .failure(reason):
            return (envelope: nil, reason: reason)
        }
    }

    func debugApplyInboundEnvelopeWithReason(
        _ envelope: SemayEventEnvelope
    ) -> (applied: Bool, reason: String?) {
        let result = applyInboundEnvelopeWithReason(envelope)
        return (applied: result.applied, reason: result.reason)
    }

    func debugResetForTests() {
        wipeLocalDatabaseForRestore()
    }
#endif

    private func collectNostrEvents(
        filter: NostrFilter,
        relays: [String],
        timeoutSeconds: TimeInterval
    ) async -> [NostrEvent] {
        guard !relays.isEmpty else { return [] }
        let subscriptionID = "semay-feed-\(UUID().uuidString.prefix(8))"

        return await withCheckedContinuation { continuation in
            var seenEventIDs: Set<String> = []
            var events: [NostrEvent] = []
            var finished = false

            @MainActor
            func finish() {
                guard !finished else { return }
                finished = true
                NostrRelayManager.shared.unsubscribe(id: subscriptionID)
                continuation.resume(returning: events)
            }

            NostrRelayManager.shared.subscribe(
                filter: filter,
                id: subscriptionID,
                relayUrls: relays,
                handler: { event in
                    guard seenEventIDs.insert(event.id).inserted else { return }
                    events.append(event)
                },
                onEOSE: {
                    Task { @MainActor in
                        finish()
                    }
                }
            )

            Task { @MainActor in
                let nanos = UInt64((max(1, timeoutSeconds) * 1_000_000_000).rounded())
                try? await Task.sleep(nanoseconds: nanos)
                finish()
            }
        }
    }

    private func semayRelayTargets(for envelope: SemayEventEnvelope) -> [String] {
        if let geohash = semayEventGeohash(envelope) {
            return semayRelayTargets(forGeohashes: Set([geohash]))
        }
        return semayRelayTargets(forGeohashes: semayAnchorGeohashes())
    }

    private func semayRelayTargets(forGeohashes geohashes: Set<String>) -> [String] {
        var urls: [String] = []
        var seen: Set<String> = []
        let count = max(3, TransportConfig.nostrGeoRelayCount)

        for geohash in geohashes {
            let relays = GeoRelayDirectory.shared.closestRelays(toGeohash: geohash, count: count)
            for relay in relays {
                if seen.insert(relay).inserted {
                    urls.append(relay)
                }
            }
        }
        return urls
    }

    private func semaySyncGeohashes() -> Set<String> {
        var geohashes = semayAnchorGeohashes()
        for pin in pins.prefix(32) {
            geohashes.insert(Geohash.encode(latitude: pin.latitude, longitude: pin.longitude, precision: 5).lowercased())
        }
        for business in businesses.prefix(32) {
            geohashes.insert(Geohash.encode(latitude: business.latitude, longitude: business.longitude, precision: 5).lowercased())
        }
        return geohashes
    }

    private func semayAnchorGeohashes() -> Set<String> {
        let anchors: [(Double, Double)] = [
            (15.3229, 38.9251), // Asmara
            (8.9806, 38.7578),  // Addis Ababa
        ]
        return Set(anchors.map { Geohash.encode(latitude: $0.0, longitude: $0.1, precision: 5).lowercased() })
    }

    private func semayEventGeohash(_ envelope: SemayEventEnvelope) -> String? {
        if let geohash = geohashFromPayload(envelope.payload) {
            return geohash
        }

        if envelope.eventType == .pinApproval,
           let pinID = envelope.payload["pin_id"] {
            let rows = query(
                "SELECT latitude, longitude FROM pins WHERE pin_id = ? LIMIT 1",
                binds: [.text(pinID)]
            )
            if let row = rows.first {
                let lat = (row["latitude"] as? Double) ?? Double(row["latitude"] as? Int ?? 0)
                let lon = (row["longitude"] as? Double) ?? Double(row["longitude"] as? Int ?? 0)
                if abs(lat) <= 90, abs(lon) <= 180, !(lat == 0 && lon == 0) {
                    return Geohash.encode(latitude: lat, longitude: lon, precision: 5).lowercased()
                }
            }
        }

        return nil
    }

    private func geohashFromPayload(_ payload: [String: String]) -> String? {
        if let latitude = Double(payload["latitude"] ?? ""),
           let longitude = Double(payload["longitude"] ?? ""),
           abs(latitude) <= 90,
           abs(longitude) <= 180,
           !(latitude == 0 && longitude == 0) {
            return Geohash.encode(latitude: latitude, longitude: longitude, precision: 5).lowercased()
        }

        let plusCode = payload["plus_code"]?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !plusCode.isEmpty, let area = OpenLocationCode.decode(plusCode) {
            return Geohash.encode(latitude: area.centerLatitude, longitude: area.centerLongitude, precision: 5).lowercased()
        }

        return nil
    }

    private func loadDueOutboxRows(limit: Int) -> [OutboxEnvelopeRow] {
        let safeLimit = max(1, min(limit, 200))
        let now = Int(Date().timeIntervalSince1970)
        let rows = query(
            """
            SELECT event_id, envelope_json, attempts, created_at
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
            let createdAt = (row["created_at"] as? Int) ?? now
            guard let data = envelopeJSON.data(using: .utf8),
                  let envelope = try? JSONDecoder().decode(SemayEventEnvelope.self, from: data) else {
                markOutboxFailed(eventID: eventID, attempts: attempts + 1, error: "invalid-stored-envelope-json")
                continue
            }
            if attempts >= outboxAttemptsLimit(for: envelope.eventType) {
                markOutboxFailed(
                    eventID: eventID,
                    attempts: attempts,
                    error: "max-attempts-exceeded"
                )
                continue
            }
            output.append(
                OutboxEnvelopeRow(
                    eventID: eventID,
                    envelope: envelope,
                    attempts: attempts,
                    createdAt: createdAt
                )
            )
        }
        return output
    }

    private struct ServiceContributionQualityResult {
        let score: Int
        let reasons: [String]

        var reasonsJSON: String {
            (try? String(data: JSONEncoder().encode(reasons), encoding: .utf8)) ?? "[]"
        }
    }

    private func evaluateServiceContributionQuality(_ entry: SemayServiceDirectoryEntry) -> ServiceContributionQualityResult {
        var flags: [String] = []

        if entry.name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            || entry.serviceType.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            flags.append("missing_required_fields")
        }

        if !isLatitudeLongitudeWithinBounds(latitude: entry.latitude, longitude: entry.longitude) {
            flags.append("invalid_coordinates")
        }

        let duplicateCandidates = query(
            """
            SELECT service_id, latitude, longitude
            FROM service_directory_entries
            WHERE service_id != ?
              AND status = 'active'
              AND lower(name) = lower(?)
              AND lower(category) = lower(?)
            LIMIT 12
            """,
            binds: [.text(entry.serviceID), .text(entry.name), .text(entry.category)]
        )
        if duplicateCandidates.contains(where: { row in
            let lat = (row["latitude"] as? Double) ?? Double(row["latitude"] as? Int ?? 0)
            let lon = (row["longitude"] as? Double) ?? Double(row["longitude"] as? Int ?? 0)
            return coordinateDistanceMeters(
                lat1: entry.latitude,
                lon1: entry.longitude,
                lat2: lat,
                lon2: lon
            ) <= 120
        }) {
            flags.append("possible_duplicate")
        }

        let refs = servicePhotoRefs(serviceID: entry.serviceID)
        if refs.count > Self.servicePhotoMaxRefs {
            flags.append("photo_limit_exceeded")
        }
        if refs.contains(where: { $0.width > 0 && $0.height > 0 && ($0.width < 320 || $0.height < 240) }) {
            flags.append("photo_resolution_low")
        }
        if refs.contains(where: { $0.bytesFull > 6 * 1024 * 1024 || $0.bytesThumb > 1024 * 1024 }) {
            flags.append("photo_byte_cap_exceeded")
        }
        var seenHashes = Set<String>()
        let hasDuplicateHash = refs.contains { ref in
            let hash = ref.sha256.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            guard !hash.isEmpty else { return false }
            if seenHashes.contains(hash) {
                return true
            }
            seenHashes.insert(hash)
            return false
        }
        if hasDuplicateHash {
            flags.append("photo_duplicate_hash")
        }

        if entry.trustScore < 25 {
            flags.append("author_trust_low")
        }

        let now = Int(Date().timeIntervalSince1970)
        let dayAgo = now - 86_400
        let recentQueueCount = queryInt(
            """
            SELECT COUNT(1) AS count
            FROM contribution_publication_queue
            WHERE author_pubkey = ?
              AND created_at >= ?
            """,
            binds: [.text(entry.authorPubkey), .int(dayAgo)]
        )
        if recentQueueCount >= Self.contributionDailyPublishLimit {
            flags.append("author_rate_limited")
        }

        let uniqueFlags = Array(Set(flags)).sorted()
        let penalties: [String: Int] = [
            "missing_required_fields": 35,
            "invalid_coordinates": 40,
            "possible_duplicate": 30,
            "photo_limit_exceeded": 15,
            "photo_resolution_low": 10,
            "photo_byte_cap_exceeded": 10,
            "photo_duplicate_hash": 10,
            "author_trust_low": 20,
            "author_rate_limited": 30,
        ]
        let totalPenalty = uniqueFlags.reduce(0) { partial, flag in
            partial + (penalties[flag] ?? 5)
        }
        let score = max(0, min(100, 100 - totalPenalty))
        return ServiceContributionQualityResult(score: score, reasons: uniqueFlags)
    }

    private func enqueueContributionPublication(
        entityType: String,
        entityID: String,
        authorPubkey: String,
        shareScope: String,
        publishState: String,
        qualityScore: Int,
        qualityFlags: String,
        reviewVersion: Int,
        payloadJSON: String,
        timestamp: Int
    ) {
        let queueID = "\(entityType):\(entityID)"
        _ = execute(
            """
            INSERT INTO contribution_publication_queue (
                queue_id, entity_type, entity_id, author_pubkey, share_scope, publish_state,
                quality_score, quality_flags, review_version, payload_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(queue_id) DO UPDATE SET
                share_scope = excluded.share_scope,
                publish_state = excluded.publish_state,
                quality_score = excluded.quality_score,
                quality_flags = excluded.quality_flags,
                review_version = excluded.review_version,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            binds: [
                .text(queueID),
                .text(entityType),
                .text(entityID),
                .text(authorPubkey.lowercased()),
                .text(shareScope),
                .text(publishState),
                .int(max(0, min(100, qualityScore))),
                .text(qualityFlags),
                .int(max(1, reviewVersion)),
                .text(payloadJSON),
                .int(timestamp),
                .int(timestamp),
            ]
        )
    }

    private func removeContributionQueueEntries(entityType: String, entityID: String) {
        _ = execute(
            "DELETE FROM contribution_publication_queue WHERE entity_type = ? AND entity_id = ?",
            binds: [.text(entityType), .text(entityID)]
        )
    }

    private func removePendingServiceOutboxEvents(serviceID: String) {
        let targetEntityID = "service:\(serviceID)"
        let rows = query(
            """
            SELECT event_id, envelope_json
            FROM event_outbox
            WHERE status IN ('pending', 'retry', 'failed')
            """
        )

        let eventIDsToDelete: [String] = rows.compactMap { row in
            guard let eventID = row["event_id"] as? String,
                  let envelopeJSON = row["envelope_json"] as? String,
                  let data = envelopeJSON.data(using: .utf8),
                  let envelope = try? JSONDecoder().decode(SemayEventEnvelope.self, from: data),
                  envelope.entityID == targetEntityID,
                  SemayEventEnvelope.EventType.serviceDirectoryLifecycle.contains(envelope.eventType) else {
                return nil
            }
            return eventID
        }

        guard !eventIDsToDelete.isEmpty else { return }
        for eventID in eventIDsToDelete {
            _ = execute("DELETE FROM event_outbox WHERE event_id = ?", binds: [.text(eventID)])
        }
    }

    private func coordinateDistanceMeters(lat1: Double, lon1: Double, lat2: Double, lon2: Double) -> Double {
        let earthRadius = 6_371_000.0
        let dLat = (lat2 - lat1) * .pi / 180.0
        let dLon = (lon2 - lon1) * .pi / 180.0
        let a = sin(dLat / 2) * sin(dLat / 2)
            + cos(lat1 * .pi / 180.0) * cos(lat2 * .pi / 180.0) * sin(dLon / 2) * sin(dLon / 2)
        let c = 2 * atan2(sqrt(a), sqrt(max(0, 1 - a)))
        return earthRadius * c
    }

    private func normalizedTagsJSON(from raw: String?) -> String {
        let trimmed = (raw ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return "[]" }
        guard let data = trimmed.data(using: .utf8) else { return "[]" }

        if let decoded = try? JSONDecoder().decode([String].self, from: data) {
            let normalized = decoded
                .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }
                .filter { !$0.isEmpty }
            return (try? String(data: JSONEncoder().encode(normalized), encoding: .utf8)) ?? "[]"
        }

        return "[]"
    }

    private func normalizedQualityReasonsJSON(from raw: String?) -> String {
        let trimmed = (raw ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return "[]" }
        if let data = trimmed.data(using: .utf8),
           let decoded = try? JSONDecoder().decode([String].self, from: data) {
            let normalized = decoded
                .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }
                .filter { !$0.isEmpty }
            return (try? String(data: JSONEncoder().encode(normalized), encoding: .utf8)) ?? "[]"
        }
        let split = trimmed
            .split(separator: ",")
            .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }
            .filter { !$0.isEmpty }
        if split.isEmpty {
            return "[]"
        }
        return (try? String(data: JSONEncoder().encode(split), encoding: .utf8)) ?? "[]"
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

    private func markOutboxRetry(
        eventID: String,
        eventType: SemayEventEnvelope.EventType,
        attempts: Int,
        error: String
    ) {
        if attempts >= outboxAttemptsLimit(for: eventType) {
            markOutboxFailed(eventID: eventID, attempts: attempts, error: error)
            return
        }

        let now = Int(Date().timeIntervalSince1970)
        let backoffSeconds = outboxBackoffSeconds(for: eventType, attempts: attempts)
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

    private func loadSyncCursor(key: String) -> String? {
        let rows = query(
            "SELECT cursor_value FROM sync_cursor WHERE key = ? LIMIT 1",
            binds: [.text(key)]
        )
        return rows.first?["cursor_value"] as? String
    }

    private func saveSyncCursor(key: String, value: String) {
        let now = Int(Date().timeIntervalSince1970)
        _ = execute(
            """
            INSERT INTO sync_cursor (key, cursor_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                cursor_value = excluded.cursor_value,
                updated_at = excluded.updated_at
            """,
            binds: [.text(key), .text(value), .int(now)]
        )
    }

    private func applyInboundEnvelope(_ envelope: SemayEventEnvelope) -> Bool {
        return applyInboundEnvelopeWithReason(envelope).applied
    }

    private func applyInboundEnvelopeWithReason(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        switch envelope.eventType {
        case .pinCreate, .pinUpdate:
            return applyInboundPin(envelope)
        case .pinApproval:
            return applyInboundPinApproval(envelope)
        case .businessRegister, .businessUpdate:
            return applyInboundBusiness(envelope)
        case .bulletinPost:
            return applyInboundBulletin(envelope)
        case .routeCurated, .routeCuratedCreate, .routeCuratedUpdate, .routeCuratedRetract, .routeCuratedReport:
            return applyInboundRouteCuratedEvent(envelope)
        case .routeCuratedEndorse:
            return InboundApplyResult(applied: false, reason: "unsupported-route-event")
        case .serviceDirectoryCreate, .serviceDirectoryUpdate, .serviceDirectoryRetract:
            return applyInboundServiceDirectoryEntry(envelope)
        case .serviceDirectoryEndorse:
            return applyInboundServiceDirectoryEndorsement(envelope)
        case .serviceDirectoryReport:
            return applyInboundServiceDirectoryReport(envelope)
        case .promiseCreate, .promiseAccept, .promiseReject, .promiseSettle, .chatMessage, .chatAck:
            return .unsupportedEventType
        }
    }

    private func applyInboundPin(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        let payload = envelope.payload
        guard let pinID = payload["pin_id"], !pinID.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-pin-id")
        }
        guard let name = payload["name"], !name.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-pin-name")
        }
        let type = payload["type"] ?? ""
        let details = payload["details"] ?? ""
        guard let lat = Double(payload["latitude"] ?? ""),
              let lon = Double(payload["longitude"] ?? ""),
              isLatitudeLongitudeWithinBounds(latitude: lat, longitude: lon) else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:invalid-pin-coordinates")
        }

        let address = SemayAddress.eAddress(latitude: lat, longitude: lon)
        let plusCode = (payload["plus_code"] ?? "").isEmpty ? address.plusCode : (payload["plus_code"] ?? "")
        let eAddress = (payload["e_address"] ?? "").isEmpty ? address.eAddress : (payload["e_address"] ?? "")
        let phone = payload["phone"] ?? ""
        let author = envelope.authorPubkey.lowercased()
        let ts = envelope.createdAt

        let sql = """
        INSERT INTO pins (
            pin_id, name, type, details, latitude, longitude, plus_code, e_address, phone,
            author_pubkey, approval_count, is_visible, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
        ON CONFLICT(pin_id) DO UPDATE SET
            name = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.name ELSE pins.name END,
            type = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.type ELSE pins.type END,
            details = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.details ELSE pins.details END,
            latitude = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.latitude ELSE pins.latitude END,
            longitude = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.longitude ELSE pins.longitude END,
            plus_code = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.plus_code ELSE pins.plus_code END,
            e_address = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.e_address ELSE pins.e_address END,
            phone = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.phone ELSE pins.phone END,
            author_pubkey = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.author_pubkey ELSE pins.author_pubkey END,
            updated_at = CASE WHEN excluded.updated_at >= pins.updated_at THEN excluded.updated_at ELSE pins.updated_at END
        """

        let success = execute(
            sql,
            binds: [
                .text(pinID), .text(name), .text(type), .text(details),
                .double(lat), .double(lon), .text(plusCode), .text(eAddress), .text(phone),
                .text(author), .int(ts), .int(ts)
            ]
        )
        if !success {
            return InboundApplyResult(applied: false, reason: "storage-failed:pin-upsert")
        }
        return InboundApplyResult(applied: true, reason: nil)
    }

    private func applyInboundPinApproval(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        let payload = envelope.payload
        guard let pinID = payload["pin_id"], !pinID.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-pin-id")
        }
        let pinRows = query("SELECT 1 FROM pins WHERE pin_id = ? LIMIT 1", binds: [.text(pinID)])
        guard !pinRows.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:unknown-pin")
        }

        let approver = (payload["approver_pubkey"] ?? envelope.authorPubkey).lowercased()
        if approver.isEmpty {
            return InboundApplyResult(applied: false, reason: "policy-rejected:empty-approver")
        }
        let now = envelope.createdAt

        let insert = """
        INSERT OR IGNORE INTO pin_approvals (pin_id, approver_pubkey, distance_meters, created_at)
        VALUES (?, ?, ?, ?)
        """
        let insertSuccess = execute(insert, binds: [.text(pinID), .text(approver), .double(250.0), .int(now)])
        if !insertSuccess {
            return InboundApplyResult(applied: false, reason: "storage-failed:pin-approval-insert")
        }

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
        let updateSuccess = execute(update, binds: [.int(count), .int(visible), .int(now), .text(pinID)])
        if !updateSuccess {
            return InboundApplyResult(applied: false, reason: "storage-failed:pin-visibility-update")
        }
        return InboundApplyResult(applied: true, reason: nil)
    }

    private func applyInboundBusiness(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        let payload = envelope.payload
        guard let businessID = payload["business_id"], !businessID.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-business-id")
        }
        guard let name = payload["name"], !name.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-business-name")
        }
        let category = payload["category"] ?? ""
        let details = payload["details"] ?? ""
        guard let lat = Double(payload["latitude"] ?? ""),
              let lon = Double(payload["longitude"] ?? ""),
              isLatitudeLongitudeWithinBounds(latitude: lat, longitude: lon) else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:invalid-business-coordinates")
        }

        let address = SemayAddress.eAddress(latitude: lat, longitude: lon)
        let plusCode = (payload["plus_code"] ?? "").isEmpty ? address.plusCode : (payload["plus_code"] ?? "")
        let eAddress = (payload["e_address"] ?? "").isEmpty ? address.eAddress : (payload["e_address"] ?? "")
        let phone = payload["phone"] ?? ""
        let lightningLink = payload["lightning_link"] ?? ""
        let cashuLink = payload["cashu_link"] ?? ""
        let owner = envelope.authorPubkey.lowercased()
        let qrPayload = "semay://business/\(businessID)"
        let ts = envelope.createdAt

        let sql = """
        INSERT INTO business_profiles (
            business_id, name, category, details, latitude, longitude, plus_code, e_address, phone,
            lightning_link, cashu_link,
            owner_pubkey, qr_payload, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(business_id) DO UPDATE SET
            name = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.name ELSE business_profiles.name END,
            category = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.category ELSE business_profiles.category END,
            details = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.details ELSE business_profiles.details END,
            latitude = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.latitude ELSE business_profiles.latitude END,
            longitude = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.longitude ELSE business_profiles.longitude END,
            plus_code = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.plus_code ELSE business_profiles.plus_code END,
            e_address = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.e_address ELSE business_profiles.e_address END,
            phone = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.phone ELSE business_profiles.phone END,
            lightning_link = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.lightning_link ELSE business_profiles.lightning_link END,
            cashu_link = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.cashu_link ELSE business_profiles.cashu_link END,
            owner_pubkey = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.owner_pubkey ELSE business_profiles.owner_pubkey END,
            qr_payload = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.qr_payload ELSE business_profiles.qr_payload END,
            updated_at = CASE WHEN excluded.updated_at >= business_profiles.updated_at THEN excluded.updated_at ELSE business_profiles.updated_at END
        """

        let success = execute(
            sql,
            binds: [
                .text(businessID), .text(name), .text(category), .text(details),
                .double(lat), .double(lon), .text(plusCode), .text(eAddress), .text(phone),
                .text(lightningLink), .text(cashuLink),
                .text(owner), .text(qrPayload), .int(ts), .int(ts)
            ]
        )
        if !success {
            return InboundApplyResult(applied: false, reason: "storage-failed:business-upsert")
        }
        return InboundApplyResult(applied: true, reason: nil)
    }

    private func applyInboundBulletin(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        let payload = envelope.payload
        guard let bulletinID = payload["bulletin_id"], !bulletinID.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-bulletin-id")
        }
        guard let title = payload["title"], !title.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-bulletin-title")
        }
        let category = payload["category"] ?? BulletinCategory.general.rawValue
        let body = payload["body"] ?? ""
        let phone = payload["phone"] ?? ""
        guard let lat = Double(payload["latitude"] ?? ""),
              let lon = Double(payload["longitude"] ?? ""),
              isLatitudeLongitudeWithinBounds(latitude: lat, longitude: lon) else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:invalid-bulletin-coordinates")
        }

        let address = SemayAddress.eAddress(latitude: lat, longitude: lon)
        let plusCode = (payload["plus_code"] ?? "").isEmpty ? address.plusCode : (payload["plus_code"] ?? "")
        let eAddress = (payload["e_address"] ?? "").isEmpty ? address.eAddress : (payload["e_address"] ?? "")
        let author = envelope.authorPubkey.lowercased()
        let ts = envelope.createdAt

        let sql = """
        INSERT INTO bulletins (
            bulletin_id, title, category, body, phone, latitude, longitude,
            plus_code, e_address, author_pubkey, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(bulletin_id) DO UPDATE SET
            title = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.title ELSE bulletins.title END,
            category = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.category ELSE bulletins.category END,
            body = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.body ELSE bulletins.body END,
            phone = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.phone ELSE bulletins.phone END,
            latitude = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.latitude ELSE bulletins.latitude END,
            longitude = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.longitude ELSE bulletins.longitude END,
            plus_code = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.plus_code ELSE bulletins.plus_code END,
            e_address = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.e_address ELSE bulletins.e_address END,
            author_pubkey = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.author_pubkey ELSE bulletins.author_pubkey END,
            updated_at = CASE WHEN excluded.updated_at >= bulletins.updated_at THEN excluded.updated_at ELSE bulletins.updated_at END
        """

        let success = execute(
            sql,
            binds: [
                .text(bulletinID), .text(title), .text(category), .text(body), .text(phone),
                .double(lat), .double(lon),
                .text(plusCode), .text(eAddress), .text(author),
                .int(ts), .int(ts)
            ]
        )
        if !success {
            return InboundApplyResult(applied: false, reason: "storage-failed:bulletin-upsert")
        }
        return InboundApplyResult(applied: true, reason: nil)
    }

    private func applyInboundRouteCuratedEvent(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        let payload = envelope.payload
        guard let routeID = payload["route_id"], !routeID.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-route-id")
        }

        switch envelope.eventType {
        case .routeCuratedReport:
            let reason = payload["reason"] ?? "mismatch"
            let now = Int(payload["created_at"] ?? "") ?? envelope.createdAt
            _ = execute(
                """
                INSERT OR REPLACE INTO route_reports (route_id, reporter_pubkey, reason, created_at)
                VALUES (?, ?, ?, ?)
                """,
                binds: [.text(routeID), .text(envelope.authorPubkey), .text(reason), .int(now)]
            )
            refreshTrustSummariesForRoute(routeID)
            refreshRoutes()
            return InboundApplyResult(applied: true, reason: nil)

        case .routeCuratedEndorse:
            let actor = (payload["actor_pubkey"] ?? envelope.authorPubkey).lowercased()
            let score = max(0, min(5, Int(payload["score"] ?? "") ?? 1))
            let reason = payload["reason"] ?? "verified"
            let now = Int(payload["created_at"] ?? "") ?? envelope.createdAt

            let rows = query("SELECT 1 FROM curated_routes WHERE route_id = ? LIMIT 1", binds: [.text(routeID)])
            guard !rows.isEmpty else {
                return InboundApplyResult(applied: false, reason: "policy-rejected:unknown-route-for-endorse")
            }

            _ = execute(
                """
                INSERT INTO route_endorsements (route_id, actor_pubkey, score, reason, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(route_id, actor_pubkey) DO UPDATE SET
                    score = excluded.score,
                    reason = excluded.reason,
                    created_at = excluded.created_at
                """,
                binds: [.text(routeID), .text(actor), .int(score), .text(reason), .int(now)]
            )

            refreshTrustSummariesForRoute(routeID)
            refreshRoutes()
            return InboundApplyResult(applied: true, reason: nil)

        case .routeCuratedCreate, .routeCuratedUpdate, .routeCuratedRetract, .routeCurated:
            let title = payload["title"] ?? ""
            let summary = payload["summary"] ?? ""
            let city = payload["city"] ?? ""
            let fromLabel = payload["from_label"] ?? ""
            let toLabel = payload["to_label"] ?? ""
            let transportType = SemayRouteTransport(
                rawValue: (payload["transport_type"] ?? "")
            )?.rawValue ?? SemayRouteTransport.unknown.rawValue
            let waypoints = decodedWaypoints(from: payload["waypoints"] ?? "[]")
            let reliabilityScore = max(0, min(100, Int(payload["reliability_score"] ?? "") ?? 0))
            let trustScore = Int(payload["trust_score"] ?? "") ?? max(0, min(100, reliabilityScore))
            let sourceTrustTier = Int(payload["source_trust_tier"] ?? "") ?? 0
            let statusRaw = (payload["status"] ?? "active").lowercased()
            let status = (statusRaw == "retracted" || statusRaw == "active") ? statusRaw : "active"
            if envelope.eventType == .routeCuratedCreate || envelope.eventType == .routeCuratedUpdate {
                if title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    return InboundApplyResult(applied: false, reason: "policy-rejected:missing-route-title")
                }
            }
            if envelope.eventType == .routeCuratedCreate && waypoints.isEmpty {
                return InboundApplyResult(applied: false, reason: "policy-rejected:missing-route-waypoints")
            }

            if fromLabel.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty && toLabel.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty && waypoints.isEmpty {
                return InboundApplyResult(applied: false, reason: "policy-rejected:missing-route-geometry")
            }

            let updatedAt = Int(payload["updated_at"] ?? "") ?? max(routeDefaultUpdatedAt(routeID), envelope.createdAt)
            let createdAt = Int(payload["created_at"] ?? "") ?? min(updatedAt, envelope.createdAt)
            let author = payload["author_pubkey"] ?? envelope.authorPubkey
            let trustScoreClamped = max(0, min(100, trustScore))
            let sourceTrustTierClamped = max(0, min(4, sourceTrustTier))

            let route = SemayCuratedRoute(
                routeID: routeID,
                title: title,
                summary: summary,
                city: city,
                fromLabel: fromLabel,
                toLabel: toLabel,
                transportType: transportType,
                waypoints: waypoints,
                reliabilityScore: max(0, min(100, reliabilityScore)),
                trustScore: trustScoreClamped,
                sourceTrustTier: sourceTrustTierClamped,
                status: status == "retracted" ? "retracted" : "active",
                authorPubkey: author,
                createdAt: createdAt,
                updatedAt: max(createdAt, updatedAt)
            )

            let sourceEventType: SemayEventEnvelope.EventType = envelope.eventType == .routeCurated
                ? .routeCuratedUpdate
                : envelope.eventType
            persistCuratedRoute(route, sourceEventType: sourceEventType)
            return InboundApplyResult(applied: true, reason: nil)

        default:
            return InboundApplyResult(applied: false, reason: "unsupported-route-event")
        }
    }

    private func routeDefaultUpdatedAt(_ routeID: String) -> Int {
        let rows = query("SELECT updated_at FROM curated_routes WHERE route_id = ? LIMIT 1", binds: [.text(routeID)])
        return Int(rows.first?["updated_at"] as? Int ?? 0)
    }

    private func applyInboundServiceDirectoryEntry(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        let payload = envelope.payload
        guard let serviceID = payload["service_id"], !serviceID.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-service-id")
        }
        if envelope.eventType == .serviceDirectoryCreate || envelope.eventType == .serviceDirectoryUpdate {
            guard let name = payload["name"], !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                return InboundApplyResult(applied: false, reason: "policy-rejected:missing-service-name")
            }
        }
        guard envelope.eventType == .serviceDirectoryCreate || envelope.eventType == .serviceDirectoryUpdate || envelope.eventType == .serviceDirectoryRetract else {
            return InboundApplyResult(applied: false, reason: "unsupported-service-event")
        }

        let rawServiceType = (payload["service_type"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let serviceType = rawServiceType.isEmpty ? "community-service" : rawServiceType
        let category = payload["category"] ?? ""
        let details = payload["details"] ?? ""
        let city = payload["city"] ?? ""
        let country = payload["country"] ?? ""
        let lat = Double(payload["latitude"] ?? "")
        let lon = Double(payload["longitude"] ?? "")
        let normalizedLatitude = lat ?? 0
        let normalizedLongitude = lon ?? 0
        let name = envelope.eventType == .serviceDirectoryRetract
            ? (payload["name"] ?? "")
            : (payload["name"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)

        if envelope.eventType == .serviceDirectoryCreate || envelope.eventType == .serviceDirectoryUpdate {
            if name.isEmpty {
                return InboundApplyResult(applied: false, reason: "policy-rejected:missing-service-name")
            }
            if !isLatitudeLongitudeWithinBounds(latitude: normalizedLatitude, longitude: normalizedLongitude) {
                return InboundApplyResult(applied: false, reason: "policy-rejected:invalid-service-coordinates")
            }
        }

        let sourceUrgency = (payload["urgency"] ?? "").lowercased()
        let urgency = SemayServiceUrgency(rawValue: sourceUrgency) != nil ? sourceUrgency : SemayServiceUrgency.medium.rawValue
        if envelope.eventType == .serviceDirectoryUpdate || envelope.eventType == .serviceDirectoryCreate {
            guard ["high", "critical", "low", "medium"].contains(urgency) else {
                return InboundApplyResult(applied: false, reason: "policy-rejected:invalid-service-urgency")
            }
        }

        let address = SemayAddress.eAddress(
            latitude: envelope.eventType == .serviceDirectoryRetract ? 0 : normalizedLatitude,
            longitude: envelope.eventType == .serviceDirectoryRetract ? 0 : normalizedLongitude
        )
        let plusCode = (payload["plus_code"] ?? "").isEmpty ? (envelope.eventType == .serviceDirectoryRetract ? "" : address.plusCode) : (payload["plus_code"] ?? "")
        let eAddress = (payload["e_address"] ?? "").isEmpty ? (envelope.eventType == .serviceDirectoryRetract ? "" : address.eAddress) : (payload["e_address"] ?? "")
        let display = SemayAddressDisplayBuilder.build(
            nameHint: name,
            latitude: normalizedLatitude,
            longitude: normalizedLongitude,
            plusCode: plusCode,
            eAddress: eAddress
        )
        let addressLabel = (payload["address_label"] ?? "").isEmpty ? display.addressLabel : (payload["address_label"] ?? "")
        let locality = (payload["locality"] ?? "").isEmpty ? display.locality : (payload["locality"] ?? "")
        let adminArea = (payload["admin_area"] ?? "").isEmpty ? display.adminArea : (payload["admin_area"] ?? "")
        let countryCode = (payload["country_code"] ?? "").isEmpty ? display.countryCode : (payload["country_code"] ?? "")
        let resolvedCity = city.isEmpty ? locality : city
        let resolvedCountry = country.isEmpty ? display.countryName : country
        let phone = payload["phone"] ?? ""
        let website = payload["website"] ?? ""
        let emergencyContact = payload["emergency_contact"] ?? ""
        let verified = {
            let raw = (payload["verified"] ?? "").lowercased()
            return raw == "true" || raw == "1"
        }()
        let trustScore = Int(payload["trust_score"] ?? "") ?? (verified ? 65 : 40)
        let sourceTrustTier = Int(payload["source_trust_tier"] ?? "") ?? 0
        let statusRaw = (payload["status"] ?? "active").lowercased()
        let status = statusRaw == "retracted" || statusRaw == "active" ? statusRaw : "active"
        let tagsJSON = normalizedTagsJSON(from: payload["tags_json"])
        let shareScopeRaw = (payload["share_scope"] ?? "personal").lowercased()
        let shareScope = SemayContributionScope(rawValue: shareScopeRaw) ?? .personal
        let publishStateRaw = (payload["publish_state"] ?? "").lowercased()
        let publishState = SemayContributionPublishState(rawValue: publishStateRaw)
            ?? (shareScope == .network ? .pendingReview : .localOnly)
        let qualityScore = max(0, min(100, Int(payload["quality_score"] ?? "") ?? 0))
        let qualityReasonsJSON = normalizedQualityReasonsJSON(from: payload["quality_reasons"])
        let reviewVersion = max(1, Int(payload["review_version"] ?? "") ?? 1)
        let lastQualityCheckedAt = max(0, Int(payload["last_quality_checked_at"] ?? "") ?? 0)
        let author = payload["author_pubkey"] ?? envelope.authorPubkey
        let updatedAt = Int(payload["updated_at"] ?? "") ?? envelope.createdAt
        let createdAt = Int(payload["created_at"] ?? "") ?? envelope.createdAt
        let payloadHasPhotoRefs = payload["photo_refs_json"] != nil
        var decodedPhotoRefs: [SemayServicePhotoRef] = []
        if let refsRaw = payload["photo_refs_json"] {
            guard let parsed = decodePhotoRefsFromPayload(
                refsRaw,
                serviceID: serviceID,
                createdAt: createdAt,
                updatedAt: updatedAt
            ) else {
                return InboundApplyResult(applied: false, reason: "policy-rejected:invalid-photo-refs-json")
            }
            decodedPhotoRefs = parsed
        }
        let primaryPhotoID = (payload["primary_photo_id"] ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        let photoCount = Int(payload["photo_count"] ?? "") ?? decodedPhotoRefs.count

        let entry = SemayServiceDirectoryEntry(
            serviceID: serviceID,
            name: name,
            serviceType: serviceType,
            category: category,
            details: details,
            city: resolvedCity,
            country: resolvedCountry,
            latitude: normalizedLatitude,
            longitude: normalizedLongitude,
            plusCode: plusCode,
            eAddress: eAddress,
            addressLabel: addressLabel,
            locality: locality,
            adminArea: adminArea,
            countryCode: countryCode,
            phone: phone,
            website: website,
            emergencyContact: emergencyContact,
            urgency: urgency,
            verified: verified,
            trustScore: max(0, min(100, trustScore)),
            sourceTrustTier: max(0, min(4, sourceTrustTier)),
            status: status,
            tagsJSON: tagsJSON,
            primaryPhotoID: primaryPhotoID,
            photoCount: max(0, photoCount),
            shareScope: shareScope,
            publishState: publishState,
            qualityScore: qualityScore,
            qualityReasonsJSON: qualityReasonsJSON,
            reviewVersion: reviewVersion,
            lastQualityCheckedAt: lastQualityCheckedAt,
            authorPubkey: author,
            createdAt: createdAt,
            updatedAt: max(createdAt, updatedAt)
        )
        persistServiceDirectoryEntry(entry, sourceEventType: envelope.eventType)
        if payloadHasPhotoRefs {
            upsertServicePhotoRefs(serviceID: serviceID, refs: decodedPhotoRefs, emitServiceUpdate: false)
        }
        return InboundApplyResult(applied: true, reason: nil)
    }

    private func applyInboundServiceDirectoryEndorsement(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        let payload = envelope.payload
        guard let serviceID = payload["service_id"], !serviceID.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-service-id")
        }
        let serviceRows = query("SELECT 1 FROM service_directory_entries WHERE service_id = ? LIMIT 1", binds: [.text(serviceID)])
        guard !serviceRows.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:unknown-service-for-endorse")
        }
        let reason = payload["reason"] ?? "verified"
        let actor = (payload["actor_pubkey"] ?? envelope.authorPubkey).lowercased()
        let score = max(0, min(5, Int(payload["score"] ?? "") ?? 1))
        let now = Int(payload["created_at"] ?? "") ?? envelope.createdAt

        _ = execute(
            """
            INSERT INTO service_endorsements (service_id, actor_pubkey, score, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(service_id, actor_pubkey) DO UPDATE SET
                score = excluded.score,
                reason = excluded.reason,
                created_at = excluded.created_at
            """,
            binds: [.text(serviceID), .text(actor), .int(score), .text(reason), .int(now)]
        )
        refreshTrustSummariesForService(serviceID)
        refreshServiceDirectory()
        return InboundApplyResult(applied: true, reason: nil)
    }

    private func applyInboundServiceDirectoryReport(_ envelope: SemayEventEnvelope) -> InboundApplyResult {
        let payload = envelope.payload
        guard let serviceID = payload["service_id"], !serviceID.isEmpty else {
            return InboundApplyResult(applied: false, reason: "policy-rejected:missing-service-id")
        }
        let reason = payload["reason"] ?? "mismatch"
        let now = Int(payload["created_at"] ?? "") ?? envelope.createdAt
        _ = execute(
            """
            INSERT OR REPLACE INTO service_reports (service_id, reporter_pubkey, reason, created_at)
            VALUES (?, ?, ?, ?)
            """,
            binds: [.text(serviceID), .text(envelope.authorPubkey), .text(reason), .int(now)]
        )
        refreshTrustSummariesForService(serviceID)
        refreshServiceDirectory()
        return InboundApplyResult(applied: true, reason: nil)
    }

    private func isLatitudeLongitudeWithinBounds(latitude: Double, longitude: Double) -> Bool {
        if abs(latitude) > 90 || abs(longitude) > 180 { return false }
        return !(latitude == 0 && longitude == 0)
    }

}

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
