import Foundation

enum PromiseStatus: String, Codable, CaseIterable {
    case pending
    case accepted
    case rejected
    case settled
    case expired
}

enum SettlementProofType: String, Codable, CaseIterable {
    case lightningPreimage = "lightning_preimage"
    case lightningPaymentHash = "lightning_payment_hash"
}

enum SettlementSubmitter: String, Codable, CaseIterable {
    case payer
    case merchant
}

struct PromiseNote: Codable, Identifiable, Equatable {
    let promiseID: String
    let merchantID: String
    let payerPubkey: String
    let amountMsat: UInt64
    let fiatQuote: String?
    let currency: String
    let expiresAt: Int
    let nonce: String
    let payerSignature: String
    var status: PromiseStatus
    let createdAt: Int
    var updatedAt: Int

    var id: String { promiseID }

    enum CodingKeys: String, CodingKey {
        case promiseID = "promise_id"
        case merchantID = "merchant_id"
        case payerPubkey = "payer_pubkey"
        case amountMsat = "amount_msat"
        case fiatQuote = "fiat_quote"
        case currency
        case expiresAt = "expires_at"
        case nonce
        case payerSignature = "payer_signature"
        case status
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }

    init(
        promiseID: String = UUID().uuidString.lowercased(),
        merchantID: String,
        payerPubkey: String,
        amountMsat: UInt64,
        fiatQuote: String? = nil,
        currency: String = "BTC_LN",
        expiresAt: Int,
        nonce: String,
        payerSignature: String,
        status: PromiseStatus = .pending,
        createdAt: Int = Int(Date().timeIntervalSince1970),
        updatedAt: Int = Int(Date().timeIntervalSince1970)
    ) {
        self.promiseID = promiseID
        self.merchantID = merchantID
        self.payerPubkey = payerPubkey
        self.amountMsat = amountMsat
        self.fiatQuote = fiatQuote
        self.currency = currency
        self.expiresAt = expiresAt
        self.nonce = nonce
        self.payerSignature = payerSignature
        self.status = status
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    static func defaultExpiry(now: Date = Date()) -> Int {
        Int(now.addingTimeInterval(24 * 60 * 60).timeIntervalSince1970)
    }
}

struct SettlementReceipt: Codable, Identifiable, Equatable {
    let receiptID: String
    let promiseID: String
    let proofType: SettlementProofType
    let proofValue: String
    let submittedBy: SettlementSubmitter
    let submittedAt: Int
    let submitterSignature: String

    var id: String { receiptID }

    enum CodingKeys: String, CodingKey {
        case receiptID = "receipt_id"
        case promiseID = "promise_id"
        case proofType = "proof_type"
        case proofValue = "proof_value"
        case submittedBy = "submitted_by"
        case submittedAt = "submitted_at"
        case submitterSignature = "submitter_signature"
    }

    init(
        receiptID: String = UUID().uuidString.lowercased(),
        promiseID: String,
        proofType: SettlementProofType,
        proofValue: String,
        submittedBy: SettlementSubmitter,
        submittedAt: Int = Int(Date().timeIntervalSince1970),
        submitterSignature: String
    ) {
        self.receiptID = receiptID
        self.promiseID = promiseID
        self.proofType = proofType
        self.proofValue = proofValue
        self.submittedBy = submittedBy
        self.submittedAt = submittedAt
        self.submitterSignature = submitterSignature
    }
}

struct SemayMapPin: Codable, Identifiable, Equatable {
    let pinID: String
    var name: String
    var type: String
    var details: String
    var latitude: Double
    var longitude: Double
    var plusCode: String
    var eAddress: String
    var phone: String
    var authorPubkey: String
    var approvalCount: Int
    var isVisible: Bool
    let createdAt: Int
    var updatedAt: Int

    var id: String { pinID }

    enum CodingKeys: String, CodingKey {
        case pinID = "pin_id"
        case name
        case type
        case details
        case latitude
        case longitude
        case plusCode = "plus_code"
        case eAddress = "e_address"
        case phone
        case authorPubkey = "author_pubkey"
        case approvalCount = "approval_count"
        case isVisible = "is_visible"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }

    init(
        pinID: String,
        name: String,
        type: String,
        details: String,
        latitude: Double,
        longitude: Double,
        plusCode: String = "",
        eAddress: String,
        phone: String = "",
        authorPubkey: String,
        approvalCount: Int,
        isVisible: Bool,
        createdAt: Int,
        updatedAt: Int
    ) {
        self.pinID = pinID
        self.name = name
        self.type = type
        self.details = details
        self.latitude = latitude
        self.longitude = longitude
        self.plusCode = plusCode
        self.eAddress = eAddress
        self.phone = phone
        self.authorPubkey = authorPubkey
        self.approvalCount = approvalCount
        self.isVisible = isVisible
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        pinID = try c.decode(String.self, forKey: .pinID)
        name = try c.decode(String.self, forKey: .name)
        type = try c.decode(String.self, forKey: .type)
        details = try c.decode(String.self, forKey: .details)
        latitude = try c.decode(Double.self, forKey: .latitude)
        longitude = try c.decode(Double.self, forKey: .longitude)
        plusCode = try c.decodeIfPresent(String.self, forKey: .plusCode) ?? ""
        eAddress = try c.decode(String.self, forKey: .eAddress)
        phone = try c.decodeIfPresent(String.self, forKey: .phone) ?? ""
        authorPubkey = try c.decode(String.self, forKey: .authorPubkey)
        approvalCount = try c.decode(Int.self, forKey: .approvalCount)
        isVisible = try c.decode(Bool.self, forKey: .isVisible)
        createdAt = try c.decode(Int.self, forKey: .createdAt)
        updatedAt = try c.decode(Int.self, forKey: .updatedAt)
    }
}

struct BusinessProfile: Codable, Identifiable, Equatable {
    let businessID: String
    var name: String
    var category: String
    var details: String
    var latitude: Double
    var longitude: Double
    var plusCode: String
    var eAddress: String
    var phone: String
    var lightningLink: String
    var cashuLink: String
    var ownerPubkey: String
    var qrPayload: String
    let createdAt: Int
    var updatedAt: Int

    var id: String { businessID }

    enum CodingKeys: String, CodingKey {
        case businessID = "business_id"
        case name
        case category
        case details
        case latitude
        case longitude
        case plusCode = "plus_code"
        case eAddress = "e_address"
        case phone
        case lightningLink = "lightning_link"
        case cashuLink = "cashu_link"
        case ownerPubkey = "owner_pubkey"
        case qrPayload = "qr_payload"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }

    init(
        businessID: String,
        name: String,
        category: String,
        details: String,
        latitude: Double = 0,
        longitude: Double = 0,
        plusCode: String = "",
        eAddress: String,
        phone: String = "",
        lightningLink: String = "",
        cashuLink: String = "",
        ownerPubkey: String,
        qrPayload: String,
        createdAt: Int,
        updatedAt: Int
    ) {
        self.businessID = businessID
        self.name = name
        self.category = category
        self.details = details
        self.latitude = latitude
        self.longitude = longitude
        self.plusCode = plusCode
        self.eAddress = eAddress
        self.phone = phone
        self.lightningLink = lightningLink
        self.cashuLink = cashuLink
        self.ownerPubkey = ownerPubkey
        self.qrPayload = qrPayload
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        businessID = try c.decode(String.self, forKey: .businessID)
        name = try c.decode(String.self, forKey: .name)
        category = try c.decode(String.self, forKey: .category)
        details = try c.decode(String.self, forKey: .details)
        latitude = try c.decodeIfPresent(Double.self, forKey: .latitude) ?? 0
        longitude = try c.decodeIfPresent(Double.self, forKey: .longitude) ?? 0
        plusCode = try c.decodeIfPresent(String.self, forKey: .plusCode) ?? ""
        eAddress = try c.decode(String.self, forKey: .eAddress)
        phone = try c.decodeIfPresent(String.self, forKey: .phone) ?? ""
        lightningLink = try c.decodeIfPresent(String.self, forKey: .lightningLink) ?? ""
        cashuLink = try c.decodeIfPresent(String.self, forKey: .cashuLink) ?? ""
        ownerPubkey = try c.decode(String.self, forKey: .ownerPubkey)
        qrPayload = try c.decode(String.self, forKey: .qrPayload)
        createdAt = try c.decode(Int.self, forKey: .createdAt)
        updatedAt = try c.decode(Int.self, forKey: .updatedAt)
    }
}

enum BulletinCategory: String, Codable, CaseIterable, Identifiable {
    case tourism
    case services
    case safety
    case helpWanted
    case helpOffered
    case logistics
    case opportunity
    case general

    var id: String { rawValue }

    var title: String {
        switch self {
        case .tourism: return "Tourism"
        case .services: return "Services"
        case .safety: return "Safety"
        case .helpWanted: return "Help Wanted"
        case .helpOffered: return "Help Offered"
        case .logistics: return "Logistics"
        case .opportunity: return "Opportunities"
        case .general: return "General"
        }
    }
}

enum TrustScoreTier: Int, Codable, CaseIterable {
    case low = 0
    case medium = 1
    case high = 2
    case verified = 3

    var title: String {
        switch self {
        case .low: return "Low"
        case .medium: return "Trusted"
        case .high: return "Strong"
        case .verified: return "Verified"
        }
    }
}

enum SemayRouteTransport: String, Codable, CaseIterable {
    case walk
    case bus
    case taxi
    case car
    case train
    case mixed
    case unknown

    var title: String {
        switch self {
        case .walk: return "Walk"
        case .bus: return "Bus"
        case .taxi: return "Taxi"
        case .car: return "Car"
        case .train: return "Train"
        case .mixed: return "Multi"
        case .unknown: return "Unknown"
        }
    }
}

struct SemayRouteWaypoint: Codable, Equatable {
    let title: String?
    let latitude: Double
    let longitude: Double

    var latitudeLongitude: (Double, Double) {
        (latitude, longitude)
    }
}

struct SemayCuratedRoute: Codable, Identifiable, Equatable {
    let routeID: String
    var title: String
    var summary: String
    var city: String
    var fromLabel: String
    var toLabel: String
    var transportType: String
    var waypoints: [SemayRouteWaypoint]
    var reliabilityScore: Int
    var trustScore: Int
    var sourceTrustTier: Int
    var status: String
    var authorPubkey: String
    let createdAt: Int
    var updatedAt: Int

    var id: String { routeID }

    var isActive: Bool { status == "active" }
    var isSafe: Bool { trustScore >= 70 || sourceTrustTier >= 3 }
    var trustBadge: String {
        if trustScore >= 90 { return "✅ High Trust" }
        if trustScore >= 70 { return "🟢 Trusted" }
        if trustScore >= 50 { return "🟡 Medium Trust" }
        if trustScore >= 30 { return "🟠 Emerging" }
        return "⚪ Low Trust"
    }

    var centerLatitude: Double? {
        guard let first = waypoints.first else { return nil }
        return first.latitude
    }

    var centerLongitude: Double? {
        guard let first = waypoints.first else { return nil }
        return first.longitude
    }

    init(
        routeID: String,
        title: String,
        summary: String,
        city: String,
        fromLabel: String,
        toLabel: String,
        transportType: String,
        waypoints: [SemayRouteWaypoint],
        reliabilityScore: Int,
        trustScore: Int,
        sourceTrustTier: Int,
        status: String,
        authorPubkey: String,
        createdAt: Int,
        updatedAt: Int
    ) {
        self.routeID = routeID
        self.title = title
        self.summary = summary
        self.city = city
        self.fromLabel = fromLabel
        self.toLabel = toLabel
        self.transportType = transportType
        self.waypoints = waypoints
        self.reliabilityScore = reliabilityScore
        self.trustScore = trustScore
        self.sourceTrustTier = sourceTrustTier
        self.status = status
        self.authorPubkey = authorPubkey
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        routeID = try c.decode(String.self, forKey: .routeID)
        title = try c.decode(String.self, forKey: .title)
        summary = try c.decodeIfPresent(String.self, forKey: .summary) ?? ""
        city = try c.decodeIfPresent(String.self, forKey: .city) ?? ""
        fromLabel = try c.decodeIfPresent(String.self, forKey: .fromLabel) ?? ""
        toLabel = try c.decodeIfPresent(String.self, forKey: .toLabel) ?? ""
        transportType = try c.decodeIfPresent(String.self, forKey: .transportType) ?? SemayRouteTransport.unknown.rawValue
        waypoints = (try c.decodeIfPresent([SemayRouteWaypoint].self, forKey: .waypoints)) ?? []
        reliabilityScore = try c.decodeIfPresent(Int.self, forKey: .reliabilityScore) ?? 0
        trustScore = try c.decodeIfPresent(Int.self, forKey: .trustScore) ?? max(0, reliabilityScore)
        sourceTrustTier = try c.decodeIfPresent(Int.self, forKey: .sourceTrustTier) ?? 0
        status = try c.decodeIfPresent(String.self, forKey: .status) ?? "active"
        authorPubkey = try c.decode(String.self, forKey: .authorPubkey)
        createdAt = try c.decode(Int.self, forKey: .createdAt)
        updatedAt = try c.decodeIfPresent(Int.self, forKey: .updatedAt) ?? createdAt
    }

    enum CodingKeys: String, CodingKey {
        case routeID = "route_id"
        case title
        case summary
        case city
        case fromLabel = "from_label"
        case toLabel = "to_label"
        case transportType = "transport_type"
        case waypoints
        case reliabilityScore = "reliability_score"
        case trustScore = "trust_score"
        case sourceTrustTier = "source_trust_tier"
        case status
        case authorPubkey = "author_pubkey"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }
}

enum SemayServiceUrgency: String, Codable, CaseIterable {
    case low
    case medium
    case high
    case critical

    var title: String {
        switch self {
        case .low: return "Low"
        case .medium: return "Medium"
        case .high: return "High"
        case .critical: return "Critical"
        }
    }
}

enum SemayContributionScope: String, Codable, CaseIterable {
    case personal
    case network
}

enum SemayContributionPublishState: String, Codable, CaseIterable {
    case localOnly = "local_only"
    case pendingReview = "pending_review"
    case published
    case rejected
}

struct SemayServicePhotoRef: Codable, Identifiable, Equatable {
    let photoID: String
    let serviceID: String
    let sha256: String
    let mimeType: String
    let width: Int
    let height: Int
    let bytesFull: Int
    let bytesThumb: Int
    let takenAt: Int?
    let exifLatitude: Double?
    let exifLongitude: Double?
    let geoSource: String
    let primary: Bool
    let remoteURL: String?
    let createdAt: Int
    let updatedAt: Int

    var id: String { photoID }

    enum CodingKeys: String, CodingKey {
        case photoID = "photo_id"
        case serviceID = "service_id"
        case sha256
        case mimeType = "mime_type"
        case width
        case height
        case bytesFull = "bytes_full"
        case bytesThumb = "bytes_thumb"
        case takenAt = "taken_at"
        case exifLatitude = "exif_latitude"
        case exifLongitude = "exif_longitude"
        case geoSource = "geo_source"
        case primary
        case remoteURL = "remote_url"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }

    init(
        photoID: String,
        serviceID: String,
        sha256: String,
        mimeType: String,
        width: Int,
        height: Int,
        bytesFull: Int,
        bytesThumb: Int,
        takenAt: Int?,
        exifLatitude: Double?,
        exifLongitude: Double?,
        geoSource: String,
        primary: Bool,
        remoteURL: String?,
        createdAt: Int,
        updatedAt: Int
    ) {
        self.photoID = photoID
        self.serviceID = serviceID
        self.sha256 = sha256
        self.mimeType = mimeType
        self.width = width
        self.height = height
        self.bytesFull = bytesFull
        self.bytesThumb = bytesThumb
        self.takenAt = takenAt
        self.exifLatitude = exifLatitude
        self.exifLongitude = exifLongitude
        self.geoSource = geoSource
        self.primary = primary
        self.remoteURL = remoteURL
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        photoID = try c.decode(String.self, forKey: .photoID)
        serviceID = try c.decodeIfPresent(String.self, forKey: .serviceID) ?? ""
        sha256 = try c.decodeIfPresent(String.self, forKey: .sha256) ?? ""
        mimeType = try c.decodeIfPresent(String.self, forKey: .mimeType) ?? "image/jpeg"
        width = try c.decodeIfPresent(Int.self, forKey: .width) ?? 0
        height = try c.decodeIfPresent(Int.self, forKey: .height) ?? 0
        bytesFull = try c.decodeIfPresent(Int.self, forKey: .bytesFull) ?? 0
        bytesThumb = try c.decodeIfPresent(Int.self, forKey: .bytesThumb) ?? 0
        takenAt = try c.decodeIfPresent(Int.self, forKey: .takenAt)
        exifLatitude = try c.decodeIfPresent(Double.self, forKey: .exifLatitude)
        exifLongitude = try c.decodeIfPresent(Double.self, forKey: .exifLongitude)
        geoSource = try c.decodeIfPresent(String.self, forKey: .geoSource) ?? "none"
        primary = try c.decodeIfPresent(Bool.self, forKey: .primary) ?? false
        remoteURL = try c.decodeIfPresent(String.self, forKey: .remoteURL)
        createdAt = try c.decodeIfPresent(Int.self, forKey: .createdAt) ?? Int(Date().timeIntervalSince1970)
        updatedAt = try c.decodeIfPresent(Int.self, forKey: .updatedAt) ?? createdAt
    }
}

struct SemayServiceDirectoryEntry: Codable, Identifiable, Equatable {
    let serviceID: String
    var name: String
    var serviceType: String
    var category: String
    var details: String
    var city: String
    var country: String
    var latitude: Double
    var longitude: Double
    var plusCode: String
    var eAddress: String
    var addressLabel: String
    var locality: String
    var adminArea: String
    var countryCode: String
    var phone: String
    var website: String
    var emergencyContact: String
    var urgency: String
    var verified: Bool
    var trustScore: Int
    var sourceTrustTier: Int
    var status: String
    var tagsJSON: String
    var primaryPhotoID: String
    var photoCount: Int
    var shareScope: SemayContributionScope
    var publishState: SemayContributionPublishState
    var qualityScore: Int
    var qualityReasonsJSON: String
    var reviewVersion: Int
    var lastQualityCheckedAt: Int
    var authorPubkey: String
    let createdAt: Int
    var updatedAt: Int

    var id: String { serviceID }

    var isActive: Bool { status == "active" }
    var trustBadge: String {
        if trustScore >= 90 { return "✅ High Trust" }
        if trustScore >= 70 { return "🟢 Trusted" }
        if trustScore >= 50 { return "🟡 Medium Trust" }
        if trustScore >= 30 { return "🟠 Emerging" }
        return "⚪ Low Trust"
    }

    var tags: [String] {
        guard let data = tagsJSON.data(using: .utf8),
              let decoded = try? JSONDecoder().decode([String].self, from: data) else {
            return []
        }
        return decoded
    }

    var qualityReasons: [String] {
        guard let data = qualityReasonsJSON.data(using: .utf8),
              let decoded = try? JSONDecoder().decode([String].self, from: data) else {
            return []
        }
        return decoded
    }

    init(
        serviceID: String,
        name: String,
        serviceType: String,
        category: String,
        details: String,
        city: String,
        country: String,
        latitude: Double,
        longitude: Double,
        plusCode: String,
        eAddress: String,
        addressLabel: String = "",
        locality: String = "",
        adminArea: String = "",
        countryCode: String = "",
        phone: String,
        website: String,
        emergencyContact: String,
        urgency: String,
        verified: Bool,
        trustScore: Int,
        sourceTrustTier: Int,
        status: String,
        tagsJSON: String,
        primaryPhotoID: String = "",
        photoCount: Int = 0,
        shareScope: SemayContributionScope = .personal,
        publishState: SemayContributionPublishState = .localOnly,
        qualityScore: Int = 0,
        qualityReasonsJSON: String = "[]",
        reviewVersion: Int = 1,
        lastQualityCheckedAt: Int = 0,
        authorPubkey: String,
        createdAt: Int,
        updatedAt: Int
    ) {
        self.serviceID = serviceID
        self.name = name
        self.serviceType = serviceType
        self.category = category
        self.details = details
        self.city = city
        self.country = country
        self.latitude = latitude
        self.longitude = longitude
        self.plusCode = plusCode
        self.eAddress = eAddress
        self.addressLabel = addressLabel
        self.locality = locality
        self.adminArea = adminArea
        self.countryCode = countryCode
        self.phone = phone
        self.website = website
        self.emergencyContact = emergencyContact
        self.urgency = urgency
        self.verified = verified
        self.trustScore = trustScore
        self.sourceTrustTier = sourceTrustTier
        self.status = status
        self.tagsJSON = tagsJSON
        self.primaryPhotoID = primaryPhotoID
        self.photoCount = max(0, photoCount)
        self.shareScope = shareScope
        self.publishState = publishState
        self.qualityScore = max(0, min(100, qualityScore))
        self.qualityReasonsJSON = qualityReasonsJSON
        self.reviewVersion = max(1, reviewVersion)
        self.lastQualityCheckedAt = max(0, lastQualityCheckedAt)
        self.authorPubkey = authorPubkey
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    enum CodingKeys: String, CodingKey {
        case serviceID = "service_id"
        case name
        case serviceType = "service_type"
        case category
        case details
        case city
        case country
        case latitude
        case longitude
        case plusCode = "plus_code"
        case eAddress = "e_address"
        case addressLabel = "address_label"
        case locality
        case adminArea = "admin_area"
        case countryCode = "country_code"
        case phone
        case website
        case emergencyContact = "emergency_contact"
        case urgency
        case verified
        case trustScore = "trust_score"
        case sourceTrustTier = "source_trust_tier"
        case status
        case tagsJSON = "tags_json"
        case primaryPhotoID = "primary_photo_id"
        case photoCount = "photo_count"
        case shareScope = "share_scope"
        case publishState = "publish_state"
        case qualityScore = "quality_score"
        case qualityReasonsJSON = "quality_reasons"
        case reviewVersion = "review_version"
        case lastQualityCheckedAt = "last_quality_checked_at"
        case authorPubkey = "author_pubkey"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        serviceID = try c.decode(String.self, forKey: .serviceID)
        name = try c.decode(String.self, forKey: .name)
        serviceType = try c.decodeIfPresent(String.self, forKey: .serviceType) ?? "community-service"
        category = try c.decodeIfPresent(String.self, forKey: .category) ?? ""
        details = try c.decodeIfPresent(String.self, forKey: .details) ?? ""
        city = try c.decodeIfPresent(String.self, forKey: .city) ?? ""
        country = try c.decodeIfPresent(String.self, forKey: .country) ?? ""
        latitude = try c.decodeIfPresent(Double.self, forKey: .latitude) ?? 0
        longitude = try c.decodeIfPresent(Double.self, forKey: .longitude) ?? 0
        plusCode = try c.decodeIfPresent(String.self, forKey: .plusCode) ?? ""
        eAddress = try c.decodeIfPresent(String.self, forKey: .eAddress) ?? ""
        addressLabel = try c.decodeIfPresent(String.self, forKey: .addressLabel) ?? ""
        locality = try c.decodeIfPresent(String.self, forKey: .locality) ?? ""
        adminArea = try c.decodeIfPresent(String.self, forKey: .adminArea) ?? ""
        countryCode = try c.decodeIfPresent(String.self, forKey: .countryCode) ?? ""
        phone = try c.decodeIfPresent(String.self, forKey: .phone) ?? ""
        website = try c.decodeIfPresent(String.self, forKey: .website) ?? ""
        emergencyContact = try c.decodeIfPresent(String.self, forKey: .emergencyContact) ?? ""
        urgency = try c.decodeIfPresent(String.self, forKey: .urgency) ?? SemayServiceUrgency.medium.rawValue
        verified = try c.decodeIfPresent(Bool.self, forKey: .verified) ?? false
        trustScore = try c.decodeIfPresent(Int.self, forKey: .trustScore) ?? (verified ? 65 : 40)
        sourceTrustTier = try c.decodeIfPresent(Int.self, forKey: .sourceTrustTier) ?? 0
        status = try c.decodeIfPresent(String.self, forKey: .status) ?? "active"
        tagsJSON = try c.decodeIfPresent(String.self, forKey: .tagsJSON) ?? "[]"
        primaryPhotoID = try c.decodeIfPresent(String.self, forKey: .primaryPhotoID) ?? ""
        photoCount = max(0, try c.decodeIfPresent(Int.self, forKey: .photoCount) ?? 0)
        shareScope = try c.decodeIfPresent(SemayContributionScope.self, forKey: .shareScope) ?? .personal
        publishState = try c.decodeIfPresent(SemayContributionPublishState.self, forKey: .publishState) ?? .localOnly
        qualityScore = max(0, min(100, try c.decodeIfPresent(Int.self, forKey: .qualityScore) ?? 0))
        if let qualityReasonsJSONString = try c.decodeIfPresent(String.self, forKey: .qualityReasonsJSON) {
            qualityReasonsJSON = qualityReasonsJSONString
        } else if let qualityReasonsArray = try c.decodeIfPresent([String].self, forKey: .qualityReasonsJSON),
                  let data = try? JSONEncoder().encode(qualityReasonsArray),
                  let encoded = String(data: data, encoding: .utf8) {
            qualityReasonsJSON = encoded
        } else {
            qualityReasonsJSON = "[]"
        }
        reviewVersion = max(1, try c.decodeIfPresent(Int.self, forKey: .reviewVersion) ?? 1)
        lastQualityCheckedAt = max(0, try c.decodeIfPresent(Int.self, forKey: .lastQualityCheckedAt) ?? 0)
        authorPubkey = try c.decodeIfPresent(String.self, forKey: .authorPubkey) ?? ""
        createdAt = try c.decodeIfPresent(Int.self, forKey: .createdAt) ?? Int(Date().timeIntervalSince1970)
        updatedAt = try c.decodeIfPresent(Int.self, forKey: .updatedAt) ?? createdAt
    }
}

struct BulletinPost: Codable, Identifiable, Equatable {
    let bulletinID: String
    var title: String
    var category: BulletinCategory
    var body: String
    var phone: String
    var latitude: Double
    var longitude: Double
    var plusCode: String
    var eAddress: String
    var authorPubkey: String
    let createdAt: Int
    var updatedAt: Int

    var id: String { bulletinID }

    enum CodingKeys: String, CodingKey {
        case bulletinID = "bulletin_id"
        case title
        case category
        case body
        case phone
        case latitude
        case longitude
        case plusCode = "plus_code"
        case eAddress = "e_address"
        case authorPubkey = "author_pubkey"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }

    init(
        bulletinID: String,
        title: String,
        category: BulletinCategory,
        body: String,
        phone: String = "",
        latitude: Double,
        longitude: Double,
        plusCode: String = "",
        eAddress: String,
        authorPubkey: String,
        createdAt: Int,
        updatedAt: Int
    ) {
        self.bulletinID = bulletinID
        self.title = title
        self.category = category
        self.body = body
        self.phone = phone
        self.latitude = latitude
        self.longitude = longitude
        self.plusCode = plusCode
        self.eAddress = eAddress
        self.authorPubkey = authorPubkey
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        bulletinID = try c.decode(String.self, forKey: .bulletinID)
        title = try c.decode(String.self, forKey: .title)
        category = try c.decodeIfPresent(BulletinCategory.self, forKey: .category) ?? .general
        body = try c.decode(String.self, forKey: .body)
        phone = try c.decodeIfPresent(String.self, forKey: .phone) ?? ""
        latitude = try c.decodeIfPresent(Double.self, forKey: .latitude) ?? 0
        longitude = try c.decodeIfPresent(Double.self, forKey: .longitude) ?? 0
        plusCode = try c.decodeIfPresent(String.self, forKey: .plusCode) ?? ""
        eAddress = try c.decode(String.self, forKey: .eAddress)
        authorPubkey = try c.decode(String.self, forKey: .authorPubkey)
        createdAt = try c.decode(Int.self, forKey: .createdAt)
        updatedAt = try c.decode(Int.self, forKey: .updatedAt)
    }
}
