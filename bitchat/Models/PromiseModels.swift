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
        ownerPubkey = try c.decode(String.self, forKey: .ownerPubkey)
        qrPayload = try c.decode(String.self, forKey: .qrPayload)
        createdAt = try c.decode(Int.self, forKey: .createdAt)
        updatedAt = try c.decode(Int.self, forKey: .updatedAt)
    }
}
