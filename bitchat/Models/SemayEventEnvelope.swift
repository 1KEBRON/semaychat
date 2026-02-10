import Foundation

/// Transport-agnostic envelope for Semay mutating events.
struct SemayEventEnvelope: Codable, Identifiable, Equatable {
    enum EventType: String, Codable, CaseIterable {
        case pinCreate = "pin.create"
        case pinUpdate = "pin.update"
        case pinApproval = "pin.approval"
        case businessRegister = "business.register"
        case promiseCreate = "promise.create"
        case promiseAccept = "promise.accept"
        case promiseReject = "promise.reject"
        case promiseSettle = "promise.settle"
        case chatMessage = "chat.message"
    }

    enum ValidationCategory: String, Codable {
        case protocolInvalid = "protocol_invalid"
        case policyRejected = "policy_rejected"
        case transportFailed = "transport_failed"
    }

    struct ValidationFailure: Equatable {
        let category: ValidationCategory
        let reason: String
    }

    let schemaVersion: String
    let eventID: String
    let eventType: EventType
    let entityID: String
    let authorPubkey: String
    let createdAt: Int
    let lamportClock: UInt64
    let expiresAt: Int?
    let payloadHash: String
    let signature: String
    let payload: [String: String]

    var id: String { eventID }

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case eventID = "event_id"
        case eventType = "event_type"
        case entityID = "entity_id"
        case authorPubkey = "author_pubkey"
        case createdAt = "created_at"
        case lamportClock = "lamport_clock"
        case expiresAt = "expires_at"
        case payloadHash = "payload_hash"
        case signature
        case payload
    }

    init(
        eventType: EventType,
        entityID: String,
        authorPubkey: String,
        lamportClock: UInt64,
        expiresAt: Int? = nil,
        payload: [String: String],
        signature: String
    ) {
        self.schemaVersion = "1.0"
        self.eventID = UUID().uuidString.lowercased()
        self.eventType = eventType
        self.entityID = entityID
        self.authorPubkey = authorPubkey
        self.createdAt = Int(Date().timeIntervalSince1970)
        self.lamportClock = lamportClock
        self.expiresAt = expiresAt
        self.payload = payload
        self.payloadHash = Self.canonicalPayloadHash(payload)
        self.signature = signature
    }

    func validate(now: Int = Int(Date().timeIntervalSince1970)) -> ValidationFailure? {
        guard schemaVersion == "1.0" else {
            return ValidationFailure(category: .protocolInvalid, reason: "schema-version-mismatch")
        }
        guard Self.isValidHex(authorPubkey, expectedLength: 64) else {
            return ValidationFailure(category: .protocolInvalid, reason: "invalid-author-pubkey")
        }
        guard Self.isValidHex(payloadHash, expectedLength: 64) else {
            return ValidationFailure(category: .protocolInvalid, reason: "invalid-payload-hash")
        }
        guard Self.isValidHex(signature, expectedLength: 64) else {
            return ValidationFailure(category: .protocolInvalid, reason: "invalid-signature")
        }
        if let expiry = expiresAt, expiry < now {
            return ValidationFailure(category: .policyRejected, reason: "envelope-expired")
        }
        let recomputed = Self.canonicalPayloadHash(payload)
        guard recomputed == payloadHash else {
            return ValidationFailure(category: .protocolInvalid, reason: "payload-hash-mismatch")
        }
        let expectedSignature = Self.pseudoSign(payloadHash: payloadHash, authorPubkey: authorPubkey)
        guard signature.lowercased() == expectedSignature.lowercased() else {
            return ValidationFailure(category: .protocolInvalid, reason: "signature-mismatch")
        }
        return nil
    }

    static func canonicalPayloadHash(_ payload: [String: String]) -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let data = (try? encoder.encode(payload)) ?? Data()
        return data.sha256Hash().hexEncodedString()
    }

    static func pseudoSign(payloadHash: String, authorPubkey: String) -> String {
        let raw = Data("\(payloadHash):\(authorPubkey)".utf8)
        return raw.sha256Hash().hexEncodedString()
    }

    private static func isValidHex(_ value: String, expectedLength: Int?) -> Bool {
        let isHex = value.range(of: "^[0-9a-fA-F]+$", options: .regularExpression) != nil
        guard isHex else { return false }
        if let expectedLength {
            return value.count == expectedLength
        }
        return true
    }
}
