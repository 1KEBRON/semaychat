import Foundation
import P256K

/// Transport-agnostic envelope for Semay mutating events.
struct SemayEventEnvelope: Codable, Identifiable, Equatable {
    enum EventType: String, Codable, CaseIterable {
        case pinCreate = "pin.create"
        case pinUpdate = "pin.update"
        case pinApproval = "pin.approval"
        case businessRegister = "business.register"
        case businessUpdate = "business.update"
        case bulletinPost = "bulletin.post"
        case routeCuratedCreate = "route.curated.create"
        case routeCuratedUpdate = "route.curated.update"
        case routeCuratedRetract = "route.curated.retract"
        case routeCuratedReport = "route.curated.report"
        case routeCuratedEndorse = "route.curated.endorse"
        case routeCurated = "route.curated"
        case serviceDirectoryCreate = "service.directory.create"
        case serviceDirectoryUpdate = "service.directory.update"
        case serviceDirectoryRetract = "service.directory.retract"
        case serviceDirectoryEndorse = "service.directory.endorse"
        case serviceDirectoryReport = "service.directory.report"
        case promiseCreate = "promise.create"
        case promiseAccept = "promise.accept"
        case promiseReject = "promise.reject"
        case promiseSettle = "promise.settle"
        case chatMessage = "chat.message"
        case chatAck = "chat.ack"

        static let routeLifecycle: Set<EventType> = [
            .routeCurated,
            .routeCuratedCreate,
            .routeCuratedUpdate,
            .routeCuratedRetract,
            .routeCuratedReport,
            .routeCuratedEndorse,
        ]

        static let serviceDirectoryLifecycle: Set<EventType> = [
            .serviceDirectoryCreate,
            .serviceDirectoryUpdate,
            .serviceDirectoryRetract,
            .serviceDirectoryEndorse,
            .serviceDirectoryReport,
        ]
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

    static let currentSchemaVersion = "1.0"

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

    func payloadValue(_ key: String) -> String? {
        payload[key]
    }

    func payloadIntValue(_ key: String) -> Int? {
        Int(payload[key] ?? "")
    }

    func payloadDoubleValue(_ key: String) -> Double? {
        Double(payload[key] ?? "")
    }

    func payloadBoolValue(_ key: String) -> Bool? {
        guard let raw = payload[key]?.lowercased() else { return nil }
        if raw == "1" || raw == "true" || raw == "yes" { return true }
        if raw == "0" || raw == "false" || raw == "no" { return false }
        return nil
    }

    private struct SigningPayload: Codable {
        let schemaVersion: String
        let eventID: String
        let eventType: EventType
        let entityID: String
        let authorPubkey: String
        let createdAt: Int
        let lamportClock: UInt64
        let expiresAt: Int?
        let payloadHash: String
        let payload: [String: String]

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
            case payload
        }
    }

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
        eventID: String = UUID().uuidString.lowercased(),
        createdAt: Int = Int(Date().timeIntervalSince1970),
        lamportClock: UInt64,
        expiresAt: Int? = nil,
        payload: [String: String],
        payloadHash: String? = nil,
        signature: String
    ) {
        self.schemaVersion = Self.currentSchemaVersion
        self.eventID = eventID
        self.eventType = eventType
        self.entityID = entityID
        self.authorPubkey = authorPubkey
        self.createdAt = createdAt
        self.lamportClock = lamportClock
        self.expiresAt = expiresAt
        self.payload = payload
        self.payloadHash = payloadHash ?? Self.canonicalPayloadHash(payload)
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
        guard Self.isValidHex(signature, expectedLength: nil),
              signature.count == 64 || signature.count == 128 else {
            return ValidationFailure(category: .protocolInvalid, reason: "invalid-signature")
        }
        if let expiry = expiresAt, expiry < now {
            return ValidationFailure(category: .policyRejected, reason: "envelope-expired")
        }
        let recomputed = Self.canonicalPayloadHash(payload)
        guard recomputed == payloadHash else {
            return ValidationFailure(category: .protocolInvalid, reason: "payload-hash-mismatch")
        }

        if signature.count == 128 {
            guard isValidSchnorrSignature() else {
                return ValidationFailure(category: .protocolInvalid, reason: "signature-mismatch")
            }
        } else {
            // Legacy pseudo signature (unsafe). Kept only to allow upgrading old local outbox rows.
            let expectedSignature = Self.pseudoSign(payloadHash: payloadHash, authorPubkey: authorPubkey)
            guard signature.lowercased() == expectedSignature.lowercased() else {
                return ValidationFailure(category: .protocolInvalid, reason: "signature-mismatch")
            }
        }
        return nil
    }

    func signingHash() -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let signable = SigningPayload(
            schemaVersion: schemaVersion,
            eventID: eventID,
            eventType: eventType,
            entityID: entityID,
            authorPubkey: authorPubkey,
            createdAt: createdAt,
            lamportClock: lamportClock,
            expiresAt: expiresAt,
            payloadHash: payloadHash,
            payload: payload
        )
        let data = (try? encoder.encode(signable)) ?? Data()
        return data.sha256Hash()
    }

    static func signed(
        eventType: EventType,
        entityID: String,
        identity: NostrIdentity,
        lamportClock: UInt64,
        expiresAt: Int? = nil,
        payload: [String: String],
        eventID: String = UUID().uuidString.lowercased(),
        createdAt: Int = Int(Date().timeIntervalSince1970)
    ) throws -> SemayEventEnvelope {
        let author = identity.publicKeyHex.lowercased()
        let payloadHash = canonicalPayloadHash(payload)

        let unsigned = SemayEventEnvelope(
            eventType: eventType,
            entityID: entityID,
            authorPubkey: author,
            eventID: eventID,
            createdAt: createdAt,
            lamportClock: lamportClock,
            expiresAt: expiresAt,
            payload: payload,
            payloadHash: payloadHash,
            signature: ""
        )

        let sig = try schnorrSign(messageHash: unsigned.signingHash(), identity: identity)
        return SemayEventEnvelope(
            eventType: eventType,
            entityID: entityID,
            authorPubkey: author,
            eventID: eventID,
            createdAt: createdAt,
            lamportClock: lamportClock,
            expiresAt: expiresAt,
            payload: payload,
            payloadHash: payloadHash,
            signature: sig
        )
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

    private static func schnorrSign(messageHash: Data, identity: NostrIdentity) throws -> String {
        let key = try identity.schnorrSigningKey()

        var messageBytes = [UInt8](messageHash)
        var auxRand = [UInt8](repeating: 0, count: 32)
        _ = auxRand.withUnsafeMutableBytes { ptr in
            SecRandomCopyBytes(kSecRandomDefault, 32, ptr.baseAddress!)
        }

        let signature = try key.signature(message: &messageBytes, auxiliaryRand: &auxRand)
        return signature.dataRepresentation.hexEncodedString()
    }

    private func isValidSchnorrSignature() -> Bool {
        guard signature.count == 128,
              let sigData = Data(hexString: signature),
              sigData.count == 64,
              let pubData = Data(hexString: authorPubkey),
              pubData.count == 32,
              let sig = try? P256K.Schnorr.SchnorrSignature(dataRepresentation: sigData)
        else {
            return false
        }

        var msg = [UInt8](signingHash())
        let xonly = P256K.Schnorr.XonlyKey(dataRepresentation: pubData)
        return xonly.isValid(sig, for: &msg)
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
