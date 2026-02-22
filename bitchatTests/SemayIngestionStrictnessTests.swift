import Foundation
import Testing

@testable import bitchat

@MainActor
struct SemayIngestionStrictnessTests {
    private let store = SemayDataStore.shared
    private static let semayContentPrefix = "semay1:"

    @Test func debugDecodeRejectsWrongEventKind() throws {
        let identity = try NostrIdentity.generate()
        let envelope = try makePinCreateEnvelope(identity: identity)
        let event = try makeSemayNostrEvent(
            from: envelope,
            identity: identity,
            eventKind: .ephemeralEvent
        )

        let result = store.debugDecodeSemayEnvelope(from: event, source: "test")
        #expect(result.envelope == nil)
        #expect(result.reason == "protocol-invalid:event-kind")
    }

    @Test func debugDecodeRejectsMalformedPayloadBase64() throws {
        let identity = try NostrIdentity.generate()
        let envelope = try makePinCreateEnvelope(identity: identity)
        let event = try makeSemayNostrEvent(
            from: envelope,
            identity: identity,
            contentMutator: { content in
                content = "\(Self.semayContentPrefix)@@@"
            }
        )

        let result = store.debugDecodeSemayEnvelope(from: event, source: "test")
        #expect(result.envelope == nil)
        #expect(result.reason == "protocol-invalid:base64-decode")
    }

    @Test func debugDecodeRejectsPayloadHashMismatch() throws {
        let identity = try NostrIdentity.generate()
        let envelope = try makePinCreateEnvelope(identity: identity)

        let event = try makeSemayNostrEvent(
            from: envelope,
            identity: identity,
            jsonMutator: { json in
                json["payload_hash"] = "000000000000000000000000000000000000000000000000000000000000000000"
            }
        )

        let result = store.debugDecodeSemayEnvelope(from: event, source: "test")
        #expect(result.envelope == nil)
        #expect(result.reason == "protocol_invalid:invalid-payload-hash")
    }

    @Test func debugApplyRejectsPolicyInvalidPinPayload() throws {
        let identity = try NostrIdentity.generate()
        let invalidEnvelope = try makePinCreateEnvelope(
            identity: identity,
            payloadOverrides: ["name": ""]
        )

        store.debugResetForTests()
        let result = store.debugApplyInboundEnvelopeWithReason(invalidEnvelope)
        #expect(!result.applied)
        #expect(result.reason == "policy-rejected:missing-pin-name")
    }

    @Test func debugApplyRejectsUnknownPinApproval() throws {
        let identity = try NostrIdentity.generate()
        let envelope = SemayEventEnvelope(
            eventType: .pinApproval,
            entityID: "pin:missing",
            authorPubkey: identity.publicKeyHex.lowercased(),
            createdAt: 1_700_000_000,
            lamportClock: 1,
            payload: [
                "pin_id": "missing",
                "approver_pubkey": identity.publicKeyHex.lowercased()
            ],
            signature: "00"
        )
        let signature = SemayEventEnvelope.pseudoSign(payloadHash: envelope.payloadHash, authorPubkey: envelope.authorPubkey)

        let validEnvelope = SemayEventEnvelope(
            eventType: .pinApproval,
            entityID: "pin:missing",
            authorPubkey: identity.publicKeyHex.lowercased(),
            createdAt: 1_700_000_000,
            lamportClock: 1,
            payload: envelope.payload,
            payloadHash: envelope.payloadHash,
            signature: signature
        )

        store.debugResetForTests()
        let result = store.debugApplyInboundEnvelopeWithReason(validEnvelope)
        #expect(!result.applied)
        #expect(result.reason == "policy-rejected:unknown-pin")
    }

    private func makePinCreateEnvelope(
        identity: NostrIdentity,
        payloadOverrides: [String: String] = [:]
    ) throws -> SemayEventEnvelope {
        let payload: [String: String] = [
            "pin_id": "pin-test",
            "name": "Semay Coffee",
            "type": "coffee",
            "details": "Open desk, maps nearby",
            "latitude": "15.3229",
            "longitude": "38.9251"
        ].merging(payloadOverrides) { $1 }

        return try SemayEventEnvelope.signed(
            eventType: .pinCreate,
            entityID: "pin:\(payload["pin_id"]!)",
            identity: identity,
            lamportClock: 1,
            payload: payload,
            createdAt: 1_700_000_000
        )
    }

    private func makeSemayNostrEvent(
        from envelope: SemayEventEnvelope,
        identity: NostrIdentity,
        eventKind: NostrProtocol.EventKind = .textNote,
        jsonMutator: ((inout [String: Any]) -> Void)? = nil,
        contentMutator: (inout String) -> Void = { _ in }
    ) throws -> NostrEvent {
        var json = (try JSONSerialization.jsonObject(
            with: JSONEncoder().encode(envelope)
        ) as? [String: Any]) ?? [:]

        jsonMutator?(&json)
        let envelopeData = try JSONSerialization.data(withJSONObject: json)
        var content = "\(Self.semayContentPrefix)\(Base64URL.encode(envelopeData))"
        contentMutator(&content)

        let event = NostrEvent(
            pubkey: identity.publicKeyHex,
            createdAt: Date(timeIntervalSince1970: TimeInterval(envelope.createdAt)),
            kind: eventKind,
            tags: [
                ["t", "semay-v1"],
                ["event_id", envelope.eventID],
                ["event_type", envelope.eventType.rawValue]
            ],
            content: content
        )
        return try event.sign(with: identity.schnorrSigningKey())
    }
}
