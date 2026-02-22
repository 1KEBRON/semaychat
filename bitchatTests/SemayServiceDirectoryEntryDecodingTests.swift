import Foundation
import Testing
@testable import bitchat

struct SemayServiceDirectoryEntryDecodingTests {
    @Test func legacyPayloadDecodesWithDefaults() throws {
        let legacyJSON = """
        {
          "service_id": "svc-1",
          "name": "Clinic One",
          "service_type": "clinic",
          "category": "health",
          "details": "24/7",
          "city": "Asmara",
          "country": "Eritrea",
          "latitude": 15.32,
          "longitude": 38.92,
          "plus_code": "7G7W8WFG+52",
          "e_address": "E-SF-EXJ-Tana",
          "phone": "+291700000",
          "website": "",
          "emergency_contact": "",
          "urgency": "high",
          "verified": true,
          "trust_score": 88,
          "source_trust_tier": 2,
          "status": "active",
          "tags_json": "[\\"clinic\\"]",
          "author_pubkey": "abc",
          "created_at": 10,
          "updated_at": 20
        }
        """

        let entry = try JSONDecoder().decode(SemayServiceDirectoryEntry.self, from: Data(legacyJSON.utf8))
        #expect(entry.addressLabel.isEmpty)
        #expect(entry.locality.isEmpty)
        #expect(entry.adminArea.isEmpty)
        #expect(entry.countryCode.isEmpty)
        #expect(entry.shareScope == .personal)
        #expect(entry.publishState == .localOnly)
        #expect(entry.qualityScore == 0)
        #expect(entry.reviewVersion == 1)
        #expect(entry.serviceID == "svc-1")
        #expect(entry.name == "Clinic One")
    }

    @Test func photoHelperFieldsDecodeWhenPresent() throws {
        let json = """
        {
          "service_id": "svc-2",
          "name": "Asmara Market",
          "service_type": "store",
          "category": "shop",
          "details": "Groceries",
          "city": "Asmara",
          "country": "Eritrea",
          "latitude": 15.3229,
          "longitude": 38.9251,
          "plus_code": "7G7W8WFG+52",
          "e_address": "E-ER-ASM-1A2",
          "phone": "",
          "website": "",
          "emergency_contact": "",
          "urgency": "low",
          "verified": false,
          "trust_score": 40,
          "source_trust_tier": 0,
          "status": "active",
          "tags_json": "[]",
          "primary_photo_id": "photo-123",
          "photo_count": 2,
          "share_scope": "network",
          "publish_state": "pending_review",
          "quality_score": 82,
          "quality_reasons": "[\\"photo_resolution_low\\"]",
          "review_version": 3,
          "last_quality_checked_at": 333,
          "author_pubkey": "abc",
          "created_at": 100,
          "updated_at": 200
        }
        """

        let entry = try JSONDecoder().decode(SemayServiceDirectoryEntry.self, from: Data(json.utf8))
        #expect(entry.primaryPhotoID == "photo-123")
        #expect(entry.photoCount == 2)
        #expect(entry.shareScope == .network)
        #expect(entry.publishState == .pendingReview)
        #expect(entry.qualityScore == 82)
        #expect(entry.qualityReasons == ["photo_resolution_low"])
        #expect(entry.reviewVersion == 3)
        #expect(entry.lastQualityCheckedAt == 333)
    }
}
