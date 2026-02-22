import Foundation
import Testing
@testable import bitchat

struct OpenLocationCodeTests {
    @Test func encodeKnownLocations() {
        #expect(OpenLocationCode.encode(latitude: 15.3229, longitude: 38.9251) == "7G7W8WFG+52")
        #expect(OpenLocationCode.encode(latitude: 8.9806, longitude: 38.7578) == "6GWWXQJ5+64")
        #expect(OpenLocationCode.encode(latitude: 37.4220, longitude: -122.0840) == "849VCWC8+RC")
    }

    @Test func semayAddressIsDeterministicAndStable() {
        let a = SemayAddress.eAddress(latitude: 15.3229, longitude: 38.9251)
        let b = SemayAddress.eAddress(latitude: 15.3229, longitude: 38.9251)
        #expect(a.plusCode == b.plusCode)
        #expect(a.eAddress == b.eAddress)
        #expect(a.plusCode == "7G7W8WFG+52")
        #expect(a.eAddress == "E-SF-EXJ-Tana")
    }
}

