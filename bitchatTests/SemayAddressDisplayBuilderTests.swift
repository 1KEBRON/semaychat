import Foundation
import Testing
@testable import bitchat

struct SemayAddressDisplayBuilderTests {
    @Test func buildUsesGazetteerForAsmara() {
        let address = SemayAddress.eAddress(latitude: 15.3229, longitude: 38.9251)
        let display = SemayAddressDisplayBuilder.build(
            nameHint: "Asmara Market",
            latitude: 15.3229,
            longitude: 38.9251,
            plusCode: address.plusCode,
            eAddress: address.eAddress
        )

        #expect(display.locality == "Asmara")
        #expect(display.countryCode == "ER")
        #expect(display.adminArea == "Maekel")
        #expect(display.addressLabel.contains(address.plusCode))
        #expect(display.addressLabel.contains(address.eAddress))
    }

    @Test func buildFallsBackToDeterministicAddress() {
        let address = SemayAddress.eAddress(latitude: 0.0, longitude: 0.0)
        let display = SemayAddressDisplayBuilder.build(
            nameHint: "Unknown Place",
            latitude: 0.0,
            longitude: 0.0,
            plusCode: address.plusCode,
            eAddress: address.eAddress
        )

        #expect(display.locality.isEmpty)
        #expect(display.adminArea.isEmpty)
        #expect(display.countryCode == "ER")
        #expect(display.addressLabel.contains(address.plusCode))
        #expect(display.addressLabel.contains(address.eAddress))
    }
}
