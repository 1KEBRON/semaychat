import XCTest

@testable import bitchat

final class PlusCodeTests: XCTestCase {
    func testEncodeMatchesKnownExample() {
        // Example commonly used in Open Location Code docs.
        let code = OpenLocationCode.encode(latitude: 37.421908, longitude: -122.084681, codeLength: 10)
        XCTAssertEqual(code, "849VCWC8+Q4")
    }

    func testDecodeAreaContainsOriginalPoint() {
        let lat = 37.421908
        let lon = -122.084681
        let code = OpenLocationCode.encode(latitude: lat, longitude: lon, codeLength: 10)
        guard let area = OpenLocationCode.decode(code) else {
            XCTFail("Decode returned nil for a valid full plus code")
            return
        }
        XCTAssertTrue(lat >= area.latitudeLo && lat <= area.latitudeHi)
        XCTAssertTrue(lon >= area.longitudeLo && lon <= area.longitudeHi)
    }

    func testDecodeAcceptsNoSeparator() {
        let codeWithSep = "849VCWC8+R9"
        let codeNoSep = "849VCWC8R9"
        let a1 = OpenLocationCode.decode(codeWithSep)
        let a2 = OpenLocationCode.decode(codeNoSep)
        XCTAssertEqual(a1, a2)
    }
}
