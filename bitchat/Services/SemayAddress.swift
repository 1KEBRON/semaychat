import Foundation

enum SemayAddress {
    static func plusCode(latitude: Double, longitude: Double) -> String {
        OpenLocationCode.encode(latitude: latitude, longitude: longitude, codeLength: 10)
    }

    static func eAddress(latitude: Double, longitude: Double) -> (plusCode: String, eAddress: String) {
        let geohash = Geohash.encode(latitude: latitude, longitude: longitude, precision: 6)
        let plus = plusCode(latitude: latitude, longitude: longitude)
        return (plus, eAddress(geohash: geohash, plusCode: plus))
    }

    static func eAddress(geohash: String, plusCode: String) -> String {
        let sector = String(geohash.prefix(2)).uppercased()
        let locality = String(geohash.dropFirst(2).prefix(3)).uppercased()
        let words = [
            "Acacia", "Asmara", "Dahlak", "FigTree", "Highland", "Meskel",
            "Nile", "RedSea", "Semien", "Tana", "Walia", "Zoba"
        ]

        let stableInput = plusCode.isEmpty ? geohash : plusCode
        let digest = Data(stableInput.utf8).sha256Hash()
        let bytes = Array(digest.prefix(4))
        let value = (UInt32(bytes[0]) << 24)
            | (UInt32(bytes[1]) << 16)
            | (UInt32(bytes[2]) << 8)
            | UInt32(bytes[3])
        let checksum = Int(value % UInt32(words.count))

        return "E-\(sector)-\(locality)-\(words[checksum])"
    }
}

