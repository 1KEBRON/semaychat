import Foundation

struct SemayAddressDisplay {
    let addressLabel: String
    let locality: String
    let adminArea: String
    let countryCode: String
    let countryName: String
}

enum SemayAddressDisplayBuilder {
    static func build(
        nameHint: String,
        latitude: Double,
        longitude: Double,
        plusCode: String,
        eAddress: String
    ) -> SemayAddressDisplay {
        let gazetteerMatch = SemayAddressGazetteer.shared.nearest(latitude: latitude, longitude: longitude)
        let locality = gazetteerMatch?.locality ?? ""
        let adminArea = gazetteerMatch?.adminArea ?? ""
        let countryCode = gazetteerMatch?.countryCode.uppercased() ?? "ER"
        let countryName = countryNameFor(code: countryCode)

        let parts = [
            nameHint.trimmingCharacters(in: .whitespacesAndNewlines),
            locality,
            adminArea,
            countryName,
        ].filter { !$0.isEmpty }

        let primary = parts.joined(separator: ", ")
        let addressBits = [
            eAddress.trimmingCharacters(in: .whitespacesAndNewlines),
            plusCode.trimmingCharacters(in: .whitespacesAndNewlines),
        ].filter { !$0.isEmpty }

        let addressLabel: String
        if primary.isEmpty {
            addressLabel = addressBits.joined(separator: " • ")
        } else if addressBits.isEmpty {
            addressLabel = primary
        } else {
            addressLabel = "\(primary) • \(addressBits.joined(separator: " • "))"
        }

        return SemayAddressDisplay(
            addressLabel: addressLabel,
            locality: locality,
            adminArea: adminArea,
            countryCode: countryCode,
            countryName: countryName
        )
    }

    private static func countryNameFor(code: String) -> String {
        switch code.uppercased() {
        case "ER": return "Eritrea"
        case "ET": return "Ethiopia"
        default: return code.uppercased()
        }
    }
}

final class SemayAddressGazetteer {
    static let shared = SemayAddressGazetteer()

    struct Entry: Codable {
        let locality: String
        let adminArea: String
        let countryCode: String
        let latitude: Double
        let longitude: Double
    }

    private let entries: [Entry]

    private init() {
        self.entries = Self.loadEntries()
    }

    func nearest(latitude: Double, longitude: Double) -> Entry? {
        guard latitude != 0 || longitude != 0 else { return nil }
        let maxDistanceKm = 120.0
        return entries
            .map { entry -> (Entry, Double) in
                (entry, haversineKm(lat1: latitude, lon1: longitude, lat2: entry.latitude, lon2: entry.longitude))
            }
            .filter { $0.1 <= maxDistanceKm }
            .min(by: { $0.1 < $1.1 })?
            .0
    }

    private static func loadEntries() -> [Entry] {
        if let url = Bundle.main.url(forResource: "semay-address-gazetteer-er", withExtension: "json"),
           let data = try? Data(contentsOf: url),
           let decoded = try? JSONDecoder().decode([Entry].self, from: data),
           !decoded.isEmpty {
            return decoded
        }

        return [
            .init(locality: "Asmara", adminArea: "Maekel", countryCode: "ER", latitude: 15.3229, longitude: 38.9251),
            .init(locality: "Keren", adminArea: "Anseba", countryCode: "ER", latitude: 15.7779, longitude: 38.4511),
            .init(locality: "Massawa", adminArea: "Northern Red Sea", countryCode: "ER", latitude: 15.6079, longitude: 39.4745),
            .init(locality: "Mendefera", adminArea: "Debub", countryCode: "ER", latitude: 14.8852, longitude: 38.8152),
            .init(locality: "Barentu", adminArea: "Gash-Barka", countryCode: "ER", latitude: 15.1058, longitude: 37.5907),
            .init(locality: "Assab", adminArea: "Southern Red Sea", countryCode: "ER", latitude: 13.0092, longitude: 42.7394),
        ]
    }

    private func haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double) -> Double {
        let radiusKm = 6_371.0
        let dLat = (lat2 - lat1) * .pi / 180
        let dLon = (lon2 - lon1) * .pi / 180
        let a = sin(dLat / 2) * sin(dLat / 2)
            + cos(lat1 * .pi / 180) * cos(lat2 * .pi / 180)
            * sin(dLon / 2) * sin(dLon / 2)
        let c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return radiusKm * c
    }
}
