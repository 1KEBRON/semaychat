import Foundation

/// Open Location Code (aka "Plus Code") encoder.
///
/// We only need full, global codes for Semay:
/// - 10 digits with the separator after 8 digits (e.g. "849VCWC8+R9")
/// - No short codes / padding support for MVP.
///
/// Reference: https://github.com/google/open-location-code (algorithm, alphabet, resolutions)
enum OpenLocationCode {
    private static let alphabet = Array("23456789CFGHJMPQRVWX")
    private static let separator: Character = "+"
    private static let separatorPosition = 8

    // Resolutions for the five pairs that make up a 10-digit code.
    // Each pair adds a lat and lon character in base-20.
    private static let pairResolutions: [Double] = [
        20.0,
        1.0,
        0.05,
        0.0025,
        0.000125
    ]

    static func encode(latitude: Double, longitude: Double, codeLength: Int = 10) -> String {
        // We implement full codes only. Enforce an even length between 2 and 10.
        let clampedLen: Int
        if codeLength < 2 {
            clampedLen = 10
        } else if codeLength > 10 {
            clampedLen = 10
        } else if codeLength % 2 != 0 {
            clampedLen = codeLength - 1
        } else {
            clampedLen = codeLength
        }

        // OLC is defined for lat [-90, 90] and lon [-180, 180).
        var lat = max(-90.0, min(90.0, latitude))
        if lat == 90.0 {
            // Avoid encoding an out-of-range value at the north pole.
            lat = 90.0 - 1e-12
        }

        var lon = longitude
        while lon < -180.0 { lon += 360.0 }
        while lon >= 180.0 { lon -= 360.0 }

        // Shift into positive ranges.
        lat += 90.0
        lon += 180.0

        var out: [Character] = []
        out.reserveCapacity(clampedLen + 1) // + separator

        // Guard against floating point rounding pushing an exact boundary just under the threshold
        // (eg 0.002 / 0.000125 becoming 15.999999999 -> floor 15 instead of 16).
        let epsilon = 1e-12

        let pairCount = clampedLen / 2
        for i in 0..<pairCount {
            let placeValue = pairResolutions[i]
            let latDigit = min(19, Int(floor((lat + epsilon) / placeValue)))
            let lonDigit = min(19, Int(floor((lon + epsilon) / placeValue)))

            // Digits are always within 0..<20 for the five-pair encoding.
            out.append(alphabet[latDigit])
            out.append(alphabet[lonDigit])

            lat -= Double(latDigit) * placeValue
            lon -= Double(lonDigit) * placeValue

            if out.count == separatorPosition {
                out.append(separator)
            }
        }

        // Ensure the separator is present even for shorter even lengths.
        if out.count < separatorPosition {
            while out.count < separatorPosition {
                out.append("0")
            }
            out.append(separator)
        } else if out.count == separatorPosition {
            out.append(separator)
        }

        return String(out)
    }
}
