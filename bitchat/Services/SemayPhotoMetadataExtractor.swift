import CoreLocation
import Foundation
import ImageIO

enum SemayPhotoMetadataExtractor {
    struct GPSCandidate: Equatable {
        let latitude: Double
        let longitude: Double

        var coordinate: CLLocationCoordinate2D {
            CLLocationCoordinate2D(latitude: latitude, longitude: longitude)
        }
    }

    static func extractGPS(from imageData: Data) -> GPSCandidate? {
        guard let source = CGImageSourceCreateWithData(imageData as CFData, nil),
              let metadata = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [CFString: Any],
              let gps = metadata[kCGImagePropertyGPSDictionary] as? [CFString: Any] else {
            return nil
        }

        guard let latitude = signedCoordinate(
            value: gps[kCGImagePropertyGPSLatitude],
            ref: gps[kCGImagePropertyGPSLatitudeRef] as? String,
            positiveRef: "N",
            negativeRef: "S"
        ),
        let longitude = signedCoordinate(
            value: gps[kCGImagePropertyGPSLongitude],
            ref: gps[kCGImagePropertyGPSLongitudeRef] as? String,
            positiveRef: "E",
            negativeRef: "W"
        ),
        abs(latitude) <= 90,
        abs(longitude) <= 180 else {
            return nil
        }

        return GPSCandidate(latitude: latitude, longitude: longitude)
    }

    private static func signedCoordinate(
        value: Any?,
        ref: String?,
        positiveRef: String,
        negativeRef: String
    ) -> Double? {
        guard let numeric = value as? NSNumber else { return nil }
        let raw = numeric.doubleValue
        let normalizedRef = (ref ?? "").uppercased()
        if normalizedRef == negativeRef {
            return -abs(raw)
        }
        if normalizedRef == positiveRef || normalizedRef.isEmpty {
            return abs(raw)
        }
        return nil
    }
}
