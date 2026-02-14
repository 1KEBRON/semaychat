import Foundation
import SwiftUI

protocol SemayModule {
    var moduleID: String { get }
    var tabTitle: String { get }
    func rootView() -> AnyView
    func handle(_ envelope: SemayEventEnvelope) async
}

struct SemayPackCoverage: Codable, Equatable {
    let bbox: [Double]
    let locales: [String]
}

struct SemayPackManifest: Codable, Equatable {
    let packID: String
    let packType: String
    let version: String
    let sizeBytes: UInt64
    let sha256: String
    let signature: String?
    let sigAlg: String?
    let url: String
    let minAppVersion: String?
    let coverage: SemayPackCoverage?
    let items: [[String: String]]?

    enum CodingKeys: String, CodingKey {
        case packID = "pack_id"
        case packType = "pack_type"
        case version
        case sizeBytes = "size_bytes"
        case sha256
        case signature
        case sigAlg = "sig_alg"
        case url
        case minAppVersion = "min_app_version"
        case coverage
        case items
    }
}

protocol SemayDataPackEngine {
    var packType: String { get }
    func canOpen(manifest: SemayPackManifest) -> Bool
    func load(manifest: SemayPackManifest, at localURL: URL) throws
}
