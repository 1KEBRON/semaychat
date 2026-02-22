import Foundation

/// A minimal node descriptor for pack catalogs and optional pack signature verification.
///
/// The hub may include additional fields; this model intentionally decodes only what the
/// iOS client needs right now.
struct SemayNodeDescriptor: Decodable, Equatable {
    let nodeName: String
    let nodeID: String
    let baseURL: String
    let signingPubkey: String?
    let signingAlg: String?
    let mapLibreEnabled: Bool?

    enum CodingKeys: String, CodingKey {
        case nodeName = "node_name"
        case nodeID = "node_id"
        case baseURL = "base_url"
        case signingPubkey = "signing_pubkey"
        case signingAlg = "signing_alg"
        case mapLibreEnabled = "maplibre_enabled"
    }
}
