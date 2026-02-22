import Foundation

/// Minimal TOFU (trust-on-first-use) store for backbone node signing keys.
///
/// This prevents an attacker from swapping the node signing key after the app has
/// already seen and trusted a node identity once.
final class SemayNodeTrustStore {
    static let shared = SemayNodeTrustStore()

    private let defaults: UserDefaults
    private let keyPrefix = "semay.node.trust.signing_pubkey."

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
    }

    func enforceTrustedSigningPubkey(nodeID: String, pubkeyHex: String) throws {
        let trimmedID = nodeID.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedPub = pubkeyHex.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedID.isEmpty, !trimmedPub.isEmpty else {
            return
        }

        let key = keyPrefix + trimmedID
        if let stored = defaults.string(forKey: key), !stored.isEmpty {
            if stored.lowercased() != trimmedPub.lowercased() {
                throw NSError(
                    domain: "SemayNodeTrustStore",
                    code: 1,
                    userInfo: [NSLocalizedDescriptionKey: "Node identity changed. Refusing to verify packs from an unexpected signing key."]
                )
            }
        } else {
            // Trust on first use.
            defaults.set(trimmedPub, forKey: key)
        }
    }
}

