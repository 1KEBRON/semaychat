import CryptoKit
import Foundation
#if canImport(CloudKit)
import CloudKit
#endif

@MainActor
final class SeedPhraseService: ObservableObject {
    static let shared = SeedPhraseService()

    struct BackupChallenge: Equatable {
        let firstIndex: Int
        let secondIndex: Int
    }

    struct IdentityBackupMetadata: Codable, Equatable {
        let schemaVersion: String
        let phraseFingerprint: String
        let createdAt: Int
        let backupCompleted: Bool

        enum CodingKeys: String, CodingKey {
            case schemaVersion = "schema_version"
            case phraseFingerprint = "phrase_fingerprint"
            case createdAt = "created_at"
            case backupCompleted = "backup_completed"
        }
    }

    struct UserPreferencesSnapshot: Codable, Equatable {
        let safeModeEnabled: Bool
        let readReceiptsEnabled: Bool
        let presenceEnabled: Bool
        let advancedSettingsEnabled: Bool

        enum CodingKeys: String, CodingKey {
            case safeModeEnabled = "safe_mode_enabled"
            case readReceiptsEnabled = "read_receipts_enabled"
            case presenceEnabled = "presence_enabled"
            case advancedSettingsEnabled = "advanced_settings_enabled"
        }
    }

    struct FavoritesSnapshot: Codable, Equatable {
        let favorites: [String]
    }

    private struct BackupEnvelope: Codable {
        let identityMetadata: IdentityBackupMetadata
        let preferences: UserPreferencesSnapshot
        let favorites: FavoritesSnapshot

        enum CodingKeys: String, CodingKey {
            case identityMetadata = "identity_metadata"
            case preferences
            case favorites
        }
    }

    @Published private(set) var hasCompletedBackup: Bool

    private let keychain: KeychainManagerProtocol
    private let service = "chat.bitchat.seed"
    private let phraseKey = "mnemonic-v1"
    private let backupFlagKey = "semay.seed.backup.completed"
    private let cloudRecordType = "SemayBackupV1"
    private let cloudRecordName = "primary"

    private lazy var words: [String] = Self.loadWordlist()

    init(keychain: KeychainManagerProtocol = KeychainManager()) {
        self.keychain = keychain
        self.hasCompletedBackup = UserDefaults.standard.bool(forKey: backupFlagKey)
    }

    func needsOnboarding() -> Bool {
        return phrase() == nil || !hasCompletedBackup
    }

    func phrase() -> String? {
        guard let data = keychain.load(key: phraseKey, service: service),
              let phrase = String(data: data, encoding: .utf8),
              !phrase.isEmpty else {
            return nil
        }
        return phrase
    }

    func getOrCreatePhrase() -> String {
        if let existing = phrase() {
            return existing
        }
        let entropy = Self.randomEntropy(byteCount: 16)
        let generated = Self.entropyToMnemonic(entropy, words: words)
        keychain.save(
            key: phraseKey,
            data: Data(generated.utf8),
            service: service,
            accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        )
        return generated
    }

    func completeBackup() {
        hasCompletedBackup = true
        UserDefaults.standard.set(true, forKey: backupFlagKey)
    }

    func restoreFromPhrase(_ rawPhrase: String) throws {
        let parts = rawPhrase
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
            .split(whereSeparator: \.isWhitespace)
            .map(String.init)

        guard parts.count == 12 else {
            throw NSError(
                domain: "SeedPhraseService",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Seed phrase must be 12 words."]
            )
        }

        let wordSet = Set(words)
        for word in parts {
            if !wordSet.contains(word) {
                throw NSError(
                    domain: "SeedPhraseService",
                    code: 2,
                    userInfo: [NSLocalizedDescriptionKey: "Invalid word: \(word)"]
                )
            }
        }

        guard Self.isValidBIP39Checksum(words: parts, wordlist: words) else {
            throw NSError(
                domain: "SeedPhraseService",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: "Seed phrase checksum is invalid."]
            )
        }

        let normalized = parts.joined(separator: " ")
        keychain.save(
            key: phraseKey,
            data: Data(normalized.utf8),
            service: service,
            accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        )

        completeBackup()
    }

    func resetBackupFlagForTesting() {
        hasCompletedBackup = false
        UserDefaults.standard.set(false, forKey: backupFlagKey)
    }

    func createChallenge() -> BackupChallenge {
        let first = Int.random(in: 1...12)
        var second = Int.random(in: 1...12)
        while second == first {
            second = Int.random(in: 1...12)
        }
        return BackupChallenge(firstIndex: min(first, second), secondIndex: max(first, second))
    }

    func verifyChallenge(_ challenge: BackupChallenge, firstWord: String, secondWord: String) -> Bool {
        guard let phrase = phrase() else { return false }
        let parts = phrase.split(separator: " ").map(String.init)
        guard parts.count == 12 else { return false }

        let lhs = parts[challenge.firstIndex - 1].lowercased()
        let rhs = parts[challenge.secondIndex - 1].lowercased()
        return lhs == firstWord.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            && rhs == secondWord.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    /// Derive deterministic 32-byte seed material from mnemonic for cross-module use.
    func derivedSeedMaterial() -> Data? {
        guard let phrase = phrase() else { return nil }
        let digest = SHA256.hash(data: Data(phrase.utf8))
        return Data(digest)
    }

    func createIdentityBackupMetadata() -> IdentityBackupMetadata? {
        guard let seed = derivedSeedMaterial() else { return nil }
        return IdentityBackupMetadata(
            schemaVersion: "1.0",
            phraseFingerprint: seed.sha256Fingerprint().lowercased(),
            createdAt: Int(Date().timeIntervalSince1970),
            backupCompleted: hasCompletedBackup
        )
    }

    #if canImport(CloudKit)
    func uploadEncryptedBackupToICloud() async throws {
        guard let metadata = createIdentityBackupMetadata() else {
            throw NSError(
                domain: "SeedPhraseService",
                code: 20,
                userInfo: [NSLocalizedDescriptionKey: "Identity seed is unavailable for backup."]
            )
        }

        let defaults = UserDefaults.standard
        let preferences = UserPreferencesSnapshot(
            safeModeEnabled: (defaults.object(forKey: "semay.safe_mode_enabled") as? Bool) ?? true,
            readReceiptsEnabled: (defaults.object(forKey: "semay.read_receipts_enabled") as? Bool) ?? false,
            presenceEnabled: (defaults.object(forKey: "semay.presence_enabled") as? Bool) ?? false,
            advancedSettingsEnabled: (defaults.object(forKey: "semay.settings.advanced") as? Bool) ?? false
        )
        let favorites = FavoritesSnapshot(favorites: [])
        let envelope = BackupEnvelope(identityMetadata: metadata, preferences: preferences, favorites: favorites)

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let plain = try encoder.encode(envelope)

        guard let key = cloudEncryptionKey() else {
            throw NSError(
                domain: "SeedPhraseService",
                code: 21,
                userInfo: [NSLocalizedDescriptionKey: "Unable to derive backup encryption key."]
            )
        }
        let sealed = try ChaChaPoly.seal(plain, using: key)
        let combined = sealed.combined

        let db = CKContainer.default().privateCloudDatabase
        let recordID = CKRecord.ID(recordName: cloudRecordName)
        let record = CKRecord(recordType: cloudRecordType, recordID: recordID)
        record["blob"] = combined as CKRecordValue
        record["schema_version"] = "1.0" as CKRecordValue
        record["updated_at"] = Date() as CKRecordValue
        try await db.save(record)
    }

    @discardableResult
    func restoreEncryptedBackupFromICloud() async throws -> Bool {
        guard let key = cloudEncryptionKey() else {
            throw NSError(
                domain: "SeedPhraseService",
                code: 22,
                userInfo: [NSLocalizedDescriptionKey: "Unable to derive backup decryption key."]
            )
        }

        let db = CKContainer.default().privateCloudDatabase
        let recordID = CKRecord.ID(recordName: cloudRecordName)
        let record = try await db.record(for: recordID)
        guard let blob = record["blob"] as? Data else {
            throw NSError(
                domain: "SeedPhraseService",
                code: 23,
                userInfo: [NSLocalizedDescriptionKey: "iCloud backup blob is missing."]
            )
        }

        let box = try ChaChaPoly.SealedBox(combined: blob)
        let plain = try ChaChaPoly.open(box, using: key)
        let decoded = try JSONDecoder().decode(BackupEnvelope.self, from: plain)

        let currentFingerprint = derivedSeedMaterial()?.sha256Fingerprint().lowercased()
        guard currentFingerprint == decoded.identityMetadata.phraseFingerprint.lowercased() else {
            throw NSError(
                domain: "SeedPhraseService",
                code: 24,
                userInfo: [NSLocalizedDescriptionKey: "Backup identity does not match this seed phrase."]
            )
        }

        let defaults = UserDefaults.standard
        defaults.set(decoded.preferences.safeModeEnabled, forKey: "semay.safe_mode_enabled")
        defaults.set(decoded.preferences.readReceiptsEnabled, forKey: "semay.read_receipts_enabled")
        defaults.set(decoded.preferences.presenceEnabled, forKey: "semay.presence_enabled")
        defaults.set(decoded.preferences.advancedSettingsEnabled, forKey: "semay.settings.advanced")
        if decoded.identityMetadata.backupCompleted {
            completeBackup()
        }

        return true
    }
    #endif

    private func cloudEncryptionKey() -> SymmetricKey? {
        guard let seed = derivedSeedMaterial() else { return nil }
        var material = Data("semay.cloud.backup.v1".utf8)
        material.append(seed)
        let digest = SHA256.hash(data: material)
        return SymmetricKey(data: Data(digest))
    }

    private static func randomEntropy(byteCount: Int) -> Data {
        var bytes = Data(count: byteCount)
        _ = bytes.withUnsafeMutableBytes { ptr in
            SecRandomCopyBytes(kSecRandomDefault, byteCount, ptr.baseAddress!)
        }
        return bytes
    }

    private static func entropyToMnemonic(_ entropy: Data, words: [String]) -> String {
        let entropyBits = entropy.count * 8
        let checksumBits = entropyBits / 32

        let entropyBitString = entropy.map { String($0, radix: 2).leftPadded(to: 8) }.joined()
        let checksumByte = SHA256.hash(data: entropy).withUnsafeBytes { raw -> UInt8 in
            raw.bindMemory(to: UInt8.self).baseAddress!.pointee
        }
        let checksumString = String(checksumByte, radix: 2).leftPadded(to: 8)
        let fullBits = entropyBitString + String(checksumString.prefix(checksumBits))

        var mnemonic: [String] = []
        mnemonic.reserveCapacity(12)

        for i in stride(from: 0, to: fullBits.count, by: 11) {
            let start = fullBits.index(fullBits.startIndex, offsetBy: i)
            let end = fullBits.index(start, offsetBy: 11)
            let chunk = String(fullBits[start..<end])
            let index = Int(chunk, radix: 2) ?? 0
            mnemonic.append(words[index])
        }

        return mnemonic.joined(separator: " ")
    }

    private static func loadWordlist() -> [String] {
        // Try app bundle first (device/runtime)
        if let url = Bundle.main.url(forResource: "bip39_english", withExtension: "txt"),
           let text = try? String(contentsOf: url, encoding: .utf8) {
            let rows = text.split(whereSeparator: \.isNewline).map(String.init)
            if rows.count == 2048 { return rows }
        }

        // Development fallback when running from source.
        let cwd = FileManager.default.currentDirectoryPath
        let devPath = URL(fileURLWithPath: cwd).appendingPathComponent("bitchat/Resources/bip39_english.txt")
        if let text = try? String(contentsOf: devPath, encoding: .utf8) {
            let rows = text.split(whereSeparator: \.isNewline).map(String.init)
            if rows.count == 2048 { return rows }
        }

        // Last resort deterministic placeholder list.
        return (0..<2048).map { "word\($0)" }
    }

    private static func isValidBIP39Checksum(words mnemonic: [String], wordlist: [String]) -> Bool {
        guard mnemonic.count == 12 else { return false }

        var indexByWord: [String: Int] = [:]
        indexByWord.reserveCapacity(wordlist.count)
        for (idx, word) in wordlist.enumerated() {
            indexByWord[word] = idx
        }

        var bits: [UInt8] = []
        bits.reserveCapacity(12 * 11)
        for word in mnemonic {
            guard let index = indexByWord[word] else { return false }
            for shift in stride(from: 10, through: 0, by: -1) {
                bits.append(UInt8((index >> shift) & 1))
            }
        }

        // For 12 words: 128 bits entropy + 4 bits checksum = 132 bits.
        guard bits.count == 132 else { return false }
        let entropyBits = Array(bits[0..<128])
        let checksumBits = Array(bits[128..<132])

        var entropy = Data(count: 16)
        for byteIndex in 0..<16 {
            var value: UInt8 = 0
            for bitIndex in 0..<8 {
                value = (value << 1) | entropyBits[byteIndex * 8 + bitIndex]
            }
            entropy[byteIndex] = value
        }

        let checksumByte = SHA256.hash(data: entropy).withUnsafeBytes { raw -> UInt8 in
            raw.bindMemory(to: UInt8.self).baseAddress!.pointee
        }
        let expected: [UInt8] = [
            UInt8((checksumByte >> 7) & 1),
            UInt8((checksumByte >> 6) & 1),
            UInt8((checksumByte >> 5) & 1),
            UInt8((checksumByte >> 4) & 1),
        ]
        return expected == checksumBits
    }
}

private extension String {
    func leftPadded(to count: Int) -> String {
        guard self.count < count else { return self }
        return String(repeating: "0", count: count - self.count) + self
    }
}
