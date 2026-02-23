import Foundation

enum SemayTranslationLanguage: String, CaseIterable, Identifiable {
    case english = "en"
    case tigrinya = "ti"
    case amharic = "am"

    var id: String { rawValue }

    var shortCode: String {
        switch self {
        case .english:
            return "EN"
        case .tigrinya:
            return "TI"
        case .amharic:
            return "AM"
        }
    }
}

private let translationEnabledKey = "semay.translation.offline_enabled"
private let translationQualityModeKey = "semay.translation.quality_mode"

enum TranslationQualityMode: String {
    case strict
    case permissive
}

private typealias SemayPhraseBook = [SemayTranslationLanguage: [SemayTranslationLanguage: [String: String]]]

private struct SemayLoadedPhraseBooks {
    let permissive: SemayPhraseBook
    let strict: SemayPhraseBook
}

private struct SemayTranslationBundle: Decodable {
    let version: String?
    let dictionary: [String: [String: [String: String]]]
    let strictDictionary: [String: [String: [String: String]]]?

    enum CodingKeys: String, CodingKey {
        case version
        case dictionary
        case strictDictionary = "strict_dictionary"
    }
}

/// Lightweight offline translation service backed by local starter dictionaries.
final class SemayTranslationService {
    static let shared = SemayTranslationService()

    // Small canonical alias table so common Ethiopic-script words resolve
    // against the starter phrasebook keys.
    private static let canonicalAliases: [String: String] = [
        "ሰላም": "selam",
        "አመሰግናለሁ": "thank you",
        "ኣመሰግናለኹ": "thank you",
        "እባክህ": "please",
        "በጃኻ": "please",
        "ውሃ": "water",
        "ቡና": "coffee",
        "አዎ": "yes",
        "እወ": "yes",
        "አይ": "no",
        "ኣይ": "no",
        "የት": "where",
        "ኣበይ": "where",
        "ስንት": "how much",
        "ክንደይ": "how much",
    ]

    // Eritrea-first default if script heuristics are inconclusive.
    private static let tigrinyaScriptMarkers = ["ኣ", "በጃኻ", "ክንደይ", "ኣበይ", "እወ"]
    private static let amharicScriptMarkers = ["አ", "እባክ", "አመሰግናለሁ", "ስንት", "የት", "እንዴት"]

    private var permissivePhraseBook: SemayPhraseBook = [:]
    private var strictPhraseBook: SemayPhraseBook = [:]
    private var translationEnabled: Bool {
        get {
            if UserDefaults.standard.object(forKey: translationEnabledKey) == nil {
                return true
            }
            return UserDefaults.standard.bool(forKey: translationEnabledKey)
        }
        set {
            UserDefaults.standard.set(newValue, forKey: translationEnabledKey)
        }
    }
    private var qualityMode: TranslationQualityMode {
        get {
            guard let raw = UserDefaults.standard.string(forKey: translationQualityModeKey),
                  let parsed = TranslationQualityMode(rawValue: raw) else {
                return .strict
            }
            return parsed
        }
        set {
            UserDefaults.standard.set(newValue.rawValue, forKey: translationQualityModeKey)
        }
    }

    private init() {
        let loaded = Self.loadStarterBundle()
        permissivePhraseBook = loaded.permissive
        strictPhraseBook = loaded.strict
    }

    func setTranslationEnabled(_ enabled: Bool) {
        translationEnabled = enabled
    }

    func setQualityMode(_ mode: TranslationQualityMode) {
        qualityMode = mode
    }

    func detectLanguage(for text: String) -> SemayTranslationLanguage {
        let containsEthiopic = text.unicodeScalars.contains { scalar in
            (scalar.value >= 0x1200 && scalar.value <= 0x137F) ||
            (scalar.value >= 0x1380 && scalar.value <= 0x139F)
        }
        guard containsEthiopic else {
            return .english
        }

        if Self.tigrinyaScriptMarkers.contains(where: { text.contains($0) }) {
            return .tigrinya
        }
        if Self.amharicScriptMarkers.contains(where: { text.contains($0) }) {
            return .amharic
        }

        let normalized = canonicalizeText(normalize(text))
        let words = Set(
            normalized
                .split(separator: " ")
                .map { String($0) }
                .filter { !$0.isEmpty }
        )

        if languageMatch(against: .tigrinya, words: words) {
            return .tigrinya
        }
        if languageMatch(against: .amharic, words: words) {
            return .amharic
        }

        return .tigrinya
    }

    func availableTargets(for text: String) -> [SemayTranslationLanguage] {
        guard translationEnabled else {
            return []
        }
        let source = detectLanguage(for: text)
        let activePhraseBook = currentPhraseBook()
        guard let destinations = activePhraseBook[source] else { return [] }

        let normalized = normalize(text)
        let canonical = canonicalizeText(normalized)
        return SemayTranslationLanguage.allCases
            .filter { target in
                guard target != source,
                      let sourceTargetBook = destinations[target],
                      !sourceTargetBook.isEmpty else {
                    return false
                }
                if qualityMode == .strict {
                    return hasStrictTranslation(
                        normalizedText: normalized,
                        canonicalText: canonical,
                        sourceTargetBook: sourceTargetBook
                    )
                }
                return true
            }
    }

    func translate(_ text: String, to targetLanguage: SemayTranslationLanguage) -> String? {
        guard translationEnabled else {
            return nil
        }
        let source = detectLanguage(for: text)
        guard source != targetLanguage else { return nil }
        let activePhraseBook = currentPhraseBook()
        guard let sourceToTargets = activePhraseBook[source],
              let sourceTargetBook = sourceToTargets[targetLanguage],
              !sourceTargetBook.isEmpty else {
            return nil
        }

        let normalized = normalize(text)
        if let direct = sourceTargetBook[normalized] {
            return direct
        }
        let canonicalLine = canonicalizeText(normalized)
        if canonicalLine != normalized, let directCanonical = sourceTargetBook[canonicalLine] {
            return directCanonical
        }
        if qualityMode == .strict {
            return nil
        }

        let words = canonicalLine
            .split(separator: " ")
            .map { String($0) }
            .filter { !$0.isEmpty }
        guard !words.isEmpty else {
            return nil
        }

        var translatedWords: [String] = []
        var translatedAny = false
        for word in words {
            if let translated = sourceTargetBook[word] {
                translatedWords.append(translated)
                translatedAny = true
            } else {
                translatedWords.append(word)
            }
        }

        return translatedAny ? translatedWords.joined(separator: " ") : nil
    }

    private func languageMatch(against language: SemayTranslationLanguage, words: Set<String>) -> Bool {
        let directVocabulary = Set(permissivePhraseBook[language]?.values.flatMap { $0.keys } ?? [])
        return !directVocabulary.isDisjoint(with: words)
    }

    private func currentPhraseBook() -> SemayPhraseBook {
        qualityMode == .strict ? strictPhraseBook : permissivePhraseBook
    }

    private func normalize(_ text: String) -> String {
        text
            .lowercased()
            .replacingOccurrences(of: "[\\p{P}\\p{S}]", with: " ", options: .regularExpression)
            .replacingOccurrences(of: "\n", with: " ")
            .split(whereSeparator: { $0.isWhitespace })
            .map { String($0) }
            .joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func canonicalizeText(_ text: String) -> String {
        text
            .split(whereSeparator: { $0.isWhitespace })
            .map { token in
                let normalizedToken = String(token).trimmingCharacters(in: .whitespacesAndNewlines)
                if let aliased = Self.canonicalAliases[normalizedToken] {
                    return aliased
                }
                return normalizedToken
            }
            .filter { !$0.isEmpty }
            .joined(separator: " ")
    }

    private func hasStrictTranslation(
        normalizedText: String,
        canonicalText: String,
        sourceTargetBook: [String: String]
    ) -> Bool {
        if sourceTargetBook[normalizedText] != nil {
            return true
        }
        if canonicalText != normalizedText, sourceTargetBook[canonicalText] != nil {
            return true
        }
        return false
    }

    private static func loadStarterBundle() -> SemayLoadedPhraseBooks {
        guard let url = Bundle.main.url(forResource: "semay-translation-starter", withExtension: "json"),
              let data = try? Data(contentsOf: url),
              let decoded = try? JSONDecoder().decode(SemayTranslationBundle.self, from: data) else {
            return SemayLoadedPhraseBooks(
                permissive: fallbackPermissivePhraseBook(),
                strict: fallbackStrictPhraseBook()
            )
        }

        let permissive = normalizePhraseBook(decoded.dictionary)
        let strict = normalizePhraseBook(decoded.strictDictionary ?? [:])

        let resolvedPermissive = permissive.isEmpty ? fallbackPermissivePhraseBook() : permissive
        let resolvedStrict = strict.isEmpty ? fallbackStrictPhraseBook() : strict
        return SemayLoadedPhraseBooks(permissive: resolvedPermissive, strict: resolvedStrict)
    }

    private static func normalizePhraseBook(_ raw: [String: [String: [String: String]]]) -> SemayPhraseBook {
        var merged: SemayPhraseBook = [:]
        for (sourceLanguage, targetMap) in raw {
            guard let source = SemayTranslationLanguage(rawValue: sourceLanguage) else { continue }

            var normalizedTargets: [SemayTranslationLanguage: [String: String]] = [:]
            for (targetLanguage, entries) in targetMap {
                guard let target = SemayTranslationLanguage(rawValue: targetLanguage) else { continue }
                var normalizedEntries: [String: String] = [:]

                for (sourcePhrase, translatedPhrase) in entries {
                    let key = normalizeForLookup(sourcePhrase)
                    if !key.isEmpty {
                        normalizedEntries[key] = translatedPhrase
                    }
                }

                if !normalizedEntries.isEmpty {
                    normalizedTargets[target] = normalizedEntries
                }
            }

            if !normalizedTargets.isEmpty {
                merged[source] = normalizedTargets
            }
        }

        if merged.isEmpty {
            return [:]
        }
        return merged
    }

    private static func normalizeForLookup(_ text: String) -> String {
        text
            .lowercased()
            .replacingOccurrences(of: "[\\p{P}\\p{S}]", with: " ", options: .regularExpression)
            .split(whereSeparator: { $0.isWhitespace })
            .map { String($0) }
            .joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func fallbackPermissivePhraseBook() -> SemayPhraseBook {
        return [
            .english: [
                .tigrinya: [
                    "hello": "selam",
                    "hi": "selam",
                    "yes": "aye",
                    "no": "aydey",
                    "thank you": "yekenye dehna",
                    "thanks": "yekenye dehna",
                    "please": "temedem",
                    "how much": "zeykrih",
                    "water": "wuha",
                    "coffee": "buna",
                    "where": "kuay?",
                    "where is": "kuay?",
                    "you": "ke",
                    "friend": "masmara"
                ],
                .amharic: [
                    "hello": "selam",
                    "hi": "selam",
                    "yes": "ewi",
                    "no": "ehun",
                    "thank you": "amenyasalalehu",
                    "thanks": "amenyasalalehu",
                    "please": "efendih",
                    "how much": "shemal?",
                    "water": "wih",
                    "coffee": "buna",
                    "where": "inza?",
                    "where is": "and?"
                ]
            ],
            .tigrinya: [
                .english: [
                    "selam": "hello",
                    "yekenye dehna": "thank you",
                    "buna": "coffee",
                    "wuha": "water",
                    "aye": "yes",
                    "aydey": "no",
                    "temedem": "please"
                ],
                .amharic: [
                    "selam": "selam",
                    "yekenye dehna": "aseegn",
                    "buna": "buna",
                    "wuha": "wih"
                ]
            ],
            .amharic: [
                .english: [
                    "selam": "hello",
                    "amenyasalalehu": "thank you",
                    "buna": "coffee",
                    "wih": "water",
                    "ewi": "yes",
                    "ehun": "no",
                    "efendih": "please"
                ],
                .tigrinya: [
                    "selam": "selam",
                    "amenyasalalehu": "yekenye",
                    "buna": "buna",
                    "wih": "wuha"
                ]
            ]
        ]
    }

    private static func fallbackStrictPhraseBook() -> SemayPhraseBook {
        return [
            .english: [
                .tigrinya: [
                    "hello": "ሰላም",
                    "hi": "ሰላም",
                    "yes": "እወ",
                    "no": "ኣይ"
                ],
                .amharic: [
                    "hello": "ሰላም",
                    "hi": "ሰላም",
                    "yes": "አዎ",
                    "no": "አይ",
                    "thank you": "አመሰግናለሁ",
                    "thanks": "አመሰግናለሁ",
                    "please": "እባክህ"
                ]
            ],
            .tigrinya: [
                .english: [
                    "ሰላም": "hello",
                    "እወ": "yes",
                    "ኣይ": "no",
                    "በጃኻ": "please"
                ],
                .amharic: [
                    "ሰላም": "ሰላም",
                    "እወ": "አዎ",
                    "ኣይ": "አይ"
                ]
            ],
            .amharic: [
                .english: [
                    "ሰላም": "hello",
                    "አመሰግናለሁ": "thank you",
                    "እባክህ": "please",
                    "አዎ": "yes",
                    "አይ": "no"
                ],
                .tigrinya: [
                    "ሰላም": "ሰላም",
                    "አዎ": "እወ",
                    "አይ": "ኣይ"
                ]
            ]
        ]
    }
}
