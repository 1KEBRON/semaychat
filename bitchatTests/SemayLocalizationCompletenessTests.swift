import Foundation
import Testing

struct SemayLocalizationCompletenessTests {
    @Test func tigrinyaAndAmharicCoverReleaseCriticalKeys() throws {
        let catalog = try loadLocalizationCatalog()
        let requiredKeys = [
            "semay.tab.map",
            "semay.tab.chat",
            "semay.tab.business",
            "semay.tab.me",
            "semay.map.search.placeholder",
            "semay.map.safety_alert",
            "semay.map.empty_hint",
            "semay.map.starter_limited",
            "semay.map.install_full_pack_prompt",
            "semay.onboarding.start_using",
            "semay.onboarding.choose_backup",
            "semay.onboarding.backup_method",
            "semay.onboarding.secure_backup",
            "semay.onboarding.write_seed_instructions",
            "semay.common.continue",
            "semay.listing.action.endorse",
            "semay.listing.action.report",
            "semay.listing.action.keep_personal",
            "semay.listing.action.share_network",
            "semay.listing.action.retract",
            "semay.listing.action.edit",
            "semay.listing.share.personal_only",
            "semay.listing.share.queued",
            "semay.listing.share.published",
            "semay.listing.share.rejected",
            "semay.listing.message.queued_for_network",
            "semay.listing.message.share_blocked_generic",
            "semay.listing.message.personal_only",
            "semay.listing.message.report_submitted",
            "semay.listing.editor.title",
            "semay.listing.editor.field.name",
            "semay.listing.editor.field.category",
            "semay.listing.editor.field.details",
            "semay.listing.editor.field.phone",
            "semay.listing.editor.field.website",
        ]

        for key in requiredKeys {
            let ti = localizedValue(catalog: catalog, key: key, locale: "ti")
            let am = localizedValue(catalog: catalog, key: key, locale: "am")
            #expect(!(ti ?? "").isEmpty, "Missing TI translation for \(key)")
            #expect(!(am ?? "").isEmpty, "Missing AM translation for \(key)")
        }
    }

    @Test func releaseCriticalTiAndAmDoNotFallbackToEnglish() throws {
        let catalog = try loadLocalizationCatalog()
        let strictKeys = [
            "semay.tab.map",
            "semay.tab.chat",
            "semay.tab.business",
            "semay.tab.me",
            "semay.map.search.placeholder",
            "semay.map.safety_alert",
            "semay.map.empty_hint",
            "semay.onboarding.start_using",
            "semay.onboarding.choose_backup",
            "semay.common.continue",
            "semay.listing.action.endorse",
            "semay.listing.action.share_network",
            "semay.listing.action.keep_personal",
            "semay.listing.share.personal_only",
            "semay.listing.share.queued",
            "semay.listing.share.published",
            "semay.listing.share.rejected",
            "semay.listing.editor.title",
        ]

        for key in strictKeys {
            let en = localizedValue(catalog: catalog, key: key, locale: "en") ?? ""
            let ti = localizedValue(catalog: catalog, key: key, locale: "ti") ?? ""
            let am = localizedValue(catalog: catalog, key: key, locale: "am") ?? ""
            #expect(!en.isEmpty, "Missing EN baseline for \(key)")
            #expect(!ti.isEmpty && ti != en, "TI fallback detected for \(key)")
            #expect(!am.isEmpty && am != en, "AM fallback detected for \(key)")
        }
    }

    private func loadLocalizationCatalog() throws -> [String: Any] {
        let testsDir = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        let packageRoot = testsDir.deletingLastPathComponent()
        let catalogURL = packageRoot
            .appendingPathComponent("bitchat", isDirectory: true)
            .appendingPathComponent("Localizable.xcstrings")

        let data = try Data(contentsOf: catalogURL)
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw NSError(
                domain: "SemayLocalizationCompletenessTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Localizable.xcstrings is not a JSON object"]
            )
        }
        return object
    }

    private func localizedValue(
        catalog: [String: Any],
        key: String,
        locale: String
    ) -> String? {
        guard let strings = catalog["strings"] as? [String: Any],
              let entry = strings[key] as? [String: Any],
              let localizations = entry["localizations"] as? [String: Any],
              let localized = localizations[locale] as? [String: Any],
              let stringUnit = localized["stringUnit"] as? [String: Any],
              let value = stringUnit["value"] as? String else {
            return nil
        }
        return value.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
