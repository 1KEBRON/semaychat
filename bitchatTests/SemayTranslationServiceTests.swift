import Testing
@testable import bitchat

@Suite(.serialized)
struct SemayTranslationServiceTests {
    @Test func englishCuratedPhraseTranslatesInStrictMode() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.strict)

        let translation = service.translate("hello", to: .tigrinya)
        #expect(translation == "ሰላም")
    }

    @Test func ethiopicAliasTranslatesToEnglish() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.strict)

        let translation = service.translate("ሰላም።", to: .english)
        #expect(translation == "hello")
    }

    @Test func punctuationNormalizationStillTranslates() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.strict)

        let translation = service.translate("THANKS!!!", to: .amharic)
        #expect(translation == "አመሰግናለሁ")
    }

    @Test func strictModeRejectsLowConfidenceWordByWordFallback() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.strict)

        let translation = service.translate("hello water", to: .tigrinya)
        #expect(translation == nil)
    }

    @Test func strictModeRejectsPermissiveOnlyDirectEntry() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.strict)

        let translation = service.translate("bus stop", to: .tigrinya)
        #expect(translation == nil)
    }

    @Test func strictModeHidesPermissiveOnlyAvailableTargets() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.strict)

        let targets = service.availableTargets(for: "bus stop")
        #expect(targets.isEmpty)
    }

    @Test func permissiveModeAllowsWordByWordFallback() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.permissive)

        let translation = service.translate("hello water", to: .tigrinya)
        #expect(translation != nil)
    }
}
