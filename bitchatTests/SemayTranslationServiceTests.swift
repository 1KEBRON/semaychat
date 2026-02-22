import Testing
@testable import bitchat

@Suite(.serialized)
struct SemayTranslationServiceTests {
    @Test func englishStarterPhraseTranslates() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.strict)

        let translation = service.translate("hello", to: .tigrinya)
        #expect(translation != nil)
        #expect(!(translation ?? "").isEmpty)
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
        #expect(translation != nil)
        #expect(!(translation ?? "").isEmpty)
    }

    @Test func strictModeRejectsLowConfidenceWordByWordFallback() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.strict)

        let translation = service.translate("hello water", to: .tigrinya)
        #expect(translation == nil)
    }

    @Test func permissiveModeAllowsWordByWordFallback() {
        let service = SemayTranslationService.shared
        service.setTranslationEnabled(true)
        service.setQualityMode(.permissive)

        let translation = service.translate("hello water", to: .tigrinya)
        #expect(translation != nil)
    }
}
