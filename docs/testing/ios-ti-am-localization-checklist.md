# iOS TI/AM Release Localization Checklist

Use this checklist for Semay TestFlight and release-candidate validation in Tigrinya (`ti`) and Amharic (`am`).

## Onboarding and Backup
- [ ] Launch onboarding in `ti`; verify header, backup method, and seed instructions are fully localized.
- [ ] Launch onboarding in `am`; verify header, backup method, and seed instructions are fully localized.
- [ ] Trigger iCloud backup unavailable state; verify no fallback English appears in warning or action text.
- [ ] Complete backup check flow in both locales; confirm error text remains clear and non-corrupt.

## Core Chat Actions
- [ ] Verify tab labels (`Map`, `Chat`, `Business`, `Me`) are localized in both locales.
- [ ] Open chat composer and common actions; confirm buttons and prompts are localized.
- [ ] Confirm strict translation mode is enabled and does not produce low-confidence substitutions for unsupported phrases.
- [ ] Validate unsupported translation path degrades gracefully (no broken glyphs, no mixed transliteration artifacts).

## Business and Listing UX
- [ ] Open listing editor in `ti`; verify title and fields (`name`, `category`, `description`, `phone`, `website`) are localized.
- [ ] Open listing editor in `am`; verify title and fields (`name`, `category`, `description`, `phone`, `website`) are localized.
- [ ] Trigger contribution actions (`share`, `keep personal`, `report`, `retract`) and verify localized status messages.
- [ ] Confirm queue/reject/published states render localized status text for both locales.

## Map Pack Prompts
- [ ] In online mode without full pack, verify install prompt copy is localized and understandable.
- [ ] In offline mode without usable pack, verify map unavailable text is localized and non-misleading.
- [ ] Validate pack install/retry errors in both locales (network unavailable, integrity check failure).

## Regression Sign-off
- [ ] Run `SemayLocalizationCompletenessTests` and confirm required TI/AM keys are present.
- [ ] Run `SemayTranslationServiceTests` and confirm strict mode blocks permissive-only entries.
- [ ] Capture screenshots for onboarding, map prompt, and listing editor in `ti` and `am`.
