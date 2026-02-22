import Foundation

@MainActor
final class SafetyModeService: ObservableObject {
    static let shared = SafetyModeService()

    @Published private(set) var safeModeEnabled: Bool
    @Published private(set) var readReceiptsEnabled: Bool
    @Published private(set) var presenceEnabled: Bool

    private let defaults: UserDefaults
    private let keySafeMode = "semay.safe_mode_enabled"
    private let keyReadReceipts = "semay.read_receipts_enabled"
    private let keyPresence = "semay.presence_enabled"

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults

        let safeModeStored = defaults.object(forKey: keySafeMode) as? Bool
        let readStored = defaults.object(forKey: keyReadReceipts) as? Bool
        let presenceStored = defaults.object(forKey: keyPresence) as? Bool

        // Safe mode is ON by default in MVP.
        self.safeModeEnabled = safeModeStored ?? true
        self.readReceiptsEnabled = readStored ?? false
        self.presenceEnabled = presenceStored ?? false

        normalizeForSafeMode()
    }

    func setSafeModeEnabled(_ enabled: Bool) {
        safeModeEnabled = enabled
        defaults.set(enabled, forKey: keySafeMode)
        normalizeForSafeMode()
    }

    func setReadReceiptsEnabled(_ enabled: Bool) {
        readReceiptsEnabled = enabled
        defaults.set(enabled, forKey: keyReadReceipts)

        if safeModeEnabled && enabled {
            // In safe mode, keep this disabled.
            readReceiptsEnabled = false
            defaults.set(false, forKey: keyReadReceipts)
        }
    }

    func setPresenceEnabled(_ enabled: Bool) {
        presenceEnabled = enabled
        defaults.set(enabled, forKey: keyPresence)

        if safeModeEnabled && enabled {
            // In safe mode, keep this disabled.
            presenceEnabled = false
            defaults.set(false, forKey: keyPresence)
        }
    }

    private func normalizeForSafeMode() {
        if safeModeEnabled {
            if readReceiptsEnabled {
                readReceiptsEnabled = false
                defaults.set(false, forKey: keyReadReceipts)
            }
            if presenceEnabled {
                presenceEnabled = false
                defaults.set(false, forKey: keyPresence)
            }
        }
    }
}
