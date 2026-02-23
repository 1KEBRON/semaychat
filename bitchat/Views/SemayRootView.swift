import SwiftUI
import MapKit
#if os(iOS)
import UIKit
import SQLite3
import UniformTypeIdentifiers
#if canImport(MapLibre)
import MapLibre
#endif
#elseif os(macOS)
import AppKit
#endif

private extension View {
    @ViewBuilder
    func semayDisableAutoCaps() -> some View {
        #if os(iOS)
        self.textInputAutocapitalization(.never)
        #else
        self
        #endif
    }

    @ViewBuilder
    func semayDisableAutocorrection() -> some View {
        #if os(iOS)
        self.autocorrectionDisabled()
        #else
        self
        #endif
    }

    @ViewBuilder
    func semayPhoneKeyboard() -> some View {
        #if os(iOS)
        self.keyboardType(.phonePad)
        #else
        self
        #endif
    }
}

private let offlineTileStoreErrorDomain = OfflineTileStore.errorDomain
private let signedPackPolicyErrorCode = OfflineTileStore.signedPackPolicyErrorCode

private func isSignedPackPolicyError(_ error: Error) -> Bool {
    let nsError = error as NSError
    return nsError.domain == offlineTileStoreErrorDomain && nsError.code == signedPackPolicyErrorCode
}

private func userFacingOfflineMapError(_ error: Error) -> String {
    let base = (error as NSError).localizedDescription
    guard isSignedPackPolicyError(error) else { return base }
    return "\(base) Install signed packs from a trusted node, or disable \"Require Signed Offline Packs\" in Me > Node (Advanced)."
}

enum SemayMapSurfaceMode: Equatable {
    case hidden
    case onlineOnly
    case offlineAvailable

    static func resolve(isOnline: Bool, hasUsableOfflinePack: Bool) -> SemayMapSurfaceMode {
        if hasUsableOfflinePack {
            return .offlineAvailable
        }
        return isOnline ? .onlineOnly : .hidden
    }
}

enum SemayMapBaseLayerMode: Equatable {
    case none
    case online
    case offline
}

enum SemayMapStatusBannerMode: Equatable {
    case online
    case offlinePack
    case offlineUnavailable

    static func resolve(isOnline: Bool, hasActiveOfflinePack: Bool) -> SemayMapStatusBannerMode {
        if hasActiveOfflinePack {
            return .offlinePack
        }
        return isOnline ? .online : .offlineUnavailable
    }
}

enum SemayMapInstallPromptPolicy {
    static func canInstallOnlinePack(
        isOnline: Bool,
        hubCatalogReachable: Bool,
        communityPackDownloadAvailable: Bool
    ) -> Bool {
        isOnline && hubCatalogReachable && communityPackDownloadAvailable
    }

    static func canInstallBundledStarter(
        isOnline: Bool,
        canInstallBundledStarterPack: Bool
    ) -> Bool {
        !isOnline && canInstallBundledStarterPack
    }

    static func canShowInstallCTA(
        isOnline: Bool,
        hubCatalogReachable: Bool,
        communityPackDownloadAvailable: Bool,
        canInstallBundledStarterPack: Bool
    ) -> Bool {
        canInstallOnlinePack(
            isOnline: isOnline,
            hubCatalogReachable: hubCatalogReachable,
            communityPackDownloadAvailable: communityPackDownloadAvailable
        ) || canInstallBundledStarter(
            isOnline: isOnline,
            canInstallBundledStarterPack: canInstallBundledStarterPack
        )
    }
}

enum SemayMapBaseLayerPolicy {
    static let minimumOnlineCoverageForOfflinePreference = 0.70

    static func resolve(
        isOnline: Bool,
        isBundledStarterSelected: Bool,
        bestPackCoverageRatio: Double?
    ) -> SemayMapBaseLayerMode {
        if isBundledStarterSelected {
            return isOnline ? .online : .none
        }
        guard let coverage = bestPackCoverageRatio else {
            return isOnline ? .online : .none
        }
        if isOnline && coverage < minimumOnlineCoverageForOfflinePreference {
            return .online
        }
        return .offline
    }
}

private enum SemayMapSurfacePolicy {
    private static let centerLatKey = "semay.map.surface.last_center_lat"
    private static let centerLonKey = "semay.map.surface.last_center_lon"
    private static let spanLatKey = "semay.map.surface.last_span_lat"
    private static let spanLonKey = "semay.map.surface.last_span_lon"

    static let regionChangedNotification = Notification.Name("semay.map.surface.region.changed")
    static let defaultRegion = MKCoordinateRegion(
        center: CLLocationCoordinate2D(latitude: 15.3229, longitude: 38.9251),
        span: MKCoordinateSpan(latitudeDelta: 0.06, longitudeDelta: 0.06)
    )

    static func mode(isOnline: Bool, hasUsableOfflinePack: Bool) -> SemayMapSurfaceMode {
        SemayMapSurfaceMode.resolve(isOnline: isOnline, hasUsableOfflinePack: hasUsableOfflinePack)
    }

    static func loadRegion() -> MKCoordinateRegion {
        let defaults = UserDefaults.standard
        guard defaults.object(forKey: centerLatKey) != nil,
              defaults.object(forKey: centerLonKey) != nil,
              defaults.object(forKey: spanLatKey) != nil,
              defaults.object(forKey: spanLonKey) != nil else {
            return defaultRegion
        }
        return MKCoordinateRegion(
            center: CLLocationCoordinate2D(
                latitude: defaults.double(forKey: centerLatKey),
                longitude: defaults.double(forKey: centerLonKey)
            ),
            span: MKCoordinateSpan(
                latitudeDelta: max(0.0001, defaults.double(forKey: spanLatKey)),
                longitudeDelta: max(0.0001, defaults.double(forKey: spanLonKey))
            )
        )
    }

    static func saveRegion(_ region: MKCoordinateRegion) {
        let defaults = UserDefaults.standard
        defaults.set(region.center.latitude, forKey: centerLatKey)
        defaults.set(region.center.longitude, forKey: centerLonKey)
        defaults.set(region.span.latitudeDelta, forKey: spanLatKey)
        defaults.set(region.span.longitudeDelta, forKey: spanLonKey)
        NotificationCenter.default.post(name: regionChangedNotification, object: nil)
    }
}

struct SemayRootView: View {
    @EnvironmentObject var viewModel: ChatViewModel
    @StateObject private var seedService = SeedPhraseService.shared
    @StateObject private var dataStore = SemayDataStore.shared
    @StateObject private var navigation = SemayNavigationState.shared
    @StateObject private var tileStore = OfflineTileStore.shared
    @StateObject private var reachability = NetworkReachabilityService.shared
    @AppStorage("semay.seed.backup.defer_until") private var backupDeferUntilEpoch: Double = 0

    @State private var selectedTab: Tab = .map
    @State private var showOnboarding = false
    @State private var mapSurfaceMode: SemayMapSurfaceMode = .onlineOnly

    enum Tab {
        case map
        case chat
        case business
        case me
    }

    var body: some View {
        TabView(selection: $selectedTab) {
            if mapSurfaceMode != .hidden {
                SemayMapTabView()
                    .environmentObject(dataStore)
                    .tag(Tab.map)
                    .tabItem {
                        Label(String(localized: "semay.tab.map", defaultValue: "Map"), systemImage: "map")
                    }
            }

            ContentView()
                .environmentObject(viewModel)
                .tag(Tab.chat)
                .tabItem {
                    Label(String(localized: "semay.tab.chat", defaultValue: "Chat"), systemImage: "message")
                }

            SemayBusinessTabView()
                .environmentObject(dataStore)
                .tag(Tab.business)
                .tabItem {
                    Label(String(localized: "semay.tab.business", defaultValue: "Business"), systemImage: "building.2")
                }

            SemayMeTabView()
                .environmentObject(dataStore)
                .environmentObject(seedService)
                .tag(Tab.me)
                .tabItem {
                    Label(String(localized: "semay.tab.me", defaultValue: "Me"), systemImage: "person.crop.circle")
                }
        }
        .onAppear {
            dataStore.refreshAll()
            showOnboarding = shouldShowBackupOnboarding()
            refreshMapSurfaceMode()
        }
        .onChange(of: reachability.isOnline) { _ in
            refreshMapSurfaceMode()
        }
        .onReceive(tileStore.$packs) { _ in
            refreshMapSurfaceMode()
        }
        .onReceive(NotificationCenter.default.publisher(for: SemayMapSurfacePolicy.regionChangedNotification)) { _ in
            refreshMapSurfaceMode()
        }
        .onReceive(NotificationCenter.default.publisher(for: .semayDeepLinkURL)) { notification in
            guard let url = notification.object as? URL else { return }
            handleSemayDeepLink(url)
        }
        .sheet(isPresented: $showOnboarding) {
            SeedBackupOnboardingView(isPresented: $showOnboarding)
                .interactiveDismissDisabled(true)
                .environmentObject(seedService)
        }
        .sheet(item: Binding(
            get: { navigation.inboundEnvelope },
            set: { navigation.inboundEnvelope = $0 }
        )) { envelope in
            SemayPromiseEnvelopeSheet(envelope: envelope)
                .environmentObject(dataStore)
        }
    }

    private func handleSemayDeepLink(_ url: URL) {
        guard url.scheme == "semay" else { return }

        let host = (url.host ?? "").lowercased()
        let parts = url.pathComponents.filter { $0 != "/" }
        guard let first = parts.first, !first.isEmpty else { return }

        switch host {
        case "loc", "plus", "pluscode":
            // Deep link to a plus code location: semay://loc/849VCWC8+R9
            navigation.selectedRouteID = nil
            navigation.selectedServiceID = nil
            if let area = OpenLocationCode.decode(first) {
                navigation.selectedBusinessID = nil
                navigation.selectedPinID = nil
                let zoom = max(0.06, max(area.latitudeSpan, area.longitudeSpan) * 50.0)
                navigation.pendingCenter = .init(latitude: area.centerLatitude, longitude: area.centerLongitude, zoomDelta: zoom)
                navigation.pendingFocus = true
                selectedTab = preferredMapDestinationTab(fallback: .chat)
            }
        case "business":
            navigation.selectedPinID = nil
            navigation.selectedRouteID = nil
            navigation.selectedServiceID = nil
            navigation.selectedBusinessID = first
            navigation.pendingFocus = true
            selectedTab = preferredMapDestinationTab(fallback: .business)
        case "pin", "place":
            navigation.selectedBusinessID = nil
            navigation.selectedRouteID = nil
            navigation.selectedServiceID = nil
            navigation.selectedPinID = first
            navigation.pendingFocus = true
            selectedTab = preferredMapDestinationTab(fallback: .business)
        case "promise", "promise-response":
            guard first.count <= 16_384,
                  let data = Base64URL.decode(first),
                  let envelope = try? JSONDecoder().decode(SemayEventEnvelope.self, from: data) else {
                return
            }
            navigation.inboundEnvelope = envelope
            selectedTab = .business
        default:
            break
        }
    }

    private func shouldShowBackupOnboarding() -> Bool {
        let now = Date().timeIntervalSince1970
        return seedService.needsOnboarding() && now >= backupDeferUntilEpoch
    }

    private func preferredMapDestinationTab(fallback: Tab) -> Tab {
        mapSurfaceMode == .hidden ? fallback : .map
    }

    private func refreshMapSurfaceMode() {
        let region = SemayMapSurfacePolicy.loadRegion()
        var usable = tileStore.hasUsablePack(for: region)
        if tileStore.isBundledStarterSelected {
            usable = false
        }
        mapSurfaceMode = SemayMapSurfacePolicy.mode(isOnline: reachability.isOnline, hasUsableOfflinePack: usable)
        selectedTab = Self.adjustedTabSelection(selectedTab, for: mapSurfaceMode)
    }

    static func adjustedTabSelection(_ selected: Tab, for mode: SemayMapSurfaceMode) -> Tab {
        if mode == .hidden, selected == .map {
            return .chat
        }
        return selected
    }
}

private struct SeedBackupOnboardingView: View {
    private enum BackupMethod: String, CaseIterable, Identifiable {
        case iCloud = "icloud"
        case manual = "manual"

        var id: String { rawValue }
    }

    @EnvironmentObject private var seedService: SeedPhraseService
    @Binding var isPresented: Bool
    @AppStorage("semay.icloud_backup_enabled") private var iCloudBackupEnabled = false
    @AppStorage("semay.seed.backup.defer_until") private var backupDeferUntilEpoch: Double = 0
    @StateObject private var reachability = NetworkReachabilityService.shared

    @State private var phrase: String = ""
    @State private var challenge: SeedPhraseService.BackupChallenge = .init(firstIndex: 2, secondIndex: 9)
    @State private var selectedMethod: BackupMethod = .iCloud
    @State private var alsoVerifySeedWhenUsingICloud = false
    @State private var firstWord = ""
    @State private var secondWord = ""
    @State private var error: String?
    @State private var isSubmitting = false

    private let backupReminderDelaySeconds: TimeInterval = 24 * 60 * 60

    private var iCloudBackupAvailable: Bool {
        seedService.isICloudBackupAvailable()
    }

    private var phraseWords: [String] {
        phrase
            .split(whereSeparator: \.isWhitespace)
            .map(String.init)
    }

    private var numberedPhrase: String {
        guard !phraseWords.isEmpty else { return phrase }
        return phraseWords.enumerated()
            .map { index, word in "\(index + 1). \(word)" }
            .joined(separator: "\n")
    }

    private var normalizedFirstWord: String {
        firstWord.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private var normalizedSecondWord: String {
        secondWord.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private var continueDisabled: Bool {
        if isSubmitting { return true }
        switch selectedMethod {
        case .iCloud:
            if !iCloudBackupAvailable {
                return true
            }
            if !reachability.isOnline {
                return true
            }
            if alsoVerifySeedWhenUsingICloud {
                return normalizedFirstWord.isEmpty || normalizedSecondWord.isEmpty
            }
            return false
        case .manual:
            return normalizedFirstWord.isEmpty || normalizedSecondWord.isEmpty
        }
    }

    var body: some View {
        NavigationStack {
            Form {
                Section(String(localized: "semay.onboarding.start_using", defaultValue: "Start Using Semay")) {
                    Text(String(
                        localized: "semay.onboarding.choose_backup",
                        defaultValue: "Choose a primary backup path. You can still write down and verify the seed even when using iCloud backup."
                    ))
                }

                Section(String(localized: "semay.onboarding.backup_method", defaultValue: "Backup Method")) {
                    Picker(String(localized: "semay.onboarding.method", defaultValue: "Method"), selection: $selectedMethod) {
                        Text(String(localized: "semay.onboarding.method.icloud", defaultValue: "iCloud")).tag(BackupMethod.iCloud)
                        Text(String(localized: "semay.onboarding.method.write_down", defaultValue: "Write It Down")).tag(BackupMethod.manual)
                    }
                    .pickerStyle(.segmented)
                }

                if selectedMethod == .iCloud && iCloudBackupAvailable {
                    Section(String(localized: "semay.onboarding.icloud_backup", defaultValue: "iCloud Backup")) {
                        Text(String(
                            localized: "semay.onboarding.icloud_backup.description",
                            defaultValue: "Encrypted backup is saved to your iCloud private storage. Plaintext seed words are not uploaded."
                        ))
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        if !reachability.isOnline {
                            Text(String(
                                localized: "semay.onboarding.no_internet_switch_manual",
                                defaultValue: "No internet connection. Switch to \"Write It Down\" to continue offline."
                            ))
                                .foregroundStyle(.orange)
                        }

                        Toggle(String(
                            localized: "semay.onboarding.also_verify_seed",
                            defaultValue: "Also write down and verify seed now"
                        ), isOn: $alsoVerifySeedWhenUsingICloud)
                    }

                    if alsoVerifySeedWhenUsingICloud {
                        manualBackupSections
                    }
                } else {
                    manualBackupSections
                }

                if !iCloudBackupAvailable {
                    Section {
                        Text(String(
                            localized: "semay.onboarding.icloud_unavailable_write_down",
                            defaultValue: "iCloud backup is unavailable for this build. Use \"Write It Down\" for now."
                        ))
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                }

                Section {
                    Button(isSubmitting ? "Working..." : continueButtonTitle) {
                        continueOnboarding()
                    }
                    .disabled(continueDisabled)

                    Button(String(
                        localized: "semay.onboarding.skip_remind_tomorrow",
                        defaultValue: "Skip for now (remind me tomorrow)"
                    )) {
                        backupDeferUntilEpoch = Date().addingTimeInterval(backupReminderDelaySeconds).timeIntervalSince1970
                        isPresented = false
                    }
                    .foregroundStyle(.secondary)
                }

                if let error, !error.isEmpty {
                    Section {
                        Text(error)
                            .foregroundStyle(.red)
                    }
                }
            }
            .navigationTitle(String(localized: "semay.onboarding.secure_backup", defaultValue: "Secure Backup"))
            .onAppear {
                phrase = seedService.getOrCreatePhrase()
                challenge = seedService.createChallenge()
                if !iCloudBackupAvailable || !reachability.isOnline {
                    selectedMethod = .manual
                }
            }
            .onChange(of: selectedMethod) { _ in
                error = nil
                if selectedMethod == .iCloud, !iCloudBackupAvailable {
                    selectedMethod = .manual
                    error = String(
                        localized: "semay.onboarding.icloud_unavailable_write_down_short",
                        defaultValue: "iCloud backup is unavailable for this build. Use \"Write It Down\"."
                    )
                }
            }
        }
    }

    private var continueButtonTitle: String {
        switch selectedMethod {
        case .iCloud:
            return String(
                localized: "semay.onboarding.backup_to_icloud_continue",
                defaultValue: "Back Up to iCloud and Continue"
            )
        case .manual:
            return String(localized: "semay.common.continue", defaultValue: "Continue")
        }
    }

    private func continueOnboarding() {
        error = nil

        switch selectedMethod {
        case .iCloud:
            continueWithICloudBackup()
        case .manual:
            continueWithManualBackup()
        }
    }

    private func continueWithManualBackup() {
        guard seedService.verifyChallenge(challenge, firstWord: firstWord, secondWord: secondWord) else {
            error = "Backup check failed. Confirm the exact words for #\(challenge.firstIndex) and #\(challenge.secondIndex)."
            return
        }

        seedService.completeBackup()
        isPresented = false
    }

    private func continueWithICloudBackup() {
        guard iCloudBackupAvailable else {
            error = "iCloud backup is unavailable for this build. Use \"Write It Down\"."
            selectedMethod = .manual
            return
        }

        guard reachability.isOnline else {
            error = "iCloud backup needs an internet connection. Switch to \"Write It Down\" to continue offline."
            return
        }

        if alsoVerifySeedWhenUsingICloud {
            guard seedService.verifyChallenge(challenge, firstWord: firstWord, secondWord: secondWord) else {
                error = "Backup check failed. Confirm the exact words for #\(challenge.firstIndex) and #\(challenge.secondIndex)."
                return
            }
        }

        isSubmitting = true
        Task { @MainActor in
            defer { isSubmitting = false }
            #if canImport(CloudKit)
            do {
                try await seedService.uploadEncryptedBackupToICloud()
                iCloudBackupEnabled = true
                seedService.completeBackup()
                isPresented = false
            } catch let uploadError {
                error = uploadError.localizedDescription
            }
            #else
            error = "iCloud backup is unavailable on this platform."
            #endif
        }
    }

    @ViewBuilder
    private var manualBackupSections: some View {
        Section(String(localized: "semay.onboarding.write_down_seed", defaultValue: "Write Down Seed")) {
            Text(String(
                localized: "semay.onboarding.write_seed_instructions",
                defaultValue: "Write down your 12-word seed exactly as shown. This is your account recovery key."
            ))
            Text(numberedPhrase)
                .font(.system(.body, design: .monospaced))
                .textSelection(.enabled)
        }

        Section(String(localized: "semay.onboarding.backup_check", defaultValue: "Backup Check")) {
            VStack(alignment: .leading, spacing: 6) {
                Text("Word #\(challenge.firstIndex)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                TextField("Enter word #\(challenge.firstIndex)", text: $firstWord)
                    .semayDisableAutoCaps()
                    .semayDisableAutocorrection()
            }

            VStack(alignment: .leading, spacing: 6) {
                Text("Word #\(challenge.secondIndex)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                TextField("Enter word #\(challenge.secondIndex)", text: $secondWord)
                    .semayDisableAutoCaps()
                    .semayDisableAutocorrection()
            }

            Button(String(
                localized: "semay.onboarding.pick_different_check_words",
                defaultValue: "Pick Different Check Words"
            )) {
                challenge = seedService.createChallenge()
                firstWord = ""
                secondWord = ""
                error = nil
            }
        }
    }
}

private struct SemayMapTabView: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.openURL) private var openURL
    @StateObject private var tileStore = OfflineTileStore.shared
    @StateObject private var libraryStore = LibraryPackStore.shared
    @StateObject private var mapEngine = MapEngineCoordinator.shared
    @StateObject private var reachability = NetworkReachabilityService.shared
    @StateObject private var locationState = LocationStateManager.shared
    @ObservedObject private var navigation = SemayNavigationState.shared
    @AppStorage("semay.settings.advanced") private var advancedSettingsEnabled = false
    @AppStorage("semay.map.country_packs.enabled") private var countryPacksEnabled = false

    @State private var region = MKCoordinateRegion(
        center: CLLocationCoordinate2D(latitude: 15.3229, longitude: 38.9251),
        span: MKCoordinateSpan(latitudeDelta: 0.06, longitudeDelta: 0.06)
    )
    @State private var showAddPin = false
    @State private var editingPin: SemayMapPin?
    @State private var pinEditorCoordinate: CLLocationCoordinate2D?
    @State private var editingBusiness: BusinessProfile?
    @State private var editingService: SemayServiceDirectoryEntry?
    @State private var promisePayBusiness: BusinessProfile?
    @State private var useOSMBaseMap = false
    @State private var useOfflineTiles = false
    @State private var showTileImporter = false
    @State private var tileImportMessage: String?
    @State private var mapActionMessage: String?
    @State private var lastAutoPackPath: String?
    @State private var showExplore = false
    @State private var installingCommunityPack = false
    @AppStorage("semay.map.dismissedOfflineMapBanner") private var dismissedOfflineMapBanner = false
    @State private var showQRScanner = false
    @State private var mapViewportInitialized = false
    @State private var communityPackDownloadAvailable = false
    @State private var hubCatalogReachable = false
    @State private var featuredCountryPacks: [HubTilePack] = []

    private struct SafetyResource: Identifiable {
        let id: String
        let title: String
        let details: String
        let phone: String?
    }

    private var safetyResources: [SafetyResource] {
        var entries: [SafetyResource] = []
        let safetyKeywords = ["hospital", "clinic", "ambulance", "police", "embassy", "consulate", "legal", "safe"]
        for service in dataStore.activeDirectoryServices where service.status == "active" {
            let category = "\(service.serviceType) \(service.category)".lowercased()
            let isMatch = safetyKeywords.contains { keyword in
                category.contains(keyword)
            }
            if !isMatch { continue }

            let phone = if !service.phone.isEmpty {
                service.phone
            } else if !service.emergencyContact.isEmpty {
                service.emergencyContact
            } else {
                ""
            }
            entries.append(
                SafetyResource(
                    id: "\(service.name)-\(service.serviceType)-\(service.city)-\(service.country)".lowercased(),
                    title: service.name,
                    details: "\(service.city), \(service.country) • \(service.serviceType)",
                    phone: phone.isEmpty ? nil : phone
                )
            )
        }

        if entries.isEmpty {
            entries = [
                SafetyResource(
                    id: "safety-checklist",
                    title: "Safety checklist",
                    details: "Stay with trusted contacts and share your location before moving through checkpoints.",
                    phone: nil
                ),
                SafetyResource(
                    id: "local-crisis-support",
                    title: "Local crisis support",
                    details: "Use verified diaspora channels and keep sensitive details off public bulletin streams.",
                    phone: nil
                ),
                SafetyResource(
                    id: "emergency-services",
                    title: "Emergency services",
                    details: "Use country official emergency numbers from local maps or physical signs when available.",
                    phone: nil
                )
            ]
        }

        return entries
    }

    var body: some View {
        mapRootBody
    }

    private var mapRootBody: some View {
        mapRootShell(with: AnyView(mapRootContent))
    }

    @ViewBuilder
    private var mapRootContent: some View {
        ZStack(alignment: .bottom) {
            mapBaseLayer
            mapOverlayPanel
        }
    }

    private func mapRootShell(with content: AnyView) -> AnyView {
        let base = AnyView(
            NavigationStack {
                content
            }
            .navigationTitle("Map")
            .toolbar(content: { mapToolbar })
        )
        let presented = mapRootPresentations(for: base)
        let lifecycleBound = mapRootLifecycleBindings(for: presented)
        let systemBound = mapRootSystemBindings(for: lifecycleBound)
        return mapRootNavigationBindings(for: systemBound)
    }

    private func mapRootPresentations(for shell: AnyView) -> AnyView {
        var view = shell

        view = AnyView(
            view.sheet(isPresented: $showAddPin) {
                AddPinSheet(
                    isPresented: $showAddPin,
                    existingPin: editingPin,
                    initialCoordinate: pinEditorCoordinate
                )
                .environmentObject(dataStore)
            }
        )

        view = AnyView(
            view.sheet(item: $editingBusiness) { business in
                BusinessEditorSheet(existingBusiness: business)
                    .environmentObject(dataStore)
            }
        )

        view = AnyView(
            view.sheet(item: $editingService) { service in
                SemayServiceEditorSheet(existingService: service)
                    .environmentObject(dataStore)
            }
        )

        view = AnyView(
            view.sheet(item: $promisePayBusiness) { business in
                SemayPromiseCreateSheet(business: business)
                    .environmentObject(dataStore)
            }
        )
#if os(iOS)
        view = AnyView(
            view.sheet(isPresented: $showQRScanner) {
                SemayQRScanSheet(isPresented: $showQRScanner)
            }
        )
#endif
        view = AnyView(
            view.sheet(isPresented: $showExplore) {
                SemayExploreSheet(
                    isPresented: $showExplore,
                    region: $region,
                    pins: dataStore.pins,
                    businesses: dataStore.businesses,
                    routes: dataStore.activeCuratedRoutes,
                    services: dataStore.activeDirectoryServices,
                    bulletins: dataStore.visibleBulletins(),
                    libraryStore: libraryStore,
                    selectedPinID: Binding(
                        get: { navigation.selectedPinID },
                        set: { navigation.selectedPinID = $0 }
                    ),
                    selectedBusinessID: Binding(
                        get: { navigation.selectedBusinessID },
                        set: { navigation.selectedBusinessID = $0 }
                    ),
                    selectedRouteID: Binding(
                        get: { navigation.selectedRouteID },
                        set: { navigation.selectedRouteID = $0 }
                    ),
                    selectedServiceID: Binding(
                        get: { navigation.selectedServiceID },
                        set: { navigation.selectedServiceID = $0 }
                    )
                )
            }
        )

        return view
    }

    private func mapRootLifecycleBindings(for shell: AnyView) -> AnyView {
        var view = shell
        view = AnyView(
            view.onAppear {
                tileStore.refresh()
                updateBaseLayerForConnectivity()
                Task {
                    await refreshCommunityPackAvailability()
                }
                if !mapViewportInitialized {
                    initializeMapViewport()
                    mapViewportInitialized = true
                }
                SemayMapSurfacePolicy.saveRegion(region)
            }
        )
        view = AnyView(
            view.onChange(of: reachability.isOnline) { _ in
                updateBaseLayerForConnectivity()
                Task {
                    await refreshCommunityPackAvailability()
                }
            }
        )
        return view
    }

    private func mapRootSystemBindings(for shell: AnyView) -> AnyView {
        var view = shell

        view = AnyView(
            view.alert("Semay", isPresented: Binding(
                get: { mapActionMessage != nil },
                set: { if !$0 { mapActionMessage = nil } }
            )) {
                Button("OK") { mapActionMessage = nil }
            } message: {
                if let mapActionMessage {
                    Text(mapActionMessage)
                }
            }
        )
#if os(iOS)
        view = AnyView(
            view.fileImporter(
                isPresented: $showTileImporter,
                allowedContentTypes: [UTType(filenameExtension: "mbtiles") ?? .item],
                allowsMultipleSelection: false
            ) { result in
                switch result {
                case .success(let urls):
                    guard let url = urls.first else { return }
                    let didAccess = url.startAccessingSecurityScopedResource()
                    defer { if didAccess { url.stopAccessingSecurityScopedResource() } }
                    do {
                        let pack = try tileStore.importPack(from: url)
                        useOfflineTiles = true
                        useOSMBaseMap = false
                        tileImportMessage = "Imported offline tiles: \(pack.name)"
                    } catch {
                        tileImportMessage = "Failed to import tiles: \(userFacingOfflineMapError(error))"
                    }
                case .failure(let error):
                    tileImportMessage = "Failed to import tiles: \(userFacingOfflineMapError(error))"
                }
            }
        )

        view = AnyView(
            view.alert("Offline Map", isPresented: Binding(
                get: { tileImportMessage != nil },
                set: { if !$0 { tileImportMessage = nil } }
            )) {
                Button("OK") { tileImportMessage = nil }
            } message: {
                if let tileImportMessage {
                    Text(tileImportMessage)
                }
            }
        )
#endif
        view = AnyView(view.onChange(of: useOfflineTiles) { newValue in
            if newValue {
                useOSMBaseMap = false
            }
        })
        view = AnyView(view.onChange(of: useOSMBaseMap) { newValue in
            if newValue {
                useOfflineTiles = false
            }
        })
        view = AnyView(view.onChange(of: region.center.latitude) { _ in
            SemayMapSurfacePolicy.saveRegion(region)
            autoSelectPackIfNeeded()
        })
        view = AnyView(view.onChange(of: region.center.longitude) { _ in
            SemayMapSurfacePolicy.saveRegion(region)
            autoSelectPackIfNeeded()
        })
        view = AnyView(view.onChange(of: region.span.latitudeDelta) { _ in
            SemayMapSurfacePolicy.saveRegion(region)
            autoSelectPackIfNeeded()
        })
        view = AnyView(view.onChange(of: region.span.longitudeDelta) { _ in
            SemayMapSurfacePolicy.saveRegion(region)
            autoSelectPackIfNeeded()
        })
        view = AnyView(view.onChange(of: dataStore.pins.count) { _ in
            if !navigation.pendingFocus {
                fitMapToPins()
            }
        })
        return view
    }

    private func mapRootNavigationBindings(for shell: AnyView) -> AnyView {
        var view = shell
        view = AnyView(view.onChange(of: navigation.selectedBusinessID) { _ in
            guard navigation.pendingFocus else { return }
            guard let id = navigation.selectedBusinessID else { return }
            if let b = dataStore.businesses.first(where: { $0.businessID == id }) {
                centerMap(latitude: b.latitude, longitude: b.longitude, zoomDelta: 0.12)
                navigation.pendingFocus = false
            }
        })
        view = AnyView(view.onChange(of: navigation.selectedPinID) { _ in
            guard navigation.pendingFocus else { return }
            guard let id = navigation.selectedPinID else { return }
            if let pin = dataStore.pins.first(where: { $0.pinID == id }) {
                centerMap(latitude: pin.latitude, longitude: pin.longitude, zoomDelta: 0.12)
                navigation.pendingFocus = false
            }
        })
        view = AnyView(view.onChange(of: navigation.selectedRouteID) { _ in
            guard let id = navigation.selectedRouteID,
                  let route = dataStore.activeCuratedRoutes.first(where: { $0.routeID == id }),
                  let first = route.waypoints.first else {
                return
            }
            centerMap(latitude: first.latitude, longitude: first.longitude, zoomDelta: 0.08)
        })
        view = AnyView(view.onChange(of: navigation.selectedServiceID) { _ in
            guard let id = navigation.selectedServiceID,
                  let service = dataStore.activeDirectoryServices.first(where: { $0.serviceID == id }) else {
                return
            }
            if service.latitude != 0 || service.longitude != 0 {
                centerMap(latitude: service.latitude, longitude: service.longitude, zoomDelta: 0.08)
            }
        })
        view = AnyView(view.onChange(of: navigation.pendingCenter) { _ in
            guard navigation.pendingFocus else { return }
            guard let pending = navigation.pendingCenter else { return }
            centerMap(latitude: pending.latitude, longitude: pending.longitude, zoomDelta: pending.zoomDelta)
            navigation.pendingCenter = nil
            navigation.pendingFocus = false
        })
        view = AnyView(view.onChange(of: dataStore.businesses.count) { _ in
            guard navigation.pendingFocus else { return }
            if let id = navigation.selectedBusinessID,
               let b = dataStore.businesses.first(where: { $0.businessID == id }) {
                centerMap(latitude: b.latitude, longitude: b.longitude, zoomDelta: 0.12)
                navigation.pendingFocus = false
            }
        })
        return view
    }

    @ViewBuilder
    private var mapBaseLayer: some View {
        #if os(iOS)
        SemayMapCanvas(
            mapEngine: mapEngine,
            region: $region,
            pins: dataStore.pins,
            selectedPinID: Binding(
                get: { navigation.selectedPinID },
                set: { navigation.selectedPinID = $0 }
            ),
            businesses: dataStore.businesses,
            selectedBusinessID: Binding(
                get: { navigation.selectedBusinessID },
                set: { navigation.selectedBusinessID = $0 }
            ),
            routes: dataStore.activeCuratedRoutes,
            selectedRouteID: Binding(
                get: { navigation.selectedRouteID },
                set: { navigation.selectedRouteID = $0 }
            ),
            services: dataStore.activeDirectoryServices,
            selectedServiceID: Binding(
                get: { navigation.selectedServiceID },
                set: { navigation.selectedServiceID = $0 }
            ),
            useOSMBaseMap: $useOSMBaseMap,
            offlinePacks: tileStore.activePackChain,
            useOfflineTiles: $useOfflineTiles,
            onLongPress: { coordinate in
                editingPin = nil
                pinEditorCoordinate = coordinate
                showAddPin = true
            }
        )
        .ignoresSafeArea(edges: .bottom)
        #else
        Map(coordinateRegion: $region, annotationItems: dataStore.pins) { pin in
            MapAnnotation(coordinate: CLLocationCoordinate2D(latitude: pin.latitude, longitude: pin.longitude)) {
                Button {
                    navigation.selectedPinID = pin.pinID
                } label: {
                    VStack(spacing: 4) {
                        Image(systemName: pin.isVisible ? "mappin.circle.fill" : "mappin.slash.circle.fill")
                            .font(.title2)
                            .foregroundStyle(pin.isVisible ? .green : .orange)
                        Text(pin.name)
                            .font(.caption2)
                            .lineLimit(1)
                            .padding(.horizontal, 6)
                            .padding(.vertical, 3)
                            .background(.thinMaterial)
                            .clipShape(Capsule())
                    }
                }
                .buttonStyle(.plain)
            }
        }
        .ignoresSafeArea(edges: .bottom)
        #endif
    }

    @ViewBuilder
    private var mapOverlayPanel: some View {
        let needsInstall = tileStore.availablePack == nil
        let showOfflineMapBanner = !dismissedOfflineMapBanner && needsInstall
        let showStarterUpgradeBanner = useOfflineTiles
            && tileStore.isBundledStarterSelected
            && !showOfflineMapBanner
            && reachability.isOnline
            && communityPackDownloadAvailable
        let featuredPack = preferredFeaturedCountryPack()
        let featuredInstallLabel = featuredPack.map(countryInstallButtonTitle(for:)) ?? String(localized: "semay.map.install", defaultValue: "Install")
        let canInstallOnlinePackNow = SemayMapInstallPromptPolicy.canInstallOnlinePack(
            isOnline: reachability.isOnline,
            hubCatalogReachable: hubCatalogReachable,
            communityPackDownloadAvailable: communityPackDownloadAvailable
        )
        let canInstallStarterNow = SemayMapInstallPromptPolicy.canInstallBundledStarter(
            isOnline: reachability.isOnline,
            canInstallBundledStarterPack: tileStore.canInstallBundledStarterPack
        )
        let canInstallNow = SemayMapInstallPromptPolicy.canShowInstallCTA(
            isOnline: reachability.isOnline,
            hubCatalogReachable: hubCatalogReachable,
            communityPackDownloadAvailable: communityPackDownloadAvailable,
            canInstallBundledStarterPack: tileStore.canInstallBundledStarterPack
        )
        let engineStatus = mapEngine.effectiveEngine == .maplibre
            ? "Engine: Semay Map"
            : (mapEngine.selectedEngine == .maplibre ? "Engine: MapKit fallback" : "Engine: MapKit")

        #if os(iOS)
        if useOfflineTiles, let pack = tileStore.availablePack {
            VStack(alignment: .leading, spacing: 4) {
                Text(mapSurfaceStatusText())
                    .font(.caption2)
                    .fontWeight(.semibold)
                Text("Offline map: \(pack.name)")
                    .font(.caption2)
                Text(engineStatus)
                    .font(.caption2)
                if tileStore.isBundledStarterSelected {
                    Text(String(
                        localized: "semay.map.starter_limited",
                        defaultValue: "Starter pack coverage is limited. Install full Eritrea pack for normal use."
                    ))
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                Text(pack.attribution)
                    .font(.caption2)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 6)
            .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 8))
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(.leading, 12)
            .padding(.top, 10)
        } else if useOSMBaseMap {
            VStack(alignment: .leading, spacing: 4) {
                Text(mapSurfaceStatusText())
                    .font(.caption2)
                    .fontWeight(.semibold)
                Text(engineStatus)
                    .font(.caption2)
                Text("© OpenStreetMap contributors")
                    .font(.caption2)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.ultraThinMaterial, in: Capsule())
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(.leading, 12)
            .padding(.top, 10)
        } else {
            VStack(alignment: .leading, spacing: 4) {
                Text(mapSurfaceStatusText())
                    .font(.caption2)
                    .fontWeight(.semibold)
                Text(engineStatus)
                    .font(.caption2)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.ultraThinMaterial, in: Capsule())
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(.leading, 12)
            .padding(.top, 10)
        }

        if showOfflineMapBanner {
            HStack(spacing: 10) {
                Image(systemName: reachability.isOnline ? "map" : "wifi.slash")
                    .foregroundStyle(.orange)
                VStack(alignment: .leading, spacing: 2) {
                    Text("Offline map")
                        .font(.caption)
                        .fontWeight(.semibold)
                    Text(
                        !reachability.isOnline
                            ? String(
                                localized: "semay.map.banner.starter_available",
                                defaultValue: "Starter map is available now. Upgrade later when online."
                            )
                            : (communityPackDownloadAvailable
                               ? (reachability.isExpensive
                                  ? String(
                                    localized: "semay.map.banner.install_wifi",
                                    defaultValue: "Install offline map pack (Wi-Fi recommended)."
                                  )
                                  : String(
                                    localized: "semay.map.banner.install_offline",
                                    defaultValue: "Install offline map pack for use without internet."
                                  ))
                               : String(
                                localized: "semay.map.banner.catalog_unavailable",
                                defaultValue: "Offline map catalog is unavailable right now."
                               ))
                    )
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                if canInstallNow {
                    Button(installingCommunityPack ? "Installing..." : featuredInstallLabel) {
                        Task {
                            await installCommunityPack()
                        }
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(installingCommunityPack)
                }

                Button("Continue") {
                    dismissedOfflineMapBanner = true
                }
                .buttonStyle(.bordered)

                if advancedSettingsEnabled, !reachability.isOnline {
                    Button("Import") {
                        showTileImporter = true
                    }
                    .buttonStyle(.bordered)
                }
            }
            .padding(10)
            .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 12))
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
            .padding(.horizontal, 12)
            .padding(.top, 10)
        }
        if showStarterUpgradeBanner {
            HStack(spacing: 10) {
                Image(systemName: "arrow.down.circle")
                    .foregroundStyle(.orange)
                VStack(alignment: .leading, spacing: 2) {
                    Text("Starter map is limited")
                        .font(.caption)
                        .fontWeight(.semibold)
                    Text(String(
                        localized: "semay.map.install_full_pack_prompt",
                        defaultValue: "Install full Eritrea pack for city-wide offline coverage."
                    ))
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button(
                    installingCommunityPack
                        ? "Installing..."
                        : (canInstallOnlinePackNow ? featuredInstallLabel : String(localized: "semay.map.install_full_pack", defaultValue: "Install full pack"))
                ) {
                    Task {
                        await installCommunityPack()
                    }
                }
                .buttonStyle(.borderedProminent)
                .disabled(installingCommunityPack || !canInstallOnlinePackNow)
            }
            .padding(10)
            .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 12))
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
            .padding(.horizontal, 12)
            .padding(.top, 10)
        }
        #endif

        VStack {
            HStack(spacing: 8) {
                Button {
                    showExplore = true
                } label: {
                    HStack(spacing: 8) {
                        Image(systemName: "magnifyingglass")
                        Text(String(
                            localized: "semay.map.search.placeholder",
                            defaultValue: "Search places, businesses, services, routes, bulletins, plus codes"
                        ))
                            .lineLimit(1)
                        Spacer()
                    }
                    .font(.callout)
                    .foregroundStyle(.primary)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 10)
                    .background(.ultraThinMaterial, in: Capsule())
                }
                .buttonStyle(.plain)

                Menu(String(localized: "semay.map.safety_alert", defaultValue: "Safety Alert")) {
                    ForEach(safetyResources) { item in
                        if let phone = item.phone,
                           let url = telURL(for: phone) {
                            Button("Call \(item.title)") {
                                openURL(url)
                            }
                        } else {
                            Button(item.title) {
                                mapActionMessage = item.details
                            }
                        }
                    }
                }
            }
            .padding(.horizontal, 12)
            .padding(.top, (showOfflineMapBanner || showStarterUpgradeBanner) ? 74 : 12)

            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)

        mapSelectionPanel
    }

    @ViewBuilder
    private var mapSelectionPanel: some View {
        if let linkedService = selectedBusinessLinkedService {
            serviceSelectionCard(linkedService)
        } else if let linkedService = selectedPinLinkedService {
            serviceSelectionCard(linkedService)
        } else if let selected = selectedBusiness {
            businessSelectionCard(selected)
        } else if let selected = selectedRoute {
            routeSelectionCard(selected)
        } else if let selected = selectedPin {
            pinSelectionCard(selected)
        } else if let selected = selectedService {
            serviceSelectionCard(selected)
        } else {
            Text(String(
                localized: "semay.map.empty_hint",
                defaultValue: "Tap a pin to view details. Use + to add places."
            ))
                .font(.caption)
                .padding(10)
                .background(.ultraThinMaterial, in: Capsule())
                .padding()
        }
    }

    @ViewBuilder
    private func businessSelectionCard(_ selected: BusinessProfile) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(selected.name).font(.headline)
                Spacer()
                Text("Business")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            if let linkedID = dataStore.linkedServiceID(entityType: "business", entityID: selected.businessID) {
                servicePhotoPreview(serviceID: linkedID)
            }
            Text("\(selected.category) • \(selected.eAddress)")
                .font(.caption)
                .foregroundStyle(.secondary)
            if !selected.plusCode.isEmpty {
                Text(selected.plusCode)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
            Text("Updated \(Date(timeIntervalSince1970: TimeInterval(selected.updatedAt)).formatted(date: .abbreviated, time: .shortened))")
                .font(.caption2)
                .foregroundStyle(.secondary)
            Text(selected.details)
                .font(.subheadline)
                .lineLimit(2)
            if !selected.lightningLink.isEmpty || !selected.cashuLink.isEmpty {
                HStack(spacing: 10) {
                    if !selected.lightningLink.isEmpty {
                        PaymentChipView(paymentType: .lightning(selected.lightningLink))
                    }
                    if !selected.cashuLink.isEmpty {
                        PaymentChipView(paymentType: .cashu(selected.cashuLink))
                    }
                }
            }
            HStack {
                if let tel = telURL(for: selected.phone) {
                    Button("Call") {
                        openURL(tel)
                    }
                    .buttonStyle(.borderedProminent)
                }
                ShareLink(item: businessShareText(selected)) {
                    Text("Share")
                }
                .buttonStyle(.bordered)
                Button("Promise Pay") {
                    promisePayBusiness = selected
                }
                .buttonStyle(.bordered)
                if let linkedID = dataStore.linkedServiceID(entityType: "business", entityID: selected.businessID) {
                    Button("Directory") {
                        navigation.selectedServiceID = linkedID
                        navigation.selectedBusinessID = nil
                    }
                    .buttonStyle(.bordered)
                }
                Button("Directions") {
                    openDirections(latitude: selected.latitude, longitude: selected.longitude, name: selected.name)
                }
                .buttonStyle(.bordered)
                .disabled(selected.latitude == 0 && selected.longitude == 0)
                if dataStore.currentUserPubkey() == selected.ownerPubkey.lowercased() {
                    Button("Update") {
                        editingBusiness = selected
                    }
                    .buttonStyle(.bordered)
                }
                Spacer()
                Button("Close") {
                    navigation.selectedBusinessID = nil
                }
                .buttonStyle(.bordered)
            }
        }
        .padding(12)
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 14))
        .padding()
    }

    @ViewBuilder
    private func routeSelectionCard(_ selected: SemayCuratedRoute) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(selected.title).font(.headline)
                Spacer()
                Text(selected.isSafe ? "🟢 Safe" : "🟠 Caution")
                    .font(.caption)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(.thinMaterial, in: Capsule())
            }
            Text("Route • \(routeTransportLabel(selected.transportType)) • \(selected.city)")
                .font(.caption)
                .foregroundStyle(.secondary)
            if !selected.summary.isEmpty {
                Text(selected.summary)
                    .font(.subheadline)
                    .lineLimit(2)
            }
            Text("From: \(selected.fromLabel) → \(selected.toLabel)")
                .font(.caption)
                .foregroundStyle(.secondary)
            HStack {
                Text("Trust \(selected.trustScore)")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                Text("•")
                    .foregroundStyle(.secondary)
                Text("Reliability \(selected.reliabilityScore)")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                Spacer()
            }
            HStack {
                if !selected.waypoints.isEmpty,
                   let centerLatitude = selected.centerLatitude,
                   let centerLongitude = selected.centerLongitude {
                    Button("Open Map Area") {
                        focus(latitude: centerLatitude, longitude: centerLongitude)
                        navigation.pendingFocus = true
                    }
                    .buttonStyle(.bordered)
                }
                Menu("Trust") {
                    Button("Endorse as Safe") {
                        let trusted = dataStore.endorseCuratedRoute(routeID: selected.routeID, score: 1, reason: "verified")
                        mapActionMessage = trusted
                            ? "Route endorsed as a safer option."
                            : "Could not save endorsement right now."
                    }
                    Button("Report", role: .destructive) {
                        dataStore.reportCuratedRoute(routeID: selected.routeID, reason: "mismatch")
                        mapActionMessage = "Route report submitted."
                    }
                    if dataStore.currentUserPubkey() == selected.authorPubkey.lowercased() {
                        Button("Retract", role: .destructive) {
                            dataStore.retractCuratedRoute(routeID: selected.routeID)
                            navigation.selectedRouteID = nil
                            mapActionMessage = "Route marked retracted."
                        }
                    }
                }
                Button("Close") {
                    navigation.selectedRouteID = nil
                }
                .buttonStyle(.bordered)
            }
        }
        .padding(12)
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 14))
        .padding()
    }

    @ViewBuilder
    private func pinSelectionCard(_ selected: SemayMapPin) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(selected.name).font(.headline)
                Spacer()
                Text(selected.isVisible ? "Visible" : "Pending")
                    .font(.caption)
                    .foregroundStyle(selected.isVisible ? .green : .orange)
            }
            Text("\(selected.type) • \(selected.eAddress)")
                .font(.caption)
                .foregroundStyle(.secondary)
            if !selected.plusCode.isEmpty {
                Text(selected.plusCode)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
            Text("Approvals: \(selected.approvalCount) • Updated \(Date(timeIntervalSince1970: TimeInterval(selected.updatedAt)).formatted(date: .abbreviated, time: .shortened))")
                .font(.caption2)
                .foregroundStyle(.secondary)
            Text(selected.details)
                .font(.subheadline)
                .lineLimit(2)
            HStack {
                Button("I'm Here / Approve") {
                    approvePin(selected)
                }
                .buttonStyle(.borderedProminent)
                ShareLink(item: placeShareText(selected)) {
                    Text("Share")
                }
                .buttonStyle(.bordered)
                if let tel = telURL(for: selected.phone) {
                    Button("Call") {
                        openURL(tel)
                    }
                    .buttonStyle(.bordered)
                }
                Button("Directions") {
                    openDirections(latitude: selected.latitude, longitude: selected.longitude, name: selected.name)
                }
                .buttonStyle(.bordered)
                if let linkedID = dataStore.linkedServiceID(entityType: "pin", entityID: selected.pinID) {
                    Button("Directory") {
                        navigation.selectedServiceID = linkedID
                        navigation.selectedPinID = nil
                    }
                    .buttonStyle(.bordered)
                }
                Button("Update") {
                    editingPin = selected
                    pinEditorCoordinate = nil
                    showAddPin = true
                }
                .buttonStyle(.bordered)
                Spacer()
                Button("Close") {
                    navigation.selectedPinID = nil
                }
                .buttonStyle(.bordered)
            }
        }
        .padding(12)
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 14))
        .padding()
    }

    @ViewBuilder
    private func serviceSelectionCard(_ selected: SemayServiceDirectoryEntry) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(selected.name).font(.headline)
                Spacer()
                Text(selected.urgency.uppercased())
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            servicePhotoPreview(serviceID: selected.serviceID)
            Text("\(selected.serviceType) • \(selected.category)")
                .font(.caption)
                .foregroundStyle(.secondary)
            if selected.verified {
                Text("Verified service")
                    .font(.caption2)
                    .foregroundStyle(.green)
            }
            if !selected.addressLabel.isEmpty {
                Text(selected.addressLabel)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            if !selected.city.isEmpty || !selected.country.isEmpty {
                Text([selected.city, selected.country].filter { !$0.isEmpty }.joined(separator: ", "))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Text(selected.trustBadge)
                .font(.caption)
            Text(shareStatusText(for: selected))
                .font(.caption2)
                .padding(.horizontal, 8)
                .padding(.vertical, 3)
                .background(shareStatusTint(for: selected).opacity(0.18), in: Capsule())
                .foregroundStyle(shareStatusTint(for: selected))
            if selected.publishState == .rejected {
                let reasons = qualityReasonList(from: selected.qualityReasonsJSON).map(qualityReasonLabel)
                if !reasons.isEmpty {
                    Text("\(listingString("semay.listing.share.blocked_prefix", "Share blocked")): \(reasons.joined(separator: ", "))")
                        .font(.caption2)
                        .foregroundStyle(.orange)
                        .lineLimit(2)
                }
            }
            if !selected.details.isEmpty {
                Text(selected.details)
                    .font(.subheadline)
                    .lineLimit(2)
            }
            HStack(spacing: 8) {
                if let phoneURL = telURL(for: selected.phone) {
                    Button("Call") {
                        openURL(phoneURL)
                    }
                    .font(.caption)
                    .buttonStyle(.bordered)
                }
                if let url = websiteURL(for: selected.website) {
                    Button("Website") {
                        openURL(url)
                    }
                    .font(.caption)
                    .buttonStyle(.bordered)
                }
            }
            HStack {
                Button(selected.latitude == 0 && selected.longitude == 0 ? "No location yet" : "Open on Map") {
                    if selected.latitude != 0 || selected.longitude != 0 {
                        focus(latitude: selected.latitude, longitude: selected.longitude)
                        navigation.pendingFocus = true
                    }
                }
                .buttonStyle(.borderedProminent)
                .disabled(selected.latitude == 0 && selected.longitude == 0)
                Menu(listingString("semay.listing.menu.trust", "Trust")) {
                    Button(listingString("semay.listing.action.endorse", "Endorse")) {
                        let trusted = dataStore.endorseServiceDirectoryEntry(serviceID: selected.serviceID, score: 1, reason: "verified")
                        mapActionMessage = trusted
                            ? listingString("semay.listing.message.endorsed", "Service endorsed.")
                            : listingString("semay.listing.message.endorse_failed", "Could not save endorsement right now.")
                    }
                    Button(listingString("semay.listing.action.report", "Report"), role: .destructive) {
                        dataStore.reportServiceDirectoryEntry(serviceID: selected.serviceID, reason: "mismatch")
                        mapActionMessage = listingString("semay.listing.message.report_submitted", "Service report submitted.")
                    }
                    if dataStore.currentUserPubkey() == selected.authorPubkey.lowercased() {
                        Button(listingString("semay.listing.action.retract", "Retract"), role: .destructive) {
                            dataStore.retractServiceDirectoryEntry(serviceID: selected.serviceID)
                            navigation.selectedServiceID = nil
                            mapActionMessage = listingString("semay.listing.message.retracted", "Service marked retracted.")
                        }
                    }
                }
                if dataStore.currentUserPubkey() == selected.authorPubkey.lowercased() {
                    Button(listingString("semay.listing.action.edit", "Edit Listing")) {
                        editingService = selected
                    }
                    .buttonStyle(.bordered)
                    Menu(listingString("semay.listing.menu.sharing", "Sharing")) {
                        Button(listingString("semay.listing.action.keep_personal", "Keep Personal")) {
                            keepPersonalOnly(for: selected)
                        }
                        Button(listingString("semay.listing.action.share_network", "Share to Network")) {
                            requestNetworkShare(for: selected)
                        }
                    }
                }
                Button(listingString("semay.listing.action.close", "Close")) {
                    navigation.selectedServiceID = nil
                }
                .buttonStyle(.bordered)
            }
        }
        .padding(12)
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 14))
        .padding()
    }

    @ToolbarContentBuilder
    private var mapToolbar: some ToolbarContent {
        #if os(iOS)
        ToolbarItem(placement: .topBarLeading) {
            Menu("Jump") {
                Button("Asmara") {
                    centerMap(latitude: 15.3229, longitude: 38.9251, zoomDelta: 0.06)
                }
                Button("Addis Ababa") {
                    centerMap(latitude: 8.9806, longitude: 38.7578, zoomDelta: 0.07)
                }
                Button("Fit All Pins") {
                    fitMapToPins()
                }
                if advancedSettingsEnabled {
                    Divider()
                    Menu("Map Engine") {
                        ForEach(MapEngine.allCases) { engine in
                            Button {
                                mapEngine.setPreferredEngine(engine)
                            } label: {
                                if mapEngine.selectedEngine == engine {
                                    Label(engine.label, systemImage: "checkmark")
                                } else {
                                    Text(engine.label)
                                }
                            }
                            .disabled(engine == .maplibre && !mapEngine.mapLibreAllowed)
                        }
                    }
                }
            }
        }
        #else
        ToolbarItem(placement: .navigation) {
            Menu("Jump") {
                Button("Asmara") {
                    centerMap(latitude: 15.3229, longitude: 38.9251, zoomDelta: 0.06)
                }
                Button("Addis Ababa") {
                    centerMap(latitude: 8.9806, longitude: 38.7578, zoomDelta: 0.07)
                }
                Button("Fit All Pins") {
                    fitMapToPins()
                }
                if advancedSettingsEnabled {
                    Divider()
                    Menu("Map Engine") {
                        ForEach(MapEngine.allCases) { engine in
                            Button {
                                mapEngine.setPreferredEngine(engine)
                            } label: {
                                if mapEngine.selectedEngine == engine {
                                    Label(engine.label, systemImage: "checkmark")
                                } else {
                                    Text(engine.label)
                                }
                            }
                            .disabled(engine == .maplibre && !mapEngine.mapLibreAllowed)
                        }
                    }
                }
            }
        }
        #endif
        ToolbarItem(placement: .primaryAction) {
            Button {
                editingPin = nil
                pinEditorCoordinate = region.center
                showAddPin = true
            } label: {
                Image(systemName: "plus")
            }
        }
        #if os(iOS)
        ToolbarItem(placement: .topBarTrailing) {
            Button {
                showQRScanner = true
            } label: {
                Image(systemName: "qrcode.viewfinder")
            }
        }
        #endif
    }

    private var selectedPin: SemayMapPin? {
        guard let id = navigation.selectedPinID else { return nil }
        return dataStore.pins.first(where: { $0.pinID == id })
    }

    private var selectedBusiness: BusinessProfile? {
        guard let id = navigation.selectedBusinessID else { return nil }
        return dataStore.businesses.first(where: { $0.businessID == id })
    }

    private var selectedBusinessLinkedService: SemayServiceDirectoryEntry? {
        guard let business = selectedBusiness else { return nil }
        guard let serviceID = dataStore.linkedServiceID(entityType: "business", entityID: business.businessID) else { return nil }
        return dataStore.activeDirectoryServices.first(where: { $0.serviceID == serviceID })
    }

    private var selectedRoute: SemayCuratedRoute? {
        guard let id = navigation.selectedRouteID else { return nil }
        return dataStore.activeCuratedRoutes.first(where: { $0.routeID == id })
    }

    private var selectedService: SemayServiceDirectoryEntry? {
        guard let id = navigation.selectedServiceID else { return nil }
        return dataStore.activeDirectoryServices.first(where: { $0.serviceID == id })
    }

    private var selectedPinLinkedService: SemayServiceDirectoryEntry? {
        guard let pin = selectedPin else { return nil }
        guard let serviceID = dataStore.linkedServiceID(entityType: "pin", entityID: pin.pinID) else { return nil }
        return dataStore.activeDirectoryServices.first(where: { $0.serviceID == serviceID })
    }

    @ViewBuilder
    private func servicePhotoPreview(serviceID: String) -> some View {
        let refs = dataStore.servicePhotoRefs(serviceID: serviceID)
        if let primary = refs.first(where: { $0.primary }) ?? refs.first {
            HStack(spacing: 10) {
                Group {
                    if let thumbURL = dataStore.servicePhotoThumbURL(serviceID: serviceID, photoID: primary.photoID) {
                        #if os(iOS)
                        if let image = UIImage(contentsOfFile: thumbURL.path) {
                            Image(uiImage: image)
                                .resizable()
                                .scaledToFill()
                        } else {
                            RoundedRectangle(cornerRadius: 8)
                                .fill(.thinMaterial)
                                .overlay {
                                    Image(systemName: "photo")
                                        .foregroundStyle(.secondary)
                                }
                        }
                        #else
                        if let image = NSImage(contentsOf: thumbURL) {
                            Image(nsImage: image)
                                .resizable()
                                .scaledToFill()
                        } else {
                            RoundedRectangle(cornerRadius: 8)
                                .fill(.thinMaterial)
                                .overlay {
                                    Image(systemName: "photo")
                                        .foregroundStyle(.secondary)
                                }
                        }
                        #endif
                    } else {
                        RoundedRectangle(cornerRadius: 8)
                            .fill(.thinMaterial)
                            .overlay {
                                Image(systemName: "photo")
                                    .foregroundStyle(.secondary)
                            }
                    }
                }
                .frame(width: 72, height: 72)
                .clipShape(RoundedRectangle(cornerRadius: 8))
                VStack(alignment: .leading, spacing: 2) {
                    Text(primary.remoteURL == nil ? "Photo evidence" : "Photo metadata only")
                        .font(.caption)
                    Text("Photos: \(max(1, refs.count))")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                Spacer()
            }
        }
    }

    private func routeTransportLabel(_ raw: String) -> String {
        let transport = SemayRouteTransport(rawValue: raw.lowercased()) ?? .unknown
        return transport.title
    }

    private func centerMap(latitude: Double, longitude: Double, zoomDelta: Double) {
        region = MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: latitude, longitude: longitude),
            span: MKCoordinateSpan(latitudeDelta: zoomDelta, longitudeDelta: zoomDelta)
        )
    }

    private func focus(latitude: Double, longitude: Double, zoomDelta: Double = 0.06) {
        centerMap(latitude: latitude, longitude: longitude, zoomDelta: zoomDelta)
    }

    private func telURL(for rawPhone: String) -> URL? {
        let cleaned = rawPhone
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .filter { "+0123456789".contains($0) }
        guard !cleaned.isEmpty else { return nil }
        return URL(string: "tel:\(cleaned)")
    }

    private func openDirections(latitude: Double, longitude: Double, name: String) {
        guard latitude != 0 || longitude != 0 else { return }
        let q = name.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? ""
        guard let url = URL(string: "http://maps.apple.com/?ll=\(latitude),\(longitude)&q=\(q)") else { return }
        openURL(url)
    }

    private func businessShareText(_ business: BusinessProfile) -> String {
        var lines: [String] = []
        lines.append(business.name)
        lines.append("\(business.category) • \(business.eAddress)")
        if !business.plusCode.isEmpty {
            lines.append(business.plusCode)
            lines.append("semay://loc/\(business.plusCode)")
        }
        if !business.phone.isEmpty {
            lines.append("Call: \(business.phone)")
        }
        lines.append("semay://business/\(business.businessID)")
        return lines.joined(separator: "\n")
    }

    private func placeShareText(_ pin: SemayMapPin) -> String {
        var lines: [String] = []
        lines.append(pin.name)
        lines.append("\(pin.type) • \(pin.eAddress)")
        if !pin.plusCode.isEmpty {
            lines.append(pin.plusCode)
            lines.append("semay://loc/\(pin.plusCode)")
        }
        if !pin.phone.isEmpty {
            lines.append("Call: \(pin.phone)")
        }
        lines.append("semay://place/\(pin.pinID)")
        return lines.joined(separator: "\n")
    }

    private func fitMapToPins() {
        guard !dataStore.pins.isEmpty else { return }
        let latitudes = dataStore.pins.map(\.latitude)
        let longitudes = dataStore.pins.map(\.longitude)
        guard let minLat = latitudes.min(),
              let maxLat = latitudes.max(),
              let minLon = longitudes.min(),
              let maxLon = longitudes.max() else { return }

        let center = CLLocationCoordinate2D(
            latitude: (minLat + maxLat) / 2.0,
            longitude: (minLon + maxLon) / 2.0
        )
        let latDelta = max(0.04, (maxLat - minLat) * 1.6)
        let lonDelta = max(0.04, (maxLon - minLon) * 1.6)
        region = MKCoordinateRegion(center: center, span: MKCoordinateSpan(latitudeDelta: latDelta, longitudeDelta: lonDelta))
    }

    private func initializeMapViewport() {
        if let currentLocation = locationState.lastKnownLocation {
            centerMap(
                latitude: currentLocation.coordinate.latitude,
                longitude: currentLocation.coordinate.longitude,
                zoomDelta: 0.06
            )
            return
        }

        if !dataStore.pins.isEmpty {
            fitMapToPins()
            return
        }

        if let bounds = tileStore.availablePack?.bounds {
            let packedLatDelta = max(0.08, (bounds.maxLat - bounds.minLat) * 1.5)
            let packedLonDelta = max(0.08, (bounds.maxLon - bounds.minLon) * 1.5)
            let latDelta = min(packedLatDelta, 0.55)
            let lonDelta = min(packedLonDelta, 0.55)
            region = MKCoordinateRegion(
                center: CLLocationCoordinate2D(
                    latitude: (bounds.minLat + bounds.maxLat) / 2.0,
                    longitude: (bounds.minLon + bounds.maxLon) / 2.0
                ),
                span: MKCoordinateSpan(latitudeDelta: latDelta, longitudeDelta: lonDelta)
            )
            return
        }

        centerMap(latitude: 15.3229, longitude: 38.9251, zoomDelta: 0.06)
    }

    private func autoSelectPackIfNeeded() {
        guard useOfflineTiles else { return }
        let preferredZoom = estimatedZoomLevel(for: region)
        guard let best = tileStore.bestPack(
            forLatitude: region.center.latitude,
            longitude: region.center.longitude,
            preferredZoom: preferredZoom
        ) else { return }
        guard best.path != tileStore.availablePack?.path else { return }
        guard best.path != lastAutoPackPath else { return }
        lastAutoPackPath = best.path
        tileStore.selectPack(best)
    }

    private func updateBaseLayerForConnectivity() {
        let best = tileStore.bestPackOrNil(for: region)
        let coverage = best.map { tileStore.coverageRatio(for: $0, in: region) }
        let mode = SemayMapBaseLayerPolicy.resolve(
            isOnline: reachability.isOnline,
            isBundledStarterSelected: tileStore.isBundledStarterSelected,
            bestPackCoverageRatio: coverage
        )

        switch mode {
        case .offline:
            guard let best else {
                useOfflineTiles = false
                useOSMBaseMap = reachability.isOnline
                return
            }
            if best.path != tileStore.availablePack?.path {
                tileStore.selectPack(best)
            }
            useOfflineTiles = true
            useOSMBaseMap = false
            lastAutoPackPath = best.path
        case .online:
            useOfflineTiles = false
            useOSMBaseMap = true
        case .none:
            useOfflineTiles = false
            useOSMBaseMap = false
        }
    }

    private func estimatedZoomLevel(for region: MKCoordinateRegion) -> Int {
        let lonDelta = max(0.0001, min(360, region.span.longitudeDelta))
        let zoom = log2(360.0 / lonDelta)
        return max(0, min(22, Int(round(zoom))))
    }

    private func approvePin(_ pin: SemayMapPin) {
        if locationState.permissionState != .authorized {
            locationState.enableLocationChannels()
            mapActionMessage = "Enable location access to approve places nearby."
            return
        }

        guard let loc = locationState.lastKnownLocation else {
            locationState.refreshChannels()
            mapActionMessage = "Getting your location… please try again in a moment."
            return
        }

        let here = loc
        let target = CLLocation(latitude: pin.latitude, longitude: pin.longitude)
        let distance = here.distance(from: target)
        if distance > 500 {
            mapActionMessage = "Move closer to approve (within 500m). You are ~\(Int(distance))m away."
            return
        }

        _ = dataStore.approvePin(pinID: pin.pinID, distanceMeters: distance)
        mapActionMessage = "Approved. Distance ~\(Int(distance))m."
    }

    private func installCommunityPack() async {
        installingCommunityPack = true
        defer { installingCommunityPack = false }
        do {
            if reachability.isOnline {
                do {
                    let installed: OfflineTilePack
                    if countryPacksEnabled,
                       let preferred = preferredFeaturedCountryPack(),
                       let packIdentifier = installIdentifier(for: preferred) {
                        installed = try await tileStore.installCountryPack(packID: packIdentifier)
                    } else {
                        installed = try await tileStore.installRecommendedPack()
                    }
                    updateBaseLayerForConnectivity()
                    await refreshCommunityPackAvailability()
                    tileImportMessage = "Downloaded offline maps: \(installed.name)."
                    return
                } catch {
                    let reason = userFacingOfflineMapError(error)
                    if isSignedPackPolicyError(error) {
                        updateBaseLayerForConnectivity()
                        await refreshCommunityPackAvailability()
                        tileImportMessage = "Couldn't download offline maps (\(reason))."
                        return
                    }
                    if tileStore.availablePack == nil, tileStore.canInstallBundledStarterPack {
                        let installed = try tileStore.installBundledStarterPack()
                        updateBaseLayerForConnectivity()
                        await refreshCommunityPackAvailability()
                        tileImportMessage = "Couldn't download full offline maps (\(reason)). Installed starter offline maps: \(installed.name)."
                        return
                    }
                    updateBaseLayerForConnectivity()
                    await refreshCommunityPackAvailability()
                    tileImportMessage = "Couldn't download full offline maps (\(reason)). Keeping your current maps."
                    return
                }
            }

            let installed = try tileStore.installBundledStarterPack()
            updateBaseLayerForConnectivity()
            await refreshCommunityPackAvailability()
            tileImportMessage = "Installed \(installed.name)."
        } catch {
            tileImportMessage = userFacingOfflineMapError(error)
        }
    }

    private func refreshCommunityPackAvailability() async {
        guard reachability.isOnline else {
            hubCatalogReachable = false
            communityPackDownloadAvailable = false
            featuredCountryPacks = []
            return
        }
        do {
            let packs = try await tileStore.fetchHubCatalog()
            hubCatalogReachable = true
            communityPackDownloadAvailable = !packs.isEmpty
            featuredCountryPacks = packs
                .filter { pack in
                    let country = ((pack.countryCode ?? "").trimmingCharacters(in: .whitespacesAndNewlines))
                    let region = ((pack.regionCode ?? "").trimmingCharacters(in: .whitespacesAndNewlines))
                    return !country.isEmpty || !region.isEmpty
                }
                .sorted { lhs, rhs in
                    let lhsFeatured = lhs.isFeatured ?? false
                    let rhsFeatured = rhs.isFeatured ?? false
                    if lhsFeatured != rhsFeatured {
                        return lhsFeatured && !rhsFeatured
                    }
                    let lhsOrder = lhs.displayOrder ?? Int.max
                    let rhsOrder = rhs.displayOrder ?? Int.max
                    if lhsOrder != rhsOrder {
                        return lhsOrder < rhsOrder
                    }
                    let lhsName = lhs.countryName ?? lhs.name
                    let rhsName = rhs.countryName ?? rhs.name
                    return lhsName.localizedCaseInsensitiveCompare(rhsName) == .orderedAscending
                }
        } catch {
            hubCatalogReachable = false
            communityPackDownloadAvailable = false
            featuredCountryPacks = []
        }
    }

    private func mapSurfaceStatusText() -> String {
        let mode = SemayMapStatusBannerMode.resolve(
            isOnline: reachability.isOnline,
            hasActiveOfflinePack: useOfflineTiles && tileStore.availablePack != nil
        )
        switch mode {
        case .offlinePack:
            return String(localized: "semay.map.state.offline_pack", defaultValue: "Offline pack installed")
        case .online:
            return String(localized: "semay.map.state.online", defaultValue: "Online map active")
        case .offlineUnavailable:
            return String(
                localized: "semay.map.state.offline_unavailable",
                defaultValue: "Offline map unavailable; download when online"
            )
        }
    }

    private func preferredFeaturedCountryPack() -> HubTilePack? {
        guard countryPacksEnabled, !featuredCountryPacks.isEmpty else { return nil }
        if let eritrea = featuredCountryPacks.first(where: {
            (($0.countryCode ?? "").lowercased() == "er")
                || (($0.regionCode ?? "").lowercased() == "er")
                || $0.name.lowercased().contains("eritrea")
        }) {
            return eritrea
        }
        if let featured = featuredCountryPacks.first(where: { $0.isFeatured ?? false }) {
            return featured
        }
        return featuredCountryPacks.first
    }

    private func installIdentifier(for pack: HubTilePack) -> String? {
        let packID = (pack.packID ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if !packID.isEmpty {
            return packID
        }
        let fallback = pack.id.trimmingCharacters(in: .whitespacesAndNewlines)
        return fallback.isEmpty ? nil : fallback
    }

    private func countryInstallButtonTitle(for pack: HubTilePack) -> String {
        let country = (pack.countryName ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if !country.isEmpty {
            return "Download \(country)"
        }
        return String(localized: "semay.map.install", defaultValue: "Install")
    }

    private func websiteURL(for rawWebsite: String) -> URL? {
        let trimmed = rawWebsite.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        if let parsed = URL(string: trimmed), parsed.scheme != nil {
            return parsed
        }
        return URL(string: "https://\(trimmed)")
    }

    private func requestNetworkShare(for service: SemayServiceDirectoryEntry) {
        let result = dataStore.requestNetworkShareForService(serviceID: service.serviceID)
        if result.accepted {
            mapActionMessage = listingString(
                "semay.listing.message.queued_for_network",
                "Listing queued for network sharing."
            )
            return
        }
        if result.reasons.isEmpty {
            mapActionMessage = listingString(
                "semay.listing.message.share_blocked_generic",
                "Share request was blocked by quality checks."
            )
            return
        }
        let reasons = result.reasons.map(qualityReasonLabel).joined(separator: ", ")
        mapActionMessage = "\(listingString("semay.listing.message.share_blocked_prefix", "Share request blocked")): \(reasons)."
    }

    private func keepPersonalOnly(for service: SemayServiceDirectoryEntry) {
        dataStore.setServiceContributionScope(serviceID: service.serviceID, scope: .personal)
        mapActionMessage = listingString("semay.listing.message.personal_only", "Listing is now personal-only.")
    }

    private func shareStatusText(for service: SemayServiceDirectoryEntry) -> String {
        switch service.publishState {
        case .localOnly:
            return listingString("semay.listing.share.personal_only", "Personal only")
        case .pendingReview:
            return listingString("semay.listing.share.queued", "Queued for network")
        case .published:
            return listingString("semay.listing.share.published", "Published to network")
        case .rejected:
            return listingString("semay.listing.share.rejected", "Network share blocked")
        }
    }

    private func shareStatusTint(for service: SemayServiceDirectoryEntry) -> Color {
        switch service.publishState {
        case .localOnly:
            return .secondary
        case .pendingReview:
            return .blue
        case .published:
            return .green
        case .rejected:
            return .orange
        }
    }

    private func qualityReasonList(from json: String) -> [String] {
        guard let data = json.data(using: .utf8),
              let decoded = try? JSONDecoder().decode([String].self, from: data) else {
            return []
        }
        return decoded
    }

    private func qualityReasonLabel(_ key: String) -> String {
        switch key {
        case "missing_required_fields":
            return listingString("semay.listing.reason.missing_required_fields", "missing required fields")
        case "invalid_coordinates":
            return listingString("semay.listing.reason.invalid_coordinates", "invalid coordinates")
        case "possible_duplicate":
            return listingString("semay.listing.reason.possible_duplicate", "possible duplicate listing")
        case "photo_limit_exceeded":
            return listingString("semay.listing.reason.photo_limit_exceeded", "photo limit exceeded")
        case "photo_resolution_low":
            return listingString("semay.listing.reason.photo_resolution_low", "photo resolution too low")
        case "photo_byte_cap_exceeded":
            return listingString("semay.listing.reason.photo_byte_cap_exceeded", "photo size too large")
        case "photo_duplicate_hash":
            return listingString("semay.listing.reason.photo_duplicate_hash", "duplicate photo detected")
        case "author_trust_low":
            return listingString("semay.listing.reason.author_trust_low", "author trust is too low")
        case "author_rate_limited":
            return listingString("semay.listing.reason.author_rate_limited", "author is temporarily rate limited")
        default:
            return key.replacingOccurrences(of: "_", with: " ")
        }
    }

    private func listingString(_ key: String, _ fallback: String) -> String {
        NSLocalizedString(key, tableName: nil, bundle: .main, value: fallback, comment: "")
    }
}

#if os(iOS)
private struct TilePackInfoSheet: View {
    private struct PackHealthSummary {
        enum Kind {
            case healthy
            case missingDeps
            case hasDependents
            case unresolved
        }

        let kind: Kind
        let label: String
        let tint: Color
        let detail: String
    }

    private struct PackIntegritySummary {
        enum Kind {
            case unsigned
            case hashVerified
            case signatureVerified
        }

        let kind: Kind
        let label: String
        let tint: Color
        let detail: String
        let hashPreview: String
        let signatureAlgorithm: String
    }

    private struct InstalledPackHealthRow: Identifiable {
        let pack: OfflineTilePack
        let activation: OfflineTilePackActivationStatus
        let health: PackHealthSummary
        let integrity: PackIntegritySummary

        var id: String { pack.path }
    }

    private struct PackOpsSummary {
        var total = 0
        var healthy = 0
        var missingDeps = 0
        var hasDependents = 0
        var unresolved = 0
    }

    @Binding var isPresented: Bool
    @ObservedObject var tileStore: OfflineTileStore
    @Binding var useOfflineTiles: Bool
    @Binding var useOSMBaseMap: Bool
    @AppStorage("semay.settings.advanced") private var advancedSettingsEnabled = false
    @AppStorage("semay.map.country_packs.enabled") private var countryPacksEnabled = false
    @AppStorage("semay.offline_maps.require_signed_packs") private var requireSignedOfflinePacks = false
    @State private var hubPacks: [HubTilePack] = []
    @State private var featuredCountryPacks: [HubTilePack] = []
    @State private var loadingCountryPacks = false
    @State private var installingCountryPackID: String?
    @State private var loadingHubPacks = false
    @State private var downloadingPackID: String?
    @State private var publishingPack = false
    @State private var hubError = ""
    @State private var hubNotice = ""
    @State private var pendingCascadeDeletePlan: OfflineTilePackCascadeDeletionPlan?
    @State private var showingCascadeDeleteConfirm = false

    private var selectedPack: OfflineTilePack? {
        tileStore.availablePack
    }

    private var installedPackRows: [InstalledPackHealthRow] {
        tileStore.packs.map { pack in
            let activation = tileStore.activationStatus(for: pack)
            return InstalledPackHealthRow(
                pack: pack,
                activation: activation,
                health: packHealth(for: pack, activationStatus: activation),
                integrity: packIntegrity(for: pack)
            )
        }
    }

    private var opsSummary: PackOpsSummary {
        var summary = PackOpsSummary()
        for row in installedPackRows {
            summary.total += 1
            switch row.health.kind {
            case .healthy:
                summary.healthy += 1
            case .missingDeps:
                summary.missingDeps += 1
            case .hasDependents:
                summary.hasDependents += 1
            case .unresolved:
                summary.unresolved += 1
            }
        }
        return summary
    }

    private var selectedPackHealth: PackHealthSummary? {
        guard let selectedPack else { return nil }
        if let existing = installedPackRows.first(where: { $0.pack.path == selectedPack.path }) {
            return existing.health
        }
        return packHealth(for: selectedPack)
    }

    var body: some View {
        NavigationStack {
            Form {
                if opsSummary.total > 0 {
                    Section("Ops Summary") {
                        LabeledContent("Installed", value: "\(opsSummary.total)")
                        LabeledContent("Healthy", value: "\(opsSummary.healthy)")
                        LabeledContent("Missing Deps", value: "\(opsSummary.missingDeps)")
                        LabeledContent("Has Dependents", value: "\(opsSummary.hasDependents)")
                        LabeledContent("Unresolved", value: "\(opsSummary.unresolved)")
                    }
                }

                Section("Selected Pack") {
                    if let pack = selectedPack {
                        let health = selectedPackHealth ?? packHealth(for: pack)
                        let integrity = installedPackRows.first(where: { $0.pack.path == pack.path })?.integrity ?? packIntegrity(for: pack)
                        LabeledContent("Name", value: pack.name)
                        LabeledContent("Health") {
                            healthBadge(health)
                        }
                        LabeledContent("Integrity") {
                            statusBadge(label: integrity.label, tint: integrity.tint)
                        }
                        if let version = pack.packVersion, !version.isEmpty {
                            LabeledContent("Version", value: version)
                        }
                        if let region = pack.regionCode, !region.isEmpty {
                            LabeledContent("Region", value: region.uppercased())
                        }
                        LabeledContent("Format", value: pack.tileFormat.rawValue)
                        LabeledContent("Zoom", value: "\(pack.minZoom)–\(pack.maxZoom)")
                        if let bounds = pack.bounds {
                            LabeledContent("Bounds", value: "\(format(bounds.minLat)),\(format(bounds.minLon)) → \(format(bounds.maxLat)),\(format(bounds.maxLon))")
                        } else {
                            LabeledContent("Bounds", value: "Unknown")
                        }
                        LabeledContent("Size", value: formatSize(pack.sizeBytes))
                        LabeledContent("Attribution", value: pack.attribution)
                        if !integrity.hashPreview.isEmpty {
                            LabeledContent("SHA256", value: integrity.hashPreview)
                        }
                        if !integrity.signatureAlgorithm.isEmpty {
                            LabeledContent("Signature", value: integrity.signatureAlgorithm)
                        }
                        if !integrity.detail.isEmpty {
                            Text(integrity.detail)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    } else {
                        Text("No offline pack selected.")
                            .foregroundStyle(.secondary)
                    }
                }

                if !hubNotice.isEmpty || !hubError.isEmpty {
                    Section("Status") {
                        if !hubNotice.isEmpty {
                            Text(hubNotice)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        if !hubError.isEmpty {
                            Text(hubError)
                                .font(.caption)
                                .foregroundStyle(.orange)
                        }
                    }
                }

                if countryPacksEnabled {
                    Section("Country Packs") {
                        HStack {
                            Text("Free downloads. Eritrea is featured first.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Spacer()
                            Button(loadingCountryPacks ? "Loading..." : "Refresh") {
                                Task {
                                    await loadCountryPacks()
                                }
                            }
                            .disabled(loadingCountryPacks)
                        }

                        if loadingCountryPacks {
                            ProgressView()
                        } else if featuredCountryPacks.isEmpty {
                            Text("No country packs available right now.")
                                .foregroundStyle(.secondary)
                        } else {
                            ForEach(featuredCountryPacks, id: \.id) { pack in
                                let dependencies = pack.dependsOn ?? []
                                HStack(alignment: .center, spacing: 12) {
                                    VStack(alignment: .leading, spacing: 4) {
                                        HStack(spacing: 8) {
                                            Text(pack.countryName ?? pack.name)
                                            if pack.isFeatured ?? false {
                                                statusBadge(label: "Featured", tint: .blue)
                                            }
                                        }
                                        Text(
                                            "Pack: \(pack.name) • \(formatSize(pack.downloadSizeBytes ?? pack.sizeBytes))"
                                        )
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                        if !dependencies.isEmpty {
                                            Text("Dependencies: \(dependencies.joined(separator: ", "))")
                                                .font(.caption2)
                                                .foregroundStyle(.secondary)
                                        }
                                        if let minVersion = pack.minAppVersion, !minVersion.isEmpty {
                                            Text("Requires app version \(minVersion)+")
                                                .font(.caption2)
                                                .foregroundStyle(.secondary)
                                        }
                                    }
                                    Spacer()
                                    Button(installingCountryPackID == pack.id ? "Installing..." : "Install") {
                                        Task {
                                            await installCountryPack(pack)
                                        }
                                    }
                                    .disabled(installingCountryPackID == pack.id)
                                }
                            }
                        }
                    }
                }

                Section("Available Packs") {
                    if installedPackRows.isEmpty {
                        Text("No packs installed.")
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(installedPackRows) { row in
                            let pack = row.pack
                            Button {
                                tileStore.selectPack(pack)
                                useOfflineTiles = true
                                useOSMBaseMap = false
                            } label: {
                                HStack {
                                    VStack(alignment: .leading, spacing: 4) {
                                        Text(pack.name)
                                        Text(formatSize(pack.sizeBytes))
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                        healthBadge(row.health)
                                        statusBadge(label: row.integrity.label, tint: row.integrity.tint)
                                        if !row.health.detail.isEmpty {
                                            Text(row.health.detail)
                                                .font(.caption2)
                                                .foregroundStyle(row.health.tint)
                                        }
                                    }
                                    Spacer()
                                    if pack.path == selectedPack?.path {
                                        Image(systemName: "checkmark.circle.fill")
                                            .foregroundStyle(.green)
                                    }
                                }
                            }
                            .disabled(!row.activation.canActivate)
                        }
                    }
                }

                if let pack = selectedPack {
                    let deletionPlan = tileStore.deletionPlan(for: pack)
                    let cascadePlan = tileStore.cascadeDeletionPlan(for: pack)
                    let activation = tileStore.activationStatus(for: pack)
                    let activationChain = tileStore.activationChain(for: pack)
                    Section("Dependency Graph") {
                        if !activationChain.isEmpty {
                            Text("Activation: \(activationGraphLabel(for: activationChain))")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        if activation.hasBlockingDependencies {
                            Text("Missing deps: \(activation.missingDependencies.joined(separator: ", "))")
                                .font(.caption)
                                .foregroundStyle(.orange)
                        }
                        if !cascadePlan.dependents.isEmpty {
                            Text("Dependents: \(collapsedPackNames(from: cascadePlan.dependents))")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        if activationChain.isEmpty, cascadePlan.dependents.isEmpty {
                            Text("No dependency links detected for this pack.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }

                    Section("Manage") {
                        if !deletionPlan.canDelete {
                            Text("This pack is required by: \(deletionPlan.blockingDependents.map(\.name).joined(separator: ", ")).")
                                .font(.caption)
                                .foregroundStyle(.orange)
                        }
                        if advancedSettingsEnabled {
                            Button(publishingPack ? "Publishing..." : "Publish Selected Pack to Source") {
                                Task {
                                    await publishPack(pack)
                                }
                            }
                            .disabled(publishingPack)
                            if !deletionPlan.canDelete {
                                Button(role: .destructive) {
                                    beginCascadeDelete(cascadePlan)
                                } label: {
                                    Text(cascadeDeleteButtonTitle(for: cascadePlan))
                                }
                            }
                        }
                        Button(role: .destructive) {
                            removePack(pack)
                        } label: {
                            Text("Delete Pack")
                        }
                        .disabled(!deletionPlan.canDelete)
                    }
                }

                if advancedSettingsEnabled {
                    Section("Node Catalog (Advanced)") {
                        if let activeHub = tileStore.activeMapSourceBaseURL, !activeHub.isEmpty {
                            Text("Node: \(activeHub)")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        Button(loadingHubPacks ? "Loading..." : "Load From Node") {
                            Task {
                                await loadHubCatalog()
                            }
                        }
                        .disabled(loadingHubPacks)

                        if loadingHubPacks {
                            ProgressView()
                        }

                        if hubPacks.isEmpty {
                            Text("No packs loaded from node yet.")
                                .foregroundStyle(.secondary)
                        } else {
                            let summary = catalogReadinessSummary(for: hubPacks)
                            let security = catalogIntegritySummary(for: hubPacks)
                            Text("Catalog: \(summary.ready) ready • \(summary.blocked) blocked")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Text("Integrity: \(security.signed) signed • \(security.hashed) hashed • \(security.unverified) unverified")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            if requireSignedOfflinePacks {
                                Text("Policy: signed packs required")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            ForEach(hubPacks, id: \.id) { pack in
                                let installPlan = tileStore.installPlan(for: pack, catalog: hubPacks)
                                let policyBlocked = requireSignedOfflinePacks && !catalogPackSatisfiesSignedPolicy(pack)
                                HStack(alignment: .center, spacing: 12) {
                                    VStack(alignment: .leading, spacing: 4) {
                                        HStack(spacing: 8) {
                                            Text(pack.name)
                                            statusBadge(
                                                label: catalogStatusLabel(for: installPlan),
                                                tint: catalogStatusTint(for: installPlan)
                                            )
                                            statusBadge(
                                                label: catalogIntegrityLabel(for: pack),
                                                tint: catalogIntegrityTint(for: pack)
                                            )
                                        }
                                        let version = (pack.packVersion ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                                        let region = (pack.regionCode ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                                        let format = (pack.tileFormat ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                                        Text(
                                            "Zoom \(pack.minZoom)-\(pack.maxZoom) • \(formatSize(pack.sizeBytes))"
                                            + (version.isEmpty ? "" : " • v\(version)")
                                            + (region.isEmpty ? "" : " • \(region.uppercased())")
                                            + (format.isEmpty ? "" : " • \(format)")
                                        )
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                        if !installPlan.dependenciesToInstall.isEmpty {
                                            Text("Graph: \(installGraphLabel(pack: pack, plan: installPlan))")
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                        }
                                        if !installPlan.missingDependencies.isEmpty {
                                            Text("Missing deps: \(installPlan.missingDependencies.joined(separator: ", "))")
                                                .font(.caption)
                                                .foregroundStyle(.orange)
                                        } else if !installPlan.alreadySatisfiedDependencies.isEmpty {
                                            Text("Deps ready: \(installPlan.alreadySatisfiedDependencies.joined(separator: ", "))")
                                                .font(.caption)
                                                .foregroundStyle(.secondary)
                                        }
                                        if installPlan.hasCycle {
                                            Text("Dependency cycle detected.")
                                                .font(.caption)
                                                .foregroundStyle(.orange)
                                        }
                                        if policyBlocked {
                                            Text("Signed-pack policy blocks install: hash/signature metadata required.")
                                                .font(.caption)
                                                .foregroundStyle(.orange)
                                        }
                                    }
                                    Spacer()
                                    if selectedPack?.path.lowercased().hasSuffix(pack.fileName.lowercased()) == true {
                                        Image(systemName: "checkmark.circle.fill")
                                            .foregroundStyle(.green)
                                    } else {
                                        Button(installButtonTitle(for: pack, plan: installPlan)) {
                                            Task {
                                                await installHubPack(pack)
                                            }
                                        }
                                        .disabled(
                                            downloadingPackID == pack.id ||
                                                installPlan.hasCycle ||
                                                !installPlan.missingDependencies.isEmpty ||
                                                policyBlocked
                                        )
                                    }
                                }
                            }
                        }
                    }
                }
            }
            .navigationTitle("Offline Maps")
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { isPresented = false }
                }
            }
            .confirmationDialog(
                "Delete With Dependents",
                isPresented: $showingCascadeDeleteConfirm,
                titleVisibility: .visible
            ) {
                if let plan = pendingCascadeDeletePlan {
                    Button(cascadeConfirmButtonTitle(for: plan), role: .destructive) {
                        executeCascadeDelete(plan)
                    }
                }
                Button("Cancel", role: .cancel) {
                    pendingCascadeDeletePlan = nil
                }
            } message: {
                if let plan = pendingCascadeDeletePlan {
                    Text(cascadeDeleteMessage(for: plan))
                }
            }
            .task {
                if countryPacksEnabled && featuredCountryPacks.isEmpty {
                    await loadCountryPacks()
                }
            }
        }
    }

    private func loadCountryPacks() async {
        loadingCountryPacks = true
        hubError = ""
        defer { loadingCountryPacks = false }
        do {
            featuredCountryPacks = try await tileStore.featuredCountryPacks()
        } catch {
            featuredCountryPacks = []
            hubError = error.localizedDescription
        }
    }

    private func installCountryPack(_ pack: HubTilePack) async {
        guard let identifier = countryPackIdentifier(for: pack) else {
            hubError = "Pack identifier is missing for \(pack.name)."
            return
        }
        installingCountryPackID = pack.id
        hubError = ""
        hubNotice = ""
        defer { installingCountryPackID = nil }
        do {
            let installed = try await tileStore.installCountryPack(packID: identifier)
            useOfflineTiles = true
            useOSMBaseMap = false
            hubNotice = "Installed \(installed.name)."
        } catch {
            hubError = userFacingOfflineMapError(error)
        }
    }

    private func countryPackIdentifier(for pack: HubTilePack) -> String? {
        let packID = (pack.packID ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if !packID.isEmpty {
            return packID
        }
        let fallback = pack.id.trimmingCharacters(in: .whitespacesAndNewlines)
        return fallback.isEmpty ? nil : fallback
    }

    private func loadHubCatalog() async {
        loadingHubPacks = true
        hubError = ""
        defer { loadingHubPacks = false }
        do {
            hubPacks = try await tileStore.fetchHubCatalog()
        } catch {
            hubError = error.localizedDescription
        }
    }

    private func installHubPack(_ pack: HubTilePack) async {
        downloadingPackID = pack.id
        hubError = ""
        hubNotice = ""
        defer { downloadingPackID = nil }
        do {
            let plan = tileStore.installPlan(for: pack, catalog: hubPacks)
            if plan.hasCycle {
                hubError = "Dependency cycle detected for \(pack.name)."
                return
            }
            if !plan.missingDependencies.isEmpty {
                hubError = "Missing required dependencies: \(plan.missingDependencies.joined(separator: ", "))"
                return
            }

            let result = try await tileStore.installHubPackWithDependencies(pack, catalog: hubPacks)
            useOfflineTiles = true
            useOSMBaseMap = false
            hubNotice = installNotice(for: result)
        } catch {
            hubError = userFacingOfflineMapError(error)
        }
    }

    private func publishPack(_ pack: OfflineTilePack) async {
        publishingPack = true
        hubError = ""
        hubNotice = ""
        defer { publishingPack = false }
        do {
            let published = try await tileStore.publishPackToHub(pack)
            hubNotice = "Published \(published.name) to node."
            await loadHubCatalog()
        } catch {
            hubError = userFacingOfflineMapError(error)
        }
    }

    private func removePack(_ pack: OfflineTilePack) {
        do {
            try tileStore.deletePack(pack)
            hubNotice = "Deleted \(pack.name)."
            hubError = ""
        } catch {
            hubError = error.localizedDescription
            hubNotice = ""
        }
        if tileStore.availablePack == nil {
            useOfflineTiles = false
            useOSMBaseMap = false
        }
    }

    private func beginCascadeDelete(_ plan: OfflineTilePackCascadeDeletionPlan) {
        guard plan.hasDependents else { return }
        pendingCascadeDeletePlan = plan
        showingCascadeDeleteConfirm = true
    }

    private func executeCascadeDelete(_ plan: OfflineTilePackCascadeDeletionPlan) {
        do {
            let removed = try tileStore.deletePackCascadingDependents(plan.target)
            hubNotice = "Deleted \(removed.count) packs: \(collapsedPackNames(from: removed))."
            hubError = ""
        } catch {
            hubError = error.localizedDescription
            hubNotice = ""
        }

        pendingCascadeDeletePlan = nil
        if tileStore.availablePack == nil {
            useOfflineTiles = false
            useOSMBaseMap = false
        }
    }

    private func format(_ value: Double) -> String {
        String(format: "%.4f", value)
    }

    private func formatPercent(_ value: Double) -> String {
        String(format: "%.2f%%", value)
    }

    private func formatSize(_ bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB]
        formatter.countStyle = .file
        return formatter.string(fromByteCount: bytes)
    }

    private func installButtonTitle(for pack: HubTilePack, plan: HubTilePackInstallPlan) -> String {
        if downloadingPackID == pack.id {
            return "Installing..."
        }
        if plan.dependenciesToInstall.isEmpty {
            return "Install"
        }
        return "Install +\(plan.dependenciesToInstall.count)"
    }

    private func installNotice(for result: HubTilePackInstallResult) -> String {
        var segments: [String] = ["Installed \(result.primary.name) from node."]
        if !result.installedDependencies.isEmpty {
            let names = result.installedDependencies.map(\.name).joined(separator: ", ")
            segments.append("Dependencies installed: \(names).")
        }
        if !result.alreadySatisfiedDependencies.isEmpty {
            segments.append("Dependencies already available: \(result.alreadySatisfiedDependencies.joined(separator: ", ")).")
        }
        return segments.joined(separator: " ")
    }

    private func activationGraphLabel(for chain: [OfflineTilePack]) -> String {
        chain.map(\.name).joined(separator: " -> ")
    }

    private func installGraphLabel(pack: HubTilePack, plan: HubTilePackInstallPlan) -> String {
        var names = plan.dependenciesToInstall.map(\.name)
        names.append(pack.name)
        return names.joined(separator: " -> ")
    }

    private func packHealth(
        for pack: OfflineTilePack,
        activationStatus: OfflineTilePackActivationStatus? = nil
    ) -> PackHealthSummary {
        let activation = activationStatus ?? tileStore.activationStatus(for: pack)
        if !activation.canActivate {
            if activation.hasBlockingDependencies {
                return PackHealthSummary(
                    kind: .missingDeps,
                    label: "Missing Deps",
                    tint: .orange,
                    detail: "Missing: \(activation.missingDependencies.joined(separator: ", "))"
                )
            }
            return PackHealthSummary(
                kind: .unresolved,
                label: "Unresolved",
                tint: .orange,
                detail: "Dependency chain is unresolved."
            )
        }

        let deletion = tileStore.deletionPlan(for: pack)
        if !deletion.canDelete {
            let count = deletion.blockingDependents.count
            return PackHealthSummary(
                kind: .hasDependents,
                label: "Has Dependents",
                tint: .blue,
                detail: "Required by \(count) pack\(count == 1 ? "" : "s")."
            )
        }

        return PackHealthSummary(kind: .healthy, label: "Healthy", tint: .green, detail: "")
    }

    private func packIntegrity(for pack: OfflineTilePack) -> PackIntegritySummary {
        let install = SemayDataStore.shared.offlinePackInstall(path: pack.path)
        let hash = normalizedText(install?.sha256)
        let signature = normalizedText(install?.signature)
        let sigAlg = normalizedText(install?.sigAlg)?.uppercased() ?? ""

        if signature != nil {
            let detail = sigAlg.isEmpty
                ? "Signature metadata available."
                : "Signature verified using \(sigAlg)."
            return PackIntegritySummary(
                kind: .signatureVerified,
                label: "Signed",
                tint: .green,
                detail: detail,
                hashPreview: shortHash(hash),
                signatureAlgorithm: sigAlg
            )
        }

        if hash != nil {
            return PackIntegritySummary(
                kind: .hashVerified,
                label: "Hash Verified",
                tint: .blue,
                detail: "SHA256 metadata available.",
                hashPreview: shortHash(hash),
                signatureAlgorithm: ""
            )
        }

        return PackIntegritySummary(
            kind: .unsigned,
            label: "Unverified",
            tint: .orange,
            detail: "No hash/signature metadata.",
            hashPreview: "",
            signatureAlgorithm: ""
        )
    }

    private func normalizedText(_ value: String?) -> String? {
        guard let text = value?.trimmingCharacters(in: .whitespacesAndNewlines), !text.isEmpty else {
            return nil
        }
        return text
    }

    private func shortHash(_ hash: String?) -> String {
        guard let hash = normalizedText(hash) else { return "" }
        if hash.count <= 18 {
            return hash
        }
        return "\(hash.prefix(10))...\(hash.suffix(8))"
    }

    @ViewBuilder
    private func statusBadge(label: String, tint: Color) -> some View {
        Text(label)
            .font(.caption2)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .foregroundStyle(tint)
            .background(tint.opacity(0.16), in: Capsule())
    }

    @ViewBuilder
    private func healthBadge(_ health: PackHealthSummary) -> some View {
        statusBadge(label: health.label, tint: health.tint)
    }

    private func catalogStatusLabel(for plan: HubTilePackInstallPlan) -> String {
        if plan.hasCycle {
            return "Cycle"
        }
        if !plan.missingDependencies.isEmpty {
            return "Blocked"
        }
        if !plan.dependenciesToInstall.isEmpty {
            return "Ready+\(plan.dependenciesToInstall.count)"
        }
        return "Ready"
    }

    private func catalogStatusTint(for plan: HubTilePackInstallPlan) -> Color {
        if plan.hasCycle || !plan.missingDependencies.isEmpty {
            return .orange
        }
        if !plan.dependenciesToInstall.isEmpty {
            return .blue
        }
        return .green
    }

    private func catalogReadinessSummary(for packs: [HubTilePack]) -> (ready: Int, blocked: Int) {
        var ready = 0
        var blocked = 0
        for pack in packs {
            let plan = tileStore.installPlan(for: pack, catalog: packs)
            if plan.hasCycle || !plan.missingDependencies.isEmpty {
                blocked += 1
            } else {
                ready += 1
            }
        }
        return (ready, blocked)
    }

    private func catalogIntegritySummary(for packs: [HubTilePack]) -> (signed: Int, hashed: Int, unverified: Int) {
        var signed = 0
        var hashed = 0
        var unverified = 0
        for pack in packs {
            let hasHash = normalizedText(pack.sha256) != nil
            let hasSignature = normalizedText(pack.signature) != nil
            if hasSignature && hasHash {
                signed += 1
            } else if hasHash {
                hashed += 1
            } else {
                unverified += 1
            }
        }
        return (signed, hashed, unverified)
    }

    private func catalogIntegrityLabel(for pack: HubTilePack) -> String {
        let hasHash = normalizedText(pack.sha256) != nil
        let hasSignature = normalizedText(pack.signature) != nil
        if hasSignature && hasHash {
            return "Signed"
        }
        if hasHash {
            return "Hashed"
        }
        return "Unverified"
    }

    private func catalogIntegrityTint(for pack: HubTilePack) -> Color {
        switch catalogIntegrityLabel(for: pack) {
        case "Signed":
            return .green
        case "Hashed":
            return .blue
        default:
            return .orange
        }
    }

    private func catalogPackSatisfiesSignedPolicy(_ pack: HubTilePack) -> Bool {
        normalizedText(pack.sha256) != nil && normalizedText(pack.signature) != nil
    }

    private func cascadeDeleteButtonTitle(for plan: OfflineTilePackCascadeDeletionPlan) -> String {
        if !plan.hasDependents {
            return "Delete With Dependents"
        }
        return "Delete With \(plan.dependents.count) Dependents"
    }

    private func cascadeConfirmButtonTitle(for plan: OfflineTilePackCascadeDeletionPlan) -> String {
        "Delete \(plan.deletionOrder.count) Packs"
    }

    private func cascadeDeleteMessage(for plan: OfflineTilePackCascadeDeletionPlan) -> String {
        let names = plan.deletionOrder.map(\.name)
        return "This permanently deletes: \(collapsedPackNames(from: names))."
    }

    private func collapsedPackNames(from packs: [OfflineTilePack]) -> String {
        collapsedPackNames(from: packs.map(\.name))
    }

    private func collapsedPackNames(from names: [String]) -> String {
        if names.count <= 3 {
            return names.joined(separator: ", ")
        }
        let prefix = names.prefix(3).joined(separator: ", ")
        return "\(prefix), +\(names.count - 3) more"
    }
}
#endif

#if os(iOS)
private struct SemayMapCanvas: View {
    @ObservedObject var mapEngine: MapEngineCoordinator
    @Binding var region: MKCoordinateRegion
    let pins: [SemayMapPin]
    @Binding var selectedPinID: String?
    let businesses: [BusinessProfile]
    @Binding var selectedBusinessID: String?
    let routes: [SemayCuratedRoute]
    @Binding var selectedRouteID: String?
    let services: [SemayServiceDirectoryEntry]
    @Binding var selectedServiceID: String?
    @Binding var useOSMBaseMap: Bool
    let offlinePacks: [OfflineTilePack]
    @Binding var useOfflineTiles: Bool
    let onLongPress: (CLLocationCoordinate2D) -> Void

    var body: some View {
        if mapEngine.effectiveEngine == .maplibre {
            SemayMapLibreView(
                region: $region,
                pins: pins,
                selectedPinID: $selectedPinID,
                businesses: businesses,
                selectedBusinessID: $selectedBusinessID,
                routes: routes,
                selectedRouteID: $selectedRouteID,
                services: services,
                selectedServiceID: $selectedServiceID,
                offlinePacks: offlinePacks,
                useOfflineTiles: useOfflineTiles,
                useOSMBaseMap: $useOSMBaseMap,
                onLongPress: onLongPress,
                onRuntimeError: { reason in
                    mapEngine.markMapLibreFailure(reason)
                }
            )
        } else {
            SemayMapKitView(
                region: $region,
                pins: pins,
                selectedPinID: $selectedPinID,
                businesses: businesses,
                selectedBusinessID: $selectedBusinessID,
                routes: routes,
                selectedRouteID: $selectedRouteID,
                services: services,
                selectedServiceID: $selectedServiceID,
                useOSMBaseMap: $useOSMBaseMap,
                offlinePacks: offlinePacks,
                useOfflineTiles: $useOfflineTiles,
                onLongPress: onLongPress
            )
        }
    }
}

#if canImport(MapLibre)
private struct SemayMapLibreView: UIViewRepresentable {
    @Binding var region: MKCoordinateRegion
    let pins: [SemayMapPin]
    @Binding var selectedPinID: String?
    let businesses: [BusinessProfile]
    @Binding var selectedBusinessID: String?
    let routes: [SemayCuratedRoute]
    @Binding var selectedRouteID: String?
    let services: [SemayServiceDirectoryEntry]
    @Binding var selectedServiceID: String?
    let offlinePacks: [OfflineTilePack]
    let useOfflineTiles: Bool
    @Binding var useOSMBaseMap: Bool
    let onLongPress: (CLLocationCoordinate2D) -> Void
    let onRuntimeError: (String) -> Void

    func makeUIView(context: Context) -> MLNMapView {
        let styleURL = context.coordinator.resolveStyleURL(
            useOfflineTiles: useOfflineTiles,
            offlinePacks: offlinePacks,
            useOSMBaseMap: useOSMBaseMap
        )
        let mapView = MLNMapView(frame: .zero, styleURL: styleURL)
        mapView.delegate = context.coordinator
        mapView.showsScale = true
        mapView.showsCompass = true
        mapView.setCenter(region.center, zoomLevel: 12, animated: false)
        context.coordinator.applyZoomLimits(on: mapView, useOfflineTiles: useOfflineTiles, offlinePacks: offlinePacks)

        let longPress = UILongPressGestureRecognizer(target: context.coordinator, action: #selector(Coordinator.handleLongPress(_:)))
        longPress.minimumPressDuration = 0.5
        mapView.addGestureRecognizer(longPress)
        context.coordinator.syncMapContent(
            on: mapView,
            pins: pins,
            businesses: businesses,
            routes: routes,
            services: services
        )
        return mapView
    }

    func updateUIView(_ mapView: MLNMapView, context: Context) {
        let center = mapView.centerCoordinate
        let centerDiff = abs(center.latitude - region.center.latitude) + abs(center.longitude - region.center.longitude)
        if centerDiff > 0.001 {
            mapView.setCenter(region.center, zoomLevel: mapView.zoomLevel, animated: true)
        }
        context.coordinator.applyStyleIfNeeded(
            on: mapView,
            useOfflineTiles: useOfflineTiles,
            offlinePacks: offlinePacks,
            useOSMBaseMap: useOSMBaseMap
        )
        context.coordinator.applyZoomLimits(on: mapView, useOfflineTiles: useOfflineTiles, offlinePacks: offlinePacks)
        context.coordinator.syncMapContent(
            on: mapView,
            pins: pins,
            businesses: businesses,
            routes: routes,
            services: services
        )
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    final class Coordinator: NSObject, MLNMapViewDelegate {
        private let parent: SemayMapLibreView
        private var styleSignature: String?

        init(_ parent: SemayMapLibreView) {
            self.parent = parent
            super.init()
        }

        @objc func handleLongPress(_ gesture: UILongPressGestureRecognizer) {
            guard gesture.state == .began else { return }
            guard let mapView = gesture.view as? MLNMapView else { return }
            let point = gesture.location(in: mapView)
            let coordinate = mapView.convert(point, toCoordinateFrom: mapView)
            parent.onLongPress(coordinate)
        }

        func mapView(_ mapView: MLNMapView, regionDidChangeAnimated animated: Bool) {
            let bounds = mapView.visibleCoordinateBounds
            let center = mapView.centerCoordinate
            parent.region = MKCoordinateRegion(
                center: center,
                span: MKCoordinateSpan(
                    latitudeDelta: abs(bounds.ne.latitude - bounds.sw.latitude),
                    longitudeDelta: abs(bounds.ne.longitude - bounds.sw.longitude)
                )
            )
        }

        func mapView(_ mapView: MLNMapView, didSelect annotation: MLNAnnotation) {
            guard let entity = annotation as? SemayMapLibreEntityAnnotation else { return }
            switch entity.kind {
            case .pin:
                parent.selectedPinID = entity.entityID
                parent.selectedBusinessID = nil
                parent.selectedRouteID = nil
                parent.selectedServiceID = nil
            case .business:
                parent.selectedPinID = nil
                parent.selectedBusinessID = entity.entityID
                parent.selectedRouteID = nil
                parent.selectedServiceID = nil
            case .route:
                parent.selectedPinID = nil
                parent.selectedBusinessID = nil
                parent.selectedRouteID = entity.entityID
                parent.selectedServiceID = nil
            case .service:
                parent.selectedPinID = nil
                parent.selectedBusinessID = nil
                parent.selectedRouteID = nil
                parent.selectedServiceID = entity.entityID
            }
        }

        func mapView(_ mapView: MLNMapView, annotationCanShowCallout annotation: MLNAnnotation) -> Bool {
            true
        }

        func mapViewDidFailLoadingMap(_ mapView: MLNMapView, withError error: Error) {
            parent.onRuntimeError("maplibre-load-failed: \(error.localizedDescription)")
        }

        func applyStyleIfNeeded(
            on mapView: MLNMapView,
            useOfflineTiles: Bool,
            offlinePacks: [OfflineTilePack],
            useOSMBaseMap: Bool
        ) {
            let nextURL = resolveStyleURL(
                useOfflineTiles: useOfflineTiles,
                offlinePacks: offlinePacks,
                useOSMBaseMap: useOSMBaseMap
            )
            let nextSignature = styleKey(
                useOfflineTiles: useOfflineTiles,
                offlinePacks: offlinePacks,
                useOSMBaseMap: useOSMBaseMap,
                styleURL: nextURL
            )
            if styleSignature == nextSignature {
                return
            }
            styleSignature = nextSignature
            if let nextURL {
                mapView.styleURL = nextURL
            } else {
                parent.onRuntimeError("maplibre-style-unavailable")
            }
        }

        func applyZoomLimits(
            on mapView: MLNMapView,
            useOfflineTiles: Bool,
            offlinePacks: [OfflineTilePack]
        ) {
            if useOfflineTiles, !offlinePacks.isEmpty {
                let minZoom = offlinePacks.map(\.minZoom).min() ?? 0
                let maxZoom = offlinePacks.map(\.maxZoom).max() ?? 16
                mapView.minimumZoomLevel = Double(minZoom)
                mapView.maximumZoomLevel = Double(maxZoom)
                return
            }
            mapView.minimumZoomLevel = 0
            mapView.maximumZoomLevel = 22
        }

        func resolveStyleURL(
            useOfflineTiles: Bool,
            offlinePacks: [OfflineTilePack],
            useOSMBaseMap: Bool
        ) -> URL? {
            if useOfflineTiles {
                guard !offlinePacks.isEmpty else {
                    parent.onRuntimeError("maplibre-offline-pack-missing")
                    return nil
                }
                do {
                    try MapLibreRasterBridge.shared.prepare(packs: offlinePacks)
                    guard let styleURL = MapLibreRasterBridge.shared.styleURL() else {
                        parent.onRuntimeError("maplibre-offline-style-missing")
                        return nil
                    }
                    return styleURL
                } catch {
                    parent.onRuntimeError("maplibre-offline-prepare-failed: \(error.localizedDescription)")
                    return nil
                }
            }

            _ = useOSMBaseMap
            guard let onlineStyleURL = URL(string: "https://demotiles.maplibre.org/style.json") else {
                parent.onRuntimeError("maplibre-online-style-invalid")
                return nil
            }
            return onlineStyleURL
        }

        private func styleKey(
            useOfflineTiles: Bool,
            offlinePacks: [OfflineTilePack],
            useOSMBaseMap: Bool,
            styleURL: URL?
        ) -> String {
            if useOfflineTiles {
                let packKey = offlinePacks.map(\.path).joined(separator: "|")
                return "offline:\(packKey):\(styleURL?.absoluteString ?? "none")"
            }
            return "online:\(useOSMBaseMap):\(styleURL?.absoluteString ?? "none")"
        }

        func syncMapContent(
            on mapView: MLNMapView,
            pins: [SemayMapPin],
            businesses: [BusinessProfile],
            routes: [SemayCuratedRoute],
            services: [SemayServiceDirectoryEntry]
        ) {
            if let annotations = mapView.annotations, !annotations.isEmpty {
                mapView.removeAnnotations(annotations)
            }

            var next: [SemayMapLibreEntityAnnotation] = []
            next.append(contentsOf: pins.map { pin in
                SemayMapLibreEntityAnnotation(
                    entityID: pin.pinID,
                    kind: .pin,
                    coordinate: CLLocationCoordinate2D(latitude: pin.latitude, longitude: pin.longitude),
                    title: pin.name,
                    subtitle: pin.type
                )
            })
            next.append(contentsOf: businesses.filter { $0.latitude != 0 || $0.longitude != 0 }.map { business in
                SemayMapLibreEntityAnnotation(
                    entityID: business.businessID,
                    kind: .business,
                    coordinate: CLLocationCoordinate2D(latitude: business.latitude, longitude: business.longitude),
                    title: business.name,
                    subtitle: business.category
                )
            })
            next.append(contentsOf: routes.compactMap { route in
                guard let lat = route.centerLatitude, let lon = route.centerLongitude else { return nil }
                return SemayMapLibreEntityAnnotation(
                    entityID: route.routeID,
                    kind: .route,
                    coordinate: CLLocationCoordinate2D(latitude: lat, longitude: lon),
                    title: route.title,
                    subtitle: route.city
                )
            })
            next.append(contentsOf: services.filter { $0.latitude != 0 || $0.longitude != 0 }.map { service in
                SemayMapLibreEntityAnnotation(
                    entityID: service.serviceID,
                    kind: .service,
                    coordinate: CLLocationCoordinate2D(latitude: service.latitude, longitude: service.longitude),
                    title: service.name,
                    subtitle: service.serviceType
                )
            })

            if !next.isEmpty {
                mapView.addAnnotations(next)
            }
        }
    }
}

private final class SemayMapLibreEntityAnnotation: MLNPointAnnotation {
    enum Kind {
        case pin
        case business
        case route
        case service
    }

    let entityID: String
    let kind: Kind

    init(entityID: String, kind: Kind, coordinate: CLLocationCoordinate2D, title: String, subtitle: String) {
        self.entityID = entityID
        self.kind = kind
        super.init()
        self.coordinate = coordinate
        self.title = title
        self.subtitle = subtitle
    }
}
#else
private struct SemayMapLibreView: View {
    @Binding var region: MKCoordinateRegion
    let pins: [SemayMapPin]
    @Binding var selectedPinID: String?
    let businesses: [BusinessProfile]
    @Binding var selectedBusinessID: String?
    let routes: [SemayCuratedRoute]
    @Binding var selectedRouteID: String?
    let services: [SemayServiceDirectoryEntry]
    @Binding var selectedServiceID: String?
    let offlinePacks: [OfflineTilePack]
    let useOfflineTiles: Bool
    @Binding var useOSMBaseMap: Bool
    let onLongPress: (CLLocationCoordinate2D) -> Void
    let onRuntimeError: (String) -> Void

    var body: some View {
        SemayMapKitView(
            region: $region,
            pins: pins,
            selectedPinID: $selectedPinID,
            businesses: businesses,
            selectedBusinessID: $selectedBusinessID,
            routes: routes,
            selectedRouteID: $selectedRouteID,
            services: services,
            selectedServiceID: $selectedServiceID,
            useOSMBaseMap: $useOSMBaseMap,
            offlinePacks: offlinePacks,
            useOfflineTiles: .constant(useOfflineTiles),
            onLongPress: onLongPress
        )
        .onAppear {
            onRuntimeError("maplibre-module-unavailable")
        }
    }
}
#endif

private struct SemayMapKitView: UIViewRepresentable {
    @Binding var region: MKCoordinateRegion
    let pins: [SemayMapPin]
    @Binding var selectedPinID: String?
    let businesses: [BusinessProfile]
    @Binding var selectedBusinessID: String?
    let routes: [SemayCuratedRoute]
    @Binding var selectedRouteID: String?
    let services: [SemayServiceDirectoryEntry]
    @Binding var selectedServiceID: String?
    @Binding var useOSMBaseMap: Bool
    let offlinePacks: [OfflineTilePack]
    @Binding var useOfflineTiles: Bool
    let onLongPress: (CLLocationCoordinate2D) -> Void

    func makeUIView(context: Context) -> MKMapView {
        let mapView = MKMapView(frame: .zero)
        mapView.delegate = context.coordinator
        mapView.isRotateEnabled = false
        mapView.showsCompass = true
        mapView.showsScale = true
        mapView.setRegion(region, animated: false)
        let longPress = UILongPressGestureRecognizer(target: context.coordinator, action: #selector(Coordinator.handleLongPress(_:)))
        longPress.minimumPressDuration = 0.5
        mapView.addGestureRecognizer(longPress)
        context.coordinator.applyBaseLayer(
            to: mapView,
            useOSM: useOSMBaseMap,
            offlinePacks: offlinePacks,
            useOfflineTiles: useOfflineTiles
        )
        return mapView
    }

    func updateUIView(_ mapView: MKMapView, context: Context) {
        if !context.coordinator.isRegionSimilar(mapView.region, region) {
            mapView.setRegion(region, animated: true)
        }
        context.coordinator.applyBaseLayer(
            to: mapView,
            useOSM: useOSMBaseMap,
            offlinePacks: offlinePacks,
            useOfflineTiles: useOfflineTiles
        )
        context.coordinator.syncMapContent(
            on: mapView,
            pins: pins,
            businesses: businesses,
            routes: routes,
            services: services
        )
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    final class Coordinator: NSObject, MKMapViewDelegate {
        private let parent: SemayMapKitView
        private let osmOverlay = MKTileOverlay(
            urlTemplate: "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        )
        private var offlineOverlays: [String: MBTilesOverlay] = [:]

        init(_ parent: SemayMapKitView) {
            self.parent = parent
            super.init()
            osmOverlay.canReplaceMapContent = true
        }

        @objc func handleLongPress(_ gesture: UILongPressGestureRecognizer) {
            guard gesture.state == .began else { return }
            guard let mapView = gesture.view as? MKMapView else { return }
            let point = gesture.location(in: mapView)
            let coordinate = mapView.convert(point, toCoordinateFrom: mapView)
            parent.onLongPress(coordinate)
        }

        func applyBaseLayer(
            to mapView: MKMapView,
            useOSM: Bool,
            offlinePacks: [OfflineTilePack],
            useOfflineTiles: Bool
        ) {
            let orderedOfflinePacks = orderedPacksForOverlay(offlinePacks)
            if useOfflineTiles, !orderedOfflinePacks.isEmpty {
                let activePaths = Set(orderedOfflinePacks.map(\.path))
                removeStaleOfflineOverlays(activePaths: activePaths)
                mapView.removeOverlays(currentOfflineOverlays(in: mapView))
                for pack in orderedOfflinePacks {
                    let overlay = ensureOfflineOverlay(for: pack)
                    mapView.addOverlay(overlay, level: .aboveLabels)
                }
                if mapView.overlays.contains(where: { $0 === osmOverlay }) {
                    mapView.removeOverlay(osmOverlay)
                }
                mapView.showsCompass = true
                return
            }

            let hasOverlay = mapView.overlays.contains { $0 === osmOverlay }
            if useOSM {
                if !hasOverlay {
                    mapView.addOverlay(osmOverlay, level: .aboveLabels)
                }
            } else if hasOverlay {
                mapView.removeOverlay(osmOverlay)
            }

            mapView.removeOverlays(currentOfflineOverlays(in: mapView))
        }

        private func orderedPacksForOverlay(_ packs: [OfflineTilePack]) -> [OfflineTilePack] {
            packs.sorted {
                if $0.minZoom != $1.minZoom {
                    return $0.minZoom < $1.minZoom
                }
                if $0.maxZoom != $1.maxZoom {
                    return $0.maxZoom < $1.maxZoom
                }
                return $0.path < $1.path
            }
        }

        private func ensureOfflineOverlay(for pack: OfflineTilePack) -> MBTilesOverlay {
            if let cached = offlineOverlays[pack.path] {
                return cached
            }
            let overlay = MBTilesOverlay(path: pack.path, minZoom: pack.minZoom, maxZoom: pack.maxZoom)
            overlay.canReplaceMapContent = true
            offlineOverlays[pack.path] = overlay
            return overlay
        }

        private func currentOfflineOverlays(in mapView: MKMapView) -> [MKTileOverlay] {
            mapView.overlays.compactMap { overlay in
                guard overlay !== osmOverlay else { return nil }
                return overlay as? MKTileOverlay
            }
        }

        private func removeStaleOfflineOverlays(activePaths: Set<String>) {
            offlineOverlays = offlineOverlays.filter { activePaths.contains($0.key) }
        }

        func syncMapContent(
            on mapView: MKMapView,
            pins: [SemayMapPin],
            businesses: [BusinessProfile],
            routes: [SemayCuratedRoute],
            services: [SemayServiceDirectoryEntry]
        ) {
            let existingPins = mapView.annotations.compactMap { $0 as? SemayPinAnnotation }
            let existingPinIDs = Set(existingPins.map(\.pinID))
            let pinIDs = Set(pins.map(\.pinID))

            let pinsToRemove = existingPins.filter { !pinIDs.contains($0.pinID) }
            if !pinsToRemove.isEmpty {
                mapView.removeAnnotations(pinsToRemove)
            }

            let newPins = pins.filter { !existingPinIDs.contains($0.pinID) }
            let newPinAnnotations = newPins.map { SemayPinAnnotation(pin: $0) }
            if !newPinAnnotations.isEmpty {
                mapView.addAnnotations(newPinAnnotations)
            }

            let existingBusinesses = mapView.annotations.compactMap { $0 as? SemayBusinessAnnotation }
            let existingBusinessIDs = Set(existingBusinesses.map(\.businessID))
            let validBusinesses = businesses.filter { $0.latitude != 0 || $0.longitude != 0 }
            let businessIDs = Set(validBusinesses.map(\.businessID))

            let businessesToRemove = existingBusinesses.filter { !businessIDs.contains($0.businessID) }
            if !businessesToRemove.isEmpty {
                mapView.removeAnnotations(businessesToRemove)
            }

            let newBusinesses = validBusinesses.filter { !existingBusinessIDs.contains($0.businessID) }
            let newBusinessAnnotations = newBusinesses.map { SemayBusinessAnnotation(business: $0) }
            if !newBusinessAnnotations.isEmpty {
                mapView.addAnnotations(newBusinessAnnotations)
            }

            syncRouteOverlays(on: mapView, routes: routes)
            syncRouteAnchors(on: mapView, routes: routes)
            syncServiceAnnotations(on: mapView, services: services)
        }

        private func syncRouteAnchors(on mapView: MKMapView, routes: [SemayCuratedRoute]) {
            let existingRouteAnchors = mapView.annotations.compactMap { $0 as? SemayRouteAnchorAnnotation }
            let existingRouteIDs = Set(existingRouteAnchors.map(\.routeID))
            let activeRoutes = routes.filter { !$0.waypoints.isEmpty }
            let routeIDs = Set(activeRoutes.map(\.routeID))

            let stale = existingRouteAnchors.filter { !routeIDs.contains($0.routeID) }
            if !stale.isEmpty {
                mapView.removeAnnotations(stale)
            }

            let newAnchors = activeRoutes
                .filter { !existingRouteIDs.contains($0.routeID) }
                .map { SemayRouteAnchorAnnotation(route: $0) }
            if !newAnchors.isEmpty {
                mapView.addAnnotations(newAnchors)
            }
        }

        private func syncRouteOverlays(on mapView: MKMapView, routes: [SemayCuratedRoute]) {
            let existingRouteOverlays = mapView.overlays.compactMap { $0 as? SemayRoutePolyline }
            if !existingRouteOverlays.isEmpty {
                mapView.removeOverlays(existingRouteOverlays)
            }

            let overlays = routes.flatMap { route -> [SemayRoutePolyline] in
                guard route.waypoints.count >= 2 else { return [] }
                let coordinates: [CLLocationCoordinate2D] = route.waypoints.compactMap { waypoint -> CLLocationCoordinate2D? in
                    guard CLLocationCoordinate2DIsValid(CLLocationCoordinate2D(latitude: waypoint.latitude, longitude: waypoint.longitude)) else {
                        return nil
                    }
                    return CLLocationCoordinate2D(latitude: waypoint.latitude, longitude: waypoint.longitude)
                }
                guard coordinates.count >= 2 else { return [] }
                return [SemayRoutePolyline(
                    routeID: route.routeID,
                    transportType: route.transportType,
                    trustScore: route.trustScore,
                    coordinates: coordinates
                )]
            }
            if !overlays.isEmpty {
                mapView.addOverlays(overlays, level: .aboveRoads)
            }
        }

        private func syncServiceAnnotations(on mapView: MKMapView, services: [SemayServiceDirectoryEntry]) {
            let existingServices = mapView.annotations.compactMap { $0 as? SemayDirectoryServiceAnnotation }
            let existingServiceIDs = Set(existingServices.map(\.serviceID))
            let mappedServices = services.filter { $0.latitude != 0 || $0.longitude != 0 }
            let serviceIDs = Set(mappedServices.map(\.serviceID))

            let stale = existingServices.filter { !serviceIDs.contains($0.serviceID) }
            if !stale.isEmpty {
                mapView.removeAnnotations(stale)
            }

            let new = mappedServices
                .filter { !existingServiceIDs.contains($0.serviceID) }
                .map { SemayDirectoryServiceAnnotation(service: $0) }
            if !new.isEmpty {
                mapView.addAnnotations(new)
            }
        }

        func mapView(_ mapView: MKMapView, didSelect view: MKAnnotationView) {
            if let pin = view.annotation as? SemayPinAnnotation {
                parent.selectedBusinessID = nil
                parent.selectedPinID = pin.pinID
                parent.selectedRouteID = nil
                parent.selectedServiceID = nil
                return
            }
            if let business = view.annotation as? SemayBusinessAnnotation {
                parent.selectedPinID = nil
                parent.selectedBusinessID = business.businessID
                parent.selectedRouteID = nil
                parent.selectedServiceID = nil
                return
            }
            if let route = view.annotation as? SemayRouteAnchorAnnotation {
                parent.selectedPinID = nil
                parent.selectedBusinessID = nil
                parent.selectedRouteID = route.routeID
                parent.selectedServiceID = nil
                return
            }
            if let service = view.annotation as? SemayDirectoryServiceAnnotation {
                parent.selectedPinID = nil
                parent.selectedBusinessID = nil
                parent.selectedRouteID = nil
                parent.selectedServiceID = service.serviceID
                return
            }
        }

        func mapView(_ mapView: MKMapView, didDeselect view: MKAnnotationView) {
            if view.annotation is SemayPinAnnotation {
                parent.selectedPinID = nil
            }
            if view.annotation is SemayBusinessAnnotation {
                parent.selectedBusinessID = nil
            }
            if view.annotation is SemayRouteAnchorAnnotation {
                parent.selectedRouteID = nil
            }
            if view.annotation is SemayDirectoryServiceAnnotation {
                parent.selectedServiceID = nil
            }
        }

        func mapView(_ mapView: MKMapView, regionDidChangeAnimated animated: Bool) {
            parent.region = mapView.region
        }

        func mapView(_ mapView: MKMapView, viewFor annotation: MKAnnotation) -> MKAnnotationView? {
            if let pin = annotation as? SemayPinAnnotation {
                let identifier = "SemayPin"
                let view = mapView.dequeueReusableAnnotationView(withIdentifier: identifier)
                    as? MKMarkerAnnotationView ?? MKMarkerAnnotationView(annotation: annotation, reuseIdentifier: identifier)
                view.annotation = annotation
                view.markerTintColor = pin.isVisible ? .systemGreen : .systemOrange
                view.glyphImage = UIImage(systemName: pin.isVisible ? "mappin.circle.fill" : "mappin.slash.circle.fill")
                view.canShowCallout = true
                return view
            }

            if annotation is SemayBusinessAnnotation {
                let identifier = "SemayBusiness"
                let view = mapView.dequeueReusableAnnotationView(withIdentifier: identifier)
                    as? MKMarkerAnnotationView ?? MKMarkerAnnotationView(annotation: annotation, reuseIdentifier: identifier)
                view.annotation = annotation
                view.markerTintColor = .systemBlue
                view.glyphImage = UIImage(systemName: "building.2.fill")
                view.canShowCallout = true
                return view
            }

            if let route = annotation as? SemayRouteAnchorAnnotation {
                let identifier = "SemayRouteAnchor"
                let view = mapView.dequeueReusableAnnotationView(withIdentifier: identifier)
                    as? MKMarkerAnnotationView ?? MKMarkerAnnotationView(annotation: annotation, reuseIdentifier: identifier)
                view.annotation = annotation
                view.markerTintColor = routeTrustColor(route.trustScore)
                view.glyphImage = UIImage(systemName: routeTransportGlyph(route.transportType))
                view.canShowCallout = true
                return view
            }

            if let service = annotation as? SemayDirectoryServiceAnnotation {
                let identifier = "SemayDirectoryService"
                let view = mapView.dequeueReusableAnnotationView(withIdentifier: identifier)
                    as? MKMarkerAnnotationView ?? MKMarkerAnnotationView(annotation: annotation, reuseIdentifier: identifier)
                view.annotation = annotation
                view.markerTintColor = service.verified ? .systemGreen : .systemTeal
                view.glyphImage = UIImage(systemName: "person.2.wave.2")
                view.canShowCallout = true
                return view
            }

            return nil
        }

        func mapView(_ mapView: MKMapView, rendererFor overlay: MKOverlay) -> MKOverlayRenderer {
            if let tileOverlay = overlay as? MKTileOverlay {
                return MKTileOverlayRenderer(tileOverlay: tileOverlay)
            }
            if let routeOverlay = overlay as? SemayRoutePolyline {
                let renderer = MKPolylineRenderer(polyline: routeOverlay)
                let selected = routeOverlay.routeID == parent.selectedRouteID
                renderer.strokeColor = routeTraceColor(
                    trustScore: routeOverlay.trustScore,
                    selected: selected
                )
                renderer.lineWidth = selected ? 7 : 5
                renderer.lineCap = .round
                renderer.lineJoin = .round
                return renderer
            }
            return MKOverlayRenderer(overlay: overlay)
        }

        private func routeTrustColor(_ score: Int) -> UIColor {
            switch score {
            case 80...:
                return .systemGreen
            case 60...79:
                return .systemBlue
            case 40...59:
                return .systemOrange
            default:
                return .systemRed
            }
        }

        private func routeTraceColor(trustScore: Int, selected: Bool) -> UIColor {
            let base = routeTrustColor(trustScore)
            return selected ? base : base.withAlphaComponent(0.8)
        }

        private func routeTransportGlyph(_ raw: String) -> String {
            let transport = SemayRouteTransport(rawValue: raw.lowercased()) ?? .unknown
            switch transport {
            case .walk:
                return "figure.walk"
            case .bus:
                return "bus"
            case .taxi:
                return "car.front.wheels"
            case .car:
                return "car.fill"
            case .train:
                return "tram.fill"
            case .mixed:
                return "arrow.triangle.branch"
            case .unknown:
                return "map"
            }
        }

        func isRegionSimilar(_ lhs: MKCoordinateRegion, _ rhs: MKCoordinateRegion) -> Bool {
            let latDiff = abs(lhs.center.latitude - rhs.center.latitude)
            let lonDiff = abs(lhs.center.longitude - rhs.center.longitude)
            let latSpanDiff = abs(lhs.span.latitudeDelta - rhs.span.latitudeDelta)
            let lonSpanDiff = abs(lhs.span.longitudeDelta - rhs.span.longitudeDelta)
            return latDiff < 0.0005 && lonDiff < 0.0005 && latSpanDiff < 0.0005 && lonSpanDiff < 0.0005
        }
    }
}

private final class SemayPinAnnotation: NSObject, MKAnnotation {
    let pinID: String
    let coordinate: CLLocationCoordinate2D
    let title: String?
    let subtitle: String?
    let isVisible: Bool

    init(pin: SemayMapPin) {
        self.pinID = pin.pinID
        self.coordinate = CLLocationCoordinate2D(latitude: pin.latitude, longitude: pin.longitude)
        self.title = pin.name
        self.subtitle = pin.type
        self.isVisible = pin.isVisible
        super.init()
    }
}

private final class SemayBusinessAnnotation: NSObject, MKAnnotation {
    let businessID: String
    let coordinate: CLLocationCoordinate2D
    let title: String?
    let subtitle: String?

    init(business: BusinessProfile) {
        self.businessID = business.businessID
        self.coordinate = CLLocationCoordinate2D(latitude: business.latitude, longitude: business.longitude)
        self.title = business.name
        self.subtitle = business.category
        super.init()
    }
}

private final class SemayRouteAnchorAnnotation: NSObject, MKAnnotation {
    let routeID: String
    let transportType: String
    let trustScore: Int
    let coordinate: CLLocationCoordinate2D
    let title: String?
    let subtitle: String?

    init(route: SemayCuratedRoute) {
        self.routeID = route.routeID
        self.transportType = route.transportType
        self.trustScore = route.trustScore
        let center = route.centerLatitude ?? 15.3229
        let point = route.centerLongitude ?? 38.9251
        self.coordinate = CLLocationCoordinate2D(latitude: center, longitude: point)
        self.title = route.title
        self.subtitle = "\(route.city) • \(route.fromLabel) → \(route.toLabel)"
        super.init()
    }
}

private final class SemayDirectoryServiceAnnotation: NSObject, MKAnnotation {
    let serviceID: String
    let verified: Bool
    let coordinate: CLLocationCoordinate2D
    let title: String?
    let subtitle: String?

    init(service: SemayServiceDirectoryEntry) {
        self.serviceID = service.serviceID
        self.verified = service.verified
        self.coordinate = CLLocationCoordinate2D(latitude: service.latitude, longitude: service.longitude)
        self.title = service.name
        self.subtitle = "\(service.serviceType) • \(service.city)"
        super.init()
    }
}

private final class SemayRoutePolyline: MKPolyline {
    let routeID: String
    let transportType: String
    let trustScore: Int

    init(routeID: String, transportType: String, trustScore: Int, coordinates: [CLLocationCoordinate2D]) {
        self.routeID = routeID
        self.transportType = transportType
        self.trustScore = trustScore
        super.init(coordinates: coordinates, count: coordinates.count)
    }
}

private final class MBTilesOverlay: MKTileOverlay {
    private let dbPath: String
    private let minZoomLevel: Int
    private let maxZoomLevel: Int
    private var db: OpaquePointer?
    private let dbQueue = DispatchQueue(label: "semay.mbtiles.db", qos: .utility)
    private let dbQueueKey = DispatchSpecificKey<UInt8>()
    private let dbQueueValue: UInt8 = 1

    init(path: String, minZoom: Int, maxZoom: Int) {
        self.dbPath = path
        self.minZoomLevel = minZoom
        self.maxZoomLevel = maxZoom
        super.init(urlTemplate: nil)
        tileSize = CGSize(width: 256, height: 256)

        // Keep a single read-only connection open to avoid the overhead of opening SQLite per tile.
        // Tile reads are serialized on dbQueue to keep sqlite usage simple and reliable.
        dbQueue.setSpecific(key: dbQueueKey, value: dbQueueValue)
        dbQueue.async { [weak self] in
            self?.openDBIfNeeded()
        }
    }

    deinit {
        // Ensure we don't close the DB while a tile read is in-flight.
        if DispatchQueue.getSpecific(key: dbQueueKey) == dbQueueValue {
            closeDB()
        } else {
            dbQueue.sync {
                closeDB()
            }
        }
    }

    override func loadTile(at path: MKTileOverlayPath, result: @escaping (Data?, Error?) -> Void) {
        if path.z < minZoomLevel || path.z > maxZoomLevel {
            result(nil, nil)
            return
        }

        dbQueue.async {
            self.openDBIfNeeded()
            let data = self.readTile(z: path.z, x: path.x, y: path.y)
            result(data, nil)
        }
    }

    private func closeDB() {
        if let db {
            sqlite3_close(db)
        }
        db = nil
    }

    private func openDBIfNeeded() {
        precondition(DispatchQueue.getSpecific(key: dbQueueKey) == dbQueueValue)
        if db != nil { return }
        var conn: OpaquePointer?
        if sqlite3_open_v2(dbPath, &conn, SQLITE_OPEN_READONLY, nil) == SQLITE_OK {
            db = conn
        } else {
            if conn != nil {
                sqlite3_close(conn)
            }
            db = nil
        }
    }

    private func readTile(z: Int, x: Int, y: Int) -> Data? {
        precondition(DispatchQueue.getSpecific(key: dbQueueKey) == dbQueueValue)
        guard let db else {
            return nil
        }

        let tmsY = (1 << z) - 1 - y
        let sql = "SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ? LIMIT 1"
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            sqlite3_finalize(stmt)
            return nil
        }
        defer { sqlite3_finalize(stmt) }

        sqlite3_bind_int(stmt, 1, Int32(z))
        sqlite3_bind_int(stmt, 2, Int32(x))
        sqlite3_bind_int(stmt, 3, Int32(tmsY))

        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard let blob = sqlite3_column_blob(stmt, 0) else { return nil }
        let size = sqlite3_column_bytes(stmt, 0)
        return Data(bytes: blob, count: Int(size))
    }
}
#endif

private struct SemayExploreSheet: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.openURL) private var openURL
    @Binding var isPresented: Bool
    @Binding var region: MKCoordinateRegion
    let pins: [SemayMapPin]
    let businesses: [BusinessProfile]
    let routes: [SemayCuratedRoute]
    let services: [SemayServiceDirectoryEntry]
    let bulletins: [BulletinPost]
    @ObservedObject var libraryStore: LibraryPackStore
    @Binding var selectedPinID: String?
    @Binding var selectedBusinessID: String?
    @Binding var selectedRouteID: String?
    @Binding var selectedServiceID: String?
    @StateObject private var reachability = NetworkReachabilityService.shared

    @State private var query: String = ""
    @State private var segment: Segment = .places
    @State private var installingLibraryPack = false
    @State private var libraryError: String?
    @State private var readerItem: SemayLibraryItem?
    @State private var showBulletinComposer = false
    @State private var bulletinActionMessage: String?
    @State private var exploreActionMessage: String?
    @State private var routeCityFilter: String = "All"
    @State private var serviceCityFilter: String = "All"
    @State private var serviceTypeFilter: String = "All"
    @State private var serviceUrgencyFilter: String = "All"
    @State private var verifiedServiceOnly = false
    @State private var bulletinCategoryFilter: String = "All"

    private enum Segment: String, CaseIterable, Identifiable {
        case places = "Places"
        case businesses = "Businesses"
        case routes = "Routes"
        case bulletins = "Bulletins"
        case library = "Library"
        case services = "Services"

        var id: String { rawValue }
    }

    var body: some View {
        NavigationStack {
            VStack(spacing: 12) {
                VStack(spacing: 10) {
                    TextField("Search places, businesses, routes, services, plus codes, or E-address", text: $query)
                        .textFieldStyle(.roundedBorder)
                        .semayDisableAutoCaps()
                        .semayDisableAutocorrection()

                    Picker("Explore", selection: $segment) {
                        ForEach(Segment.allCases) { seg in
                            Text(seg.rawValue).tag(seg)
                        }
                    }
                    .pickerStyle(.segmented)
                }
                .padding(.horizontal, 16)
                .padding(.top, 12)

                List {
                    if let jump = plusCodeJump {
                        Section {
                            Button {
                                selectedPinID = nil
                                selectedBusinessID = nil
                                selectedRouteID = nil
                                selectedServiceID = nil
                                focus(area: jump.area)
                                isPresented = false
                            } label: {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text("Go to \(jump.code)")
                                        .font(.headline)
                                    Text("Jump to location from Plus Code")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                            }
                        } header: {
                            Text("Location")
                        }
                    }

                    switch segment {
                    case .places:
                        placesSection
                    case .businesses:
                        businessesSection
                    case .routes:
                        routesSection
                    case .bulletins:
                        bulletinsSection
                    case .library:
                        librarySection
                    case .services:
                        servicesSection
                    }
                }
                .listStyle(.plain)
            }
            .navigationTitle("Explore")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Close") { isPresented = false }
                }
            }
            .sheet(item: $readerItem) { item in
                SemayLibraryReaderSheet(item: item)
            }
            .sheet(isPresented: $showBulletinComposer) {
                SemayBulletinComposerSheet(isPresented: $showBulletinComposer)
                    .environmentObject(dataStore)
            }
            .alert("Explore", isPresented: Binding(
                get: { exploreActionMessage != nil },
                set: { if !$0 { exploreActionMessage = nil } }
            )) {
                Button("OK") { exploreActionMessage = nil }
            } message: {
                if let exploreActionMessage {
                    Text(exploreActionMessage)
                }
            }
            .alert("Explore", isPresented: Binding(
                get: { bulletinActionMessage != nil },
                set: { if !$0 { bulletinActionMessage = nil } }
            )) {
                Button("OK") { bulletinActionMessage = nil }
            } message: {
                if let bulletinActionMessage {
                    Text(bulletinActionMessage)
                }
            }
        }
    }

    private var normalizedQuery: String {
        query.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private var searchQueryVariants: Set<String> {
        searchVariants(for: normalizedQuery)
    }

    private var plusCodeJump: (code: String, area: OpenLocationCode.Area)? {
        let raw = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !raw.isEmpty else { return nil }
        let compact = raw.uppercased().filter { !$0.isWhitespace }
        guard let area = OpenLocationCode.decode(compact) else { return nil }
        let canonical = OpenLocationCode.encode(latitude: area.centerLatitude, longitude: area.centerLongitude, codeLength: 10)
        return (canonical, area)
    }

    private var routeCityOptions: [String] {
        let cities = Set(routes.compactMap { $0.city.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }.filter { !$0.isEmpty })
        return ["All"] + cities.sorted().map { $0.capitalized }
    }

    private var serviceCityOptions: [String] {
        let cities = Set(
            services.compactMap { service in
                let city = service.city.trimmingCharacters(in: .whitespacesAndNewlines)
                let country = service.country.trimmingCharacters(in: .whitespacesAndNewlines)
                let key = [city, country].filter { !$0.isEmpty }.joined(separator: ",")
                return key.isEmpty ? nil : key.lowercased()
            }
        )
        return ["All"] + cities.sorted().map { $0.capitalized }
    }

    private var serviceTypeOptions: [String] {
        let types = Set(services.map { $0.serviceType.lowercased().trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty })
        return ["All"] + types.sorted().map { $0.capitalized }
    }

    private var urgencyOptions: [String] {
        ["All"] + Set(services.compactMap { service in
            let raw = service.urgency.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
            return raw.isEmpty ? nil : raw.capitalized
        }).sorted()
    }

    private var bulletinCategoryOptions: [String] {
        let categories = Set(bulletins.map { $0.category.title })
        return ["All"] + categories.sorted()
    }

    private var filteredPins: [SemayMapPin] {
        let queryVariants = searchQueryVariants
        if queryVariants.isEmpty { return pins }
        return pins.filter { pin in
            return hasQueryMatch(
                textFields: [
                    pin.name,
                    pin.type,
                    pin.details,
                    pin.eAddress,
                    pin.plusCode
                ],
                queryVariants: queryVariants
            )
        }
    }

    private var filteredBusinesses: [BusinessProfile] {
        let queryVariants = searchQueryVariants
        if queryVariants.isEmpty { return businesses }
        return businesses.filter { b in
            hasQueryMatch(
                textFields: [
                    b.name,
                    b.category,
                    b.details,
                    b.eAddress,
                    b.plusCode,
                    b.phone
                ],
                queryVariants: queryVariants
            )
        }
    }

    private var filteredBulletins: [BulletinPost] {
        let queryVariants = searchQueryVariants
        var result = bulletins

        if !queryVariants.isEmpty {
            result = result.filter { item in
                hasQueryMatch(
                    textFields: [
                        item.title,
                        item.body,
                        item.category.rawValue,
                        item.plusCode,
                        item.eAddress,
                        item.phone
                    ],
                    queryVariants: queryVariants
                )
            }
        }

        if bulletinCategoryFilter != "All" {
            result = result.filter { $0.category.title == bulletinCategoryFilter }
        }

        return result.sorted {
            if $0.updatedAt != $1.updatedAt { return $0.updatedAt > $1.updatedAt }
            return $0.createdAt > $1.createdAt
        }
    }

    private func routeSearchMatches(_ route: SemayCuratedRoute, queryVariants: Set<String>) -> Bool {
        hasQueryMatch(
            textFields: [
                route.title,
                route.summary,
                route.city,
                route.fromLabel,
                route.toLabel,
                route.transportType
            ],
            queryVariants: queryVariants
        )
    }

    private func serviceSearchMatches(_ entry: SemayServiceDirectoryEntry, queryVariants: Set<String>) -> Bool {
        hasQueryMatch(
            textFields: [
                entry.name,
                entry.serviceType,
                entry.category,
                entry.details,
                entry.city,
                entry.country,
                entry.addressLabel,
                entry.locality,
                entry.adminArea,
                entry.countryCode,
                entry.plusCode,
                entry.eAddress,
                entry.phone,
                entry.website
            ],
            queryVariants: queryVariants
        )
    }

    private func hasQueryMatch(textFields: [String], queryVariants: Set<String>) -> Bool {
        if queryVariants.isEmpty { return true }
        return textFields.contains { field in
            let fieldVariants = searchVariants(for: field)
            return queryVariants.contains { query in
                fieldVariants.contains(where: { $0.contains(query) })
            }
        }
    }

    private func searchVariants(for rawText: String) -> Set<String> {
        let normalized = normalizeSearchText(rawText)
        if normalized.isEmpty { return [] }

        var variants = Set<String>()
        variants.insert(normalized)
        variants.insert(removeSearchSeparators(normalized))

        let transliterated = transliterateEthiopic(for: rawText)
        let transliteratedNormalized = normalizeSearchText(transliterated)
        if !transliteratedNormalized.isEmpty {
            variants.insert(transliteratedNormalized)
            variants.insert(removeSearchSeparators(transliteratedNormalized))
        }

        return variants
    }

    private func normalizeSearchText(_ rawText: String) -> String {
        let folded = rawText
            .folding(options: [.diacriticInsensitive, .caseInsensitive], locale: .current)
            .lowercased()
        let allowed = CharacterSet.alphanumerics
            .union(.whitespaces)
            .union(CharacterSet(charactersIn: "+-#"))
        let compact = String(folded.unicodeScalars.filter { scalar in
            allowed.contains(scalar)
        })
        return compact
            .replacingOccurrences(of: "  ", with: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func removeSearchSeparators(_ text: String) -> String {
        text.replacingOccurrences(of: " ", with: "")
            .replacingOccurrences(of: "-", with: "")
            .replacingOccurrences(of: "#", with: "")
    }

    private func transliterateEthiopic(for text: String) -> String {
        let map: [Character: String] = [
            "ሀ": "ha", "ሁ": "hu", "ሂ": "hi", "ሃ": "ha", "ሄ": "he", "ህ": "he", "ሆ": "ho",
            "ለ": "le", "ሉ": "lu", "ሊ": "li", "ላ": "la", "ሌ": "le", "ል": "li", "ሎ": "lo",
            "ሐ": "ha", "ሑ": "hu", "ሒ": "hi", "ሓ": "ha", "ሔ": "he", "ሕ": "he", "ሖ": "ho",
            "መ": "me", "ሙ": "mu", "ሚ": "mi", "ማ": "ma", "ሜ": "me", "ም": "mi", "ሞ": "mo",
            "ሠ": "se", "ሡ": "su", "ሢ": "si", "ሣ": "sa", "ሤ": "se", "ሥ": "si", "ሦ": "so",
            "ረ": "re", "ሩ": "ru", "ሪ": "ri", "ራ": "ra", "ሬ": "re", "ር": "ri", "ሮ": "ro",
            "ሰ": "se", "ሱ": "su", "ሲ": "si", "ሳ": "sa", "ሴ": "se", "ስ": "si", "ሶ": "so",
            "ሸ": "she", "ሹ": "shu", "ሺ": "shi", "ሻ": "sha", "ሼ": "she", "ሽ": "shi", "ሾ": "sho",
            "ቀ": "qe", "ቁ": "qu", "ቂ": "qi", "ቃ": "qa", "ቄ": "qe", "ቅ": "qi", "ቆ": "qo",
            "ቈ": "qwa", "ቊ": "qwu", "ቋ": "qwi", "ቌ": "qwe", "ቍ": "qwi",
            "በ": "be", "ቡ": "bu", "ቢ": "bi", "ባ": "ba", "ቤ": "be", "ብ": "bi", "ቦ": "bo",
            "ቨ": "ve", "ቩ": "vu", "ቪ": "vi", "ቫ": "va", "ቬ": "ve", "ቭ": "vi", "ቮ": "vo",
            "ተ": "te", "ቱ": "tu", "ቲ": "ti", "ታ": "ta", "ቴ": "te", "ት": "ti", "ቶ": "to",
            "ቸ": "che", "ቹ": "chu", "ቺ": "chi", "ቻ": "cha", "ቼ": "che", "ች": "chi", "ቾ": "cho",
            "ኀ": "ha", "ኁ": "hu", "ኂ": "hi", "ኃ": "ha", "ኄ": "he", "ኅ": "he", "ኆ": "ho",
            "ነ": "ne", "ኑ": "nu", "ኒ": "ni", "ና": "na", "ኔ": "ne", "ን": "ni", "ኖ": "no",
            "አ": "a", "ኡ": "u", "ኢ": "i", "ኣ": "a", "ኤ": "e", "እ": "e", "ኦ": "o",
            "ከ": "ke", "ኩ": "ku", "ኪ": "ki", "ካ": "ka", "ኬ": "ke", "ክ": "ki", "ኮ": "ko",
            "ወ": "we", "ዉ": "wu", "ዊ": "wi", "ዋ": "wa", "ዌ": "we", "ው": "wi", "ዎ": "wo",
            "ዐ": "a", "ዑ": "u", "ዒ": "i", "ዓ": "a", "ዔ": "e", "ዕ": "e", "ዖ": "o",
            "ዘ": "ze", "ዙ": "zu", "ዚ": "zi", "ዛ": "za", "ዜ": "ze", "ዝ": "zi", "ዞ": "zo",
            "ዠ": "zhe", "ዡ": "zhu", "ዢ": "zhi", "ዣ": "zha", "ዤ": "zhe", "ዥ": "zhi", "ዦ": "zho",
            "የ": "ye", "ዩ": "yu", "ዪ": "yi", "ያ": "ya", "ዬ": "ye", "ይ": "yi", "ዮ": "yo",
            "ደ": "de", "ዱ": "du", "ዲ": "di", "ዳ": "da", "ዴ": "de", "ድ": "di", "ዶ": "do",
            "ገ": "ge", "ጉ": "gu", "ጊ": "gi", "ጋ": "ga", "ጌ": "ge", "ግ": "gi", "ጎ": "go",
            "ጠ": "te", "ጡ": "tu", "ጢ": "ti", "ጣ": "ta", "ጤ": "te", "ጥ": "ti", "ጦ": "to",
            "ጨ": "che", "ጩ": "chu", "ጪ": "chi", "ጫ": "cha", "ጬ": "che", "ጭ": "chi", "ጮ": "cho",
            "ጰ": "pe", "ጱ": "pu", "ጲ": "pi", "ጳ": "pa", "ጴ": "pe", "ጵ": "pi", "ጶ": "po",
            "ጸ": "tse", "ጹ": "tsu", "ጺ": "tsi", "ጻ": "tsa", "ጼ": "tse", "ጽ": "tsi", "ጾ": "tso",
            "ፈ": "fe", "ፉ": "fu", "ፊ": "fi", "ፋ": "fa", "ፌ": "fe", "ፍ": "fi", "ፎ": "fo"
        ]

        return text.reduce(into: "") { result, ch in
            result.append(map[ch] ?? String(ch))
        }
    }

    private var filteredRoutes: [SemayCuratedRoute] {
        let queryVariants = searchQueryVariants
        var result = routes

        if !queryVariants.isEmpty {
            result = result.filter { route in
                routeSearchMatches(route, queryVariants: queryVariants)
            }
        }
        if routeCityFilter != "All" {
            result = result.filter { $0.city.lowercased() == routeCityFilter.lowercased() }
        }

        return result.sorted {
            if $0.trustScore != $1.trustScore {
                return $0.trustScore > $1.trustScore
            }
            return $0.updatedAt > $1.updatedAt
        }
    }

    private var filteredServices: [SemayServiceDirectoryEntry] {
        let queryVariants = searchQueryVariants
        var result = services

        if !queryVariants.isEmpty {
            result = result.filter { entry in
                serviceSearchMatches(entry, queryVariants: queryVariants)
            }
        }

        if serviceCityFilter != "All" {
            result = result.filter {
                let current = [$0.city, $0.country].map { $0.lowercased() }.filter { !$0.isEmpty }
                let normalizedFilter = serviceCityFilter.lowercased()
                return current.contains(normalizedFilter) || "\(current.joined(separator: ","))".contains(normalizedFilter)
            }
        }

        if serviceTypeFilter != "All" {
            result = result.filter { $0.serviceType.lowercased() == serviceTypeFilter.lowercased() }
        }

        if serviceUrgencyFilter != "All" {
            result = result.filter { $0.urgency.lowercased() == serviceUrgencyFilter.lowercased() }
        }

        if verifiedServiceOnly {
            result = result.filter(\.verified)
        }

        return result.sorted {
            if $0.trustScore != $1.trustScore {
                return $0.trustScore > $1.trustScore
            }
            return $0.updatedAt > $1.updatedAt
        }
    }

    private func routeTransportLabel(_ raw: String) -> String {
        let transport = SemayRouteTransport(rawValue: raw.lowercased()) ?? .unknown
        return transport.title
    }

    @ViewBuilder
    private var routesSection: some View {
        Section {
            if routeCityOptions.count > 1 {
                Picker("City", selection: $routeCityFilter) {
                    ForEach(routeCityOptions, id: \.self) { city in
                        Text(city).tag(city)
                    }
                }
                .pickerStyle(.menu)
                .padding(.vertical, 6)
            }

            if filteredRoutes.isEmpty {
                Text("No routes match your search.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(filteredRoutes) { route in
                    Button {
                        selectedBusinessID = nil
                        selectedPinID = nil
                        selectedServiceID = nil
                        selectedRouteID = route.routeID
                        if let first = route.waypoints.first {
                            focus(latitude: first.latitude, longitude: first.longitude)
                        }
                        isPresented = false
                    } label: {
                        VStack(alignment: .leading, spacing: 4) {
                            HStack {
                                Text(route.title)
                                    .font(.headline)
                                Spacer()
                                Text(route.trustBadge)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                            Text("Route • \(routeTransportLabel(route.transportType)) • \(route.city)")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            if !route.summary.isEmpty {
                                Text(route.summary)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(2)
                            }
                        }
                    }
                    .contextMenu {
                        Button("Endorse as Safe") {
                            let trusted = dataStore.endorseCuratedRoute(routeID: route.routeID, score: 1, reason: "verified")
                            exploreActionMessage = trusted
                                ? "Route endorsed as a safer option."
                                : "Could not save endorsement right now."
                        }
                        Button("Report", role: .destructive) {
                            dataStore.reportCuratedRoute(routeID: route.routeID, reason: "mismatch")
                            exploreActionMessage = "Route report submitted."
                        }
                        if dataStore.currentUserPubkey() == route.authorPubkey.lowercased() {
                            Button("Retract", role: .destructive) {
                                dataStore.retractCuratedRoute(routeID: route.routeID)
                                selectedRouteID = nil
                                exploreActionMessage = "Route marked retracted."
                            }
                        }
                    }
                }
            }
        } header: {
            Text("Curated Routes")
        } footer: {
            Text("Trust-aware routes for transit and safer transit options.")
        }
    }

    @ViewBuilder
    private var servicesSection: some View {
        Section {
            if serviceTypeOptions.count > 1 {
                Picker("Service Type", selection: $serviceTypeFilter) {
                    ForEach(serviceTypeOptions, id: \.self) { value in
                        Text(value).tag(value)
                    }
                }
                .pickerStyle(.menu)
            }

            if urgencyOptions.count > 1 {
                Picker("Urgency", selection: $serviceUrgencyFilter) {
                    ForEach(urgencyOptions, id: \.self) { value in
                        Text(value).tag(value)
                    }
                }
                .pickerStyle(.menu)
            }

            if serviceCityOptions.count > 1 {
                Picker("Area", selection: $serviceCityFilter) {
                    ForEach(serviceCityOptions, id: \.self) { value in
                        Text(value).tag(value)
                    }
                }
                .pickerStyle(.menu)
            }

            Toggle("Only verified services", isOn: $verifiedServiceOnly)

            if filteredServices.isEmpty {
                Text("No services match your search.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(filteredServices) { service in
                    Button {
                        selectedBusinessID = nil
                        selectedPinID = nil
                        selectedRouteID = nil
                        selectedServiceID = service.serviceID
                        if service.latitude != 0 || service.longitude != 0 {
                            focus(latitude: service.latitude, longitude: service.longitude)
                        }
                        isPresented = false
                    } label: {
                        VStack(alignment: .leading, spacing: 4) {
                            HStack {
                                Text(service.name)
                                    .font(.headline)
                                Spacer()
                                if service.verified {
                                    Text("Verified")
                                        .font(.caption2)
                                        .padding(.horizontal, 6)
                                        .padding(.vertical, 2)
                                        .background(.green.opacity(0.16), in: Capsule())
                                }
                            }
                            Text("\(service.serviceType) • \(service.city)")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            if !service.addressLabel.isEmpty {
                                Text(service.addressLabel)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                            Text(service.trustBadge)
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                            Text(shareStatusText(for: service))
                                .font(.caption2)
                                .padding(.horizontal, 8)
                                .padding(.vertical, 3)
                                .background(shareStatusTint(for: service).opacity(0.18), in: Capsule())
                                .foregroundStyle(shareStatusTint(for: service))
                            if !service.details.isEmpty {
                                Text(service.details)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(2)
                            }
                        }
                    }
                    .contextMenu {
                        Button(listingString("semay.listing.action.endorse", "Endorse")) {
                            let trusted = dataStore.endorseServiceDirectoryEntry(serviceID: service.serviceID, score: 1, reason: "verified")
                            exploreActionMessage = trusted
                                ? listingString("semay.listing.message.endorsed", "Service endorsed.")
                                : listingString("semay.listing.message.endorse_failed", "Could not save endorsement right now.")
                        }
                        if dataStore.currentUserPubkey() == service.authorPubkey.lowercased() {
                            Button(listingString("semay.listing.action.keep_personal", "Keep Personal")) {
                                dataStore.setServiceContributionScope(serviceID: service.serviceID, scope: .personal)
                                exploreActionMessage = listingString("semay.listing.message.personal_only", "Listing is now personal-only.")
                            }
                            Button(listingString("semay.listing.action.share_network", "Share to Network")) {
                                let result = dataStore.requestNetworkShareForService(serviceID: service.serviceID)
                                if result.accepted {
                                    exploreActionMessage = listingString(
                                        "semay.listing.message.queued_for_network",
                                        "Listing queued for network sharing."
                                    )
                                } else if result.reasons.isEmpty {
                                    exploreActionMessage = listingString(
                                        "semay.listing.message.share_blocked_generic",
                                        "Share request was blocked by quality checks."
                                    )
                                } else {
                                    let reasons = result.reasons.map(qualityReasonLabel).joined(separator: ", ")
                                    exploreActionMessage = "\(listingString("semay.listing.message.share_blocked_prefix", "Share request blocked")): \(reasons)."
                                }
                            }
                        }
                        Button(listingString("semay.listing.action.report", "Report"), role: .destructive) {
                            dataStore.reportServiceDirectoryEntry(serviceID: service.serviceID, reason: "mismatch")
                            exploreActionMessage = listingString("semay.listing.message.report_submitted", "Service report submitted.")
                        }
                        if dataStore.currentUserPubkey() == service.authorPubkey.lowercased() {
                            Button(listingString("semay.listing.action.retract", "Retract"), role: .destructive) {
                                dataStore.retractServiceDirectoryEntry(serviceID: service.serviceID)
                                selectedServiceID = nil
                                exploreActionMessage = listingString("semay.listing.message.retracted", "Service marked retracted.")
                            }
                        }
                    }
                }
            }
        } header: {
            Text("Service Directory")
        } footer: {
            Text("Community directory with trust indicators and verified tags.")
        }
    }

    @ViewBuilder
    private var placesSection: some View {
        Section {
            if filteredPins.isEmpty {
                Text("No places match your search.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(filteredPins) { pin in
                    Button {
                        selectedBusinessID = nil
                        if let linkedID = dataStore.linkedServiceID(entityType: "pin", entityID: pin.pinID) {
                            selectedServiceID = linkedID
                            selectedPinID = nil
                        } else {
                            selectedPinID = pin.pinID
                            selectedServiceID = nil
                        }
                        focus(latitude: pin.latitude, longitude: pin.longitude)
                        isPresented = false
                    } label: {
                        VStack(alignment: .leading, spacing: 4) {
                            Text(pin.name)
                                .font(.headline)
                            Text("\(pin.type) • \(pin.eAddress)")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            if !pin.plusCode.isEmpty {
                                Text(pin.plusCode)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
            }
        } header: {
            Text("Places")
        }
    }

    @ViewBuilder
    private var businessesSection: some View {
        Section {
            if filteredBusinesses.isEmpty {
                Text("No businesses match your search.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(filteredBusinesses) { business in
                    Button {
                        selectedPinID = nil
                        if let linkedID = dataStore.linkedServiceID(entityType: "business", entityID: business.businessID) {
                            selectedServiceID = linkedID
                            selectedBusinessID = nil
                        } else {
                            selectedBusinessID = business.businessID
                            selectedServiceID = nil
                        }
                        if business.latitude != 0 || business.longitude != 0 {
                            focus(latitude: business.latitude, longitude: business.longitude)
                        }
                        isPresented = false
                    } label: {
                        VStack(alignment: .leading, spacing: 4) {
                            Text(business.name)
                                .font(.headline)
                            Text("\(business.category) • \(business.eAddress)")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            if !business.plusCode.isEmpty {
                                Text(business.plusCode)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                            if !business.phone.isEmpty {
                                Text("Call: \(business.phone)")
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
            }
        } header: {
            Text("Businesses")
        }
    }

    @ViewBuilder
    private var bulletinsSection: some View {
        Section {
            Button {
                showBulletinComposer = true
            } label: {
                Label("Post Bulletin", systemImage: "plus.bubble")
            }

            if bulletinCategoryOptions.count > 1 {
                Picker("Category", selection: $bulletinCategoryFilter) {
                    ForEach(bulletinCategoryOptions, id: \.self) { value in
                        Text(value).tag(value)
                    }
                }
                .pickerStyle(.menu)
            }

            if filteredBulletins.isEmpty {
                Text("No bulletins match your search.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(filteredBulletins) { bulletin in
                    VStack(alignment: .leading, spacing: 6) {
                        HStack {
                            Text(bulletin.title)
                                .font(.headline)
                            Spacer()
                            Text(bulletin.category.title)
                                .font(.caption2)
                                .padding(.horizontal, 8)
                                .padding(.vertical, 4)
                                .background(.thinMaterial, in: Capsule())
                        }
                        Text(bulletin.body)
                            .font(.subheadline)
                        Text("Updated \(Date(timeIntervalSince1970: TimeInterval(bulletin.updatedAt)).formatted(date: .abbreviated, time: .shortened))")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                        if !bulletin.plusCode.isEmpty {
                            Text("\(bulletin.eAddress) • \(bulletin.plusCode)")
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                        HStack {
                            Button("Open") {
                                focus(latitude: bulletin.latitude, longitude: bulletin.longitude)
                                selectedPinID = nil
                                selectedBusinessID = nil
                                isPresented = false
                            }
                            .buttonStyle(.bordered)
                            if let url = telURL(for: bulletin.phone) {
                                Button("Call") {
                                    openURL(url)
                                }
                                .buttonStyle(.borderedProminent)
                            }
                            Menu("More") {
                                if dataStore.isBulletinAuthorMuted(bulletin.authorPubkey) {
                                    Button("Unmute Author") {
                                        dataStore.setBulletinAuthorMuted(bulletin.authorPubkey, muted: false)
                                        bulletinActionMessage = "Author unmuted."
                                    }
                                } else {
                                    Button("Mute Author") {
                                        dataStore.setBulletinAuthorMuted(bulletin.authorPubkey, muted: true)
                                        bulletinActionMessage = "Muted this author."
                                    }
                                }
                                Button("Report") {
                                    dataStore.reportBulletin(bulletinID: bulletin.bulletinID, reason: "community-report")
                                    bulletinActionMessage = "Thanks. Bulletin reported."
                                }
                            }
                        }
                    }
                    .padding(.vertical, 4)
                }
            }
        } header: {
            Text("Bulletins")
        } footer: {
            Text("Community posts for tourism, services, safety, logistics, and opportunities.")
        }
    }

    @ViewBuilder
    private var librarySection: some View {
        Section {
            if libraryStore.packs.isEmpty {
                Text("Library not installed.")
                    .foregroundStyle(.secondary)
                if !reachability.isOnline {
                    Text("Connect to a network to install the library.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Button(installingLibraryPack ? "Installing..." : "Install Library") {
                    Task {
                        installingLibraryPack = true
                        libraryError = nil
                        defer { installingLibraryPack = false }
                        do {
                            _ = try await libraryStore.installRecommendedPack()
                        } catch {
                            libraryError = error.localizedDescription
                        }
                    }
                }
                .disabled(installingLibraryPack || !reachability.isOnline)

                if let libraryError, !libraryError.isEmpty {
                    Text(libraryError)
                        .font(.caption)
                        .foregroundStyle(.orange)
                }
            } else {
                if !libraryStore.packs.isEmpty {
                    Text("Installed packs: \(libraryStore.packs.count)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                ForEach(libraryStore.items) { item in
                    Button {
                        readerItem = item
                    } label: {
                        VStack(alignment: .leading, spacing: 4) {
                            Text(item.title)
                                .font(.headline)
                            if let language = item.language, !language.isEmpty {
                                Text(language.uppercased())
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
            }
        } header: {
            Text("Library")
        }
    }

    private func listingString(_ key: String, _ fallback: String) -> String {
        NSLocalizedString(key, tableName: nil, bundle: .main, value: fallback, comment: "")
    }

    private func shareStatusText(for service: SemayServiceDirectoryEntry) -> String {
        switch service.publishState {
        case .localOnly:
            return listingString("semay.listing.share.personal_only", "Personal only")
        case .pendingReview:
            return listingString("semay.listing.share.queued", "Queued for network")
        case .published:
            return listingString("semay.listing.share.published", "Published to network")
        case .rejected:
            return listingString("semay.listing.share.rejected", "Network share blocked")
        }
    }

    private func shareStatusTint(for service: SemayServiceDirectoryEntry) -> Color {
        switch service.publishState {
        case .localOnly:
            return .secondary
        case .pendingReview:
            return .blue
        case .published:
            return .green
        case .rejected:
            return .orange
        }
    }

    private func qualityReasonLabel(_ key: String) -> String {
        switch key {
        case "missing_required_fields":
            return listingString("semay.listing.reason.missing_required_fields", "missing required fields")
        case "invalid_coordinates":
            return listingString("semay.listing.reason.invalid_coordinates", "invalid coordinates")
        case "possible_duplicate":
            return listingString("semay.listing.reason.possible_duplicate", "possible duplicate listing")
        case "photo_limit_exceeded":
            return listingString("semay.listing.reason.photo_limit_exceeded", "photo limit exceeded")
        case "photo_resolution_low":
            return listingString("semay.listing.reason.photo_resolution_low", "photo resolution too low")
        case "photo_byte_cap_exceeded":
            return listingString("semay.listing.reason.photo_byte_cap_exceeded", "photo size too large")
        case "photo_duplicate_hash":
            return listingString("semay.listing.reason.photo_duplicate_hash", "duplicate photo detected")
        case "author_trust_low":
            return listingString("semay.listing.reason.author_trust_low", "author trust is too low")
        case "author_rate_limited":
            return listingString("semay.listing.reason.author_rate_limited", "author is temporarily rate limited")
        default:
            return key.replacingOccurrences(of: "_", with: " ")
        }
    }

    private func focus(latitude: Double, longitude: Double) {
        region = MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: latitude, longitude: longitude),
            span: MKCoordinateSpan(latitudeDelta: 0.06, longitudeDelta: 0.06)
        )
    }

    private func focus(area: OpenLocationCode.Area) {
        // 10-digit plus codes are already very precise; zoom out slightly for context.
        let latSpan = max(0.02, area.latitudeSpan * 50.0)
        let lonSpan = max(0.02, area.longitudeSpan * 50.0)
        region = MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: area.centerLatitude, longitude: area.centerLongitude),
            span: MKCoordinateSpan(latitudeDelta: latSpan, longitudeDelta: lonSpan)
        )
    }

    private func telURL(for rawPhone: String) -> URL? {
        let cleaned = rawPhone
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .filter { "+0123456789".contains($0) }
        guard !cleaned.isEmpty else { return nil }
        return URL(string: "tel:\(cleaned)")
    }
}

private struct SemayLibraryReaderSheet: View {
    let item: SemayLibraryItem
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            ScrollView {
                Text(item.contentMarkdown)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(16)
            }
            .navigationTitle(item.title)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}

private struct SemayBulletinComposerSheet: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.dismiss) private var dismiss
    @StateObject private var locationState = LocationStateManager.shared

    @Binding var isPresented: Bool
    @State private var title = ""
    @State private var category: BulletinCategory = .general
    @State private var detailsText = ""
    @State private var phone = ""
    @State private var latitude = "15.3229"
    @State private var longitude = "38.9251"
    @State private var showCoordinateEditor = false
    @State private var error: String?

    private var parsedCoordinate: CLLocationCoordinate2D? {
        guard let lat = Double(latitude), let lon = Double(longitude) else { return nil }
        return CLLocationCoordinate2D(latitude: lat, longitude: lon)
    }

    private var computedAddress: (plus: String, e: String) {
        guard let coord = parsedCoordinate else { return ("", "") }
        let addr = SemayAddress.eAddress(latitude: coord.latitude, longitude: coord.longitude)
        return (addr.plusCode, addr.eAddress)
    }

    var body: some View {
        NavigationStack {
            Form {
                TextField("Title", text: $title)
                Picker("Category", selection: $category) {
                    ForEach(BulletinCategory.allCases) { item in
                        Text(item.title).tag(item)
                    }
                }
                TextField("Details", text: $detailsText, axis: .vertical)
                    .lineLimit(4...8)
                TextField("Phone (Optional)", text: $phone)
                    .semayPhoneKeyboard()

                Section("Location") {
                    if let coord = parsedCoordinate {
                        Text(String(format: "Lat %.6f, Lon %.6f", coord.latitude, coord.longitude))
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Button("Use Current Location") {
                        if locationState.permissionState != .authorized {
                            locationState.enableLocationChannels()
                            error = "Enable location access to use your current position."
                            return
                        }
                        guard let loc = locationState.lastKnownLocation else {
                            locationState.refreshChannels()
                            error = "Getting your location. Please try again in a moment."
                            return
                        }
                        latitude = String(format: "%.6f", loc.coordinate.latitude)
                        longitude = String(format: "%.6f", loc.coordinate.longitude)
                        error = nil
                    }

                    Toggle("Edit Coordinates", isOn: $showCoordinateEditor)
                    if showCoordinateEditor {
                        TextField("Latitude", text: $latitude)
                        TextField("Longitude", text: $longitude)
                    }
                }

                if !computedAddress.plus.isEmpty {
                    Section("Address") {
                        Text("Plus Code: \(computedAddress.plus)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text("E-Address: \(computedAddress.e)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }

                if let error {
                    Text(error)
                        .foregroundStyle(.red)
                }
            }
            .navigationTitle("Post Bulletin")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") {
                        isPresented = false
                        dismiss()
                    }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Post") {
                        guard let coord = parsedCoordinate else {
                            error = "Invalid latitude/longitude."
                            return
                        }
                        _ = dataStore.postBulletin(
                            title: title,
                            category: category,
                            body: detailsText,
                            phone: phone,
                            latitude: coord.latitude,
                            longitude: coord.longitude
                        )
                        isPresented = false
                        dismiss()
                    }
                    .disabled(title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || detailsText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || parsedCoordinate == nil)
                }
            }
            .onAppear {
                if locationState.permissionState == .authorized, let loc = locationState.lastKnownLocation {
                    latitude = String(format: "%.6f", loc.coordinate.latitude)
                    longitude = String(format: "%.6f", loc.coordinate.longitude)
                }
            }
        }
    }
}

private struct AddPinSheet: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Binding var isPresented: Bool
    let existingPin: SemayMapPin?
    let initialCoordinate: CLLocationCoordinate2D?
    @StateObject private var locationState = LocationStateManager.shared

    @State private var name = ""
    @State private var type = "shop"
    @State private var details = ""
    @State private var phone = ""
    @State private var latitude = "15.3229"
    @State private var longitude = "38.9251"
    @State private var error: String?
    @State private var showCoordinateEditor = false

    private var parsedCoordinate: CLLocationCoordinate2D? {
        guard let lat = Double(latitude), let lon = Double(longitude) else { return nil }
        return CLLocationCoordinate2D(latitude: lat, longitude: lon)
    }

    private var computedAddress: (plus: String, e: String) {
        guard let coord = parsedCoordinate else { return ("", "") }
        let a = SemayAddress.eAddress(latitude: coord.latitude, longitude: coord.longitude)
        return (a.plusCode, a.eAddress)
    }

    var body: some View {
        NavigationStack {
            Form {
                TextField("Name", text: $name)
                TextField("Type", text: $type)
                TextField("Description", text: $details)
                TextField("Phone (Optional)", text: $phone)
                    .semayPhoneKeyboard()

                Section("Location") {
                    if let coord = parsedCoordinate {
                        Text(String(format: "Lat %.6f, Lon %.6f", coord.latitude, coord.longitude))
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        Text("Location not set")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }

                    Button("Use Current Location") {
                        if locationState.permissionState != .authorized {
                            locationState.enableLocationChannels()
                            error = "Enable location access to use your current position."
                            return
                        }
                        guard let loc = locationState.lastKnownLocation else {
                            locationState.refreshChannels()
                            error = "Getting your location… please try again in a moment."
                            return
                        }
                        latitude = String(format: "%.6f", loc.coordinate.latitude)
                        longitude = String(format: "%.6f", loc.coordinate.longitude)
                        error = nil
                    }

                    Toggle("Edit Coordinates", isOn: $showCoordinateEditor)

                    if showCoordinateEditor {
                        TextField("Latitude", text: $latitude)
                        TextField("Longitude", text: $longitude)
                    }
                }

                if !computedAddress.plus.isEmpty {
                    Section("Address") {
                        Text("Plus Code: \(computedAddress.plus)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text("E-Address: \(computedAddress.e)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }

                if let error {
                    Text(error)
                        .foregroundStyle(.red)
                }
            }
            .navigationTitle(existingPin == nil ? "Add Place" : "Update Place")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { isPresented = false }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") {
                        guard let coord = parsedCoordinate else {
                            error = "Invalid latitude/longitude."
                            return
                        }
                        if let existingPin {
                            let updated = dataStore.updatePin(
                                pinID: existingPin.pinID,
                                name: name,
                                type: type,
                                details: details,
                                latitude: coord.latitude,
                                longitude: coord.longitude,
                                phone: phone
                            )
                            if updated == nil {
                                error = "Unable to update this place."
                                return
                            }
                            isPresented = false
                            return
                        }

                        _ = dataStore.addPin(
                            name: name,
                            type: type,
                            details: details,
                            latitude: coord.latitude,
                            longitude: coord.longitude,
                            phone: phone
                        )
                        isPresented = false
                    }
                    .disabled(name.isEmpty || type.isEmpty || details.isEmpty || parsedCoordinate == nil)
                }
            }
            .onAppear {
                if let existingPin {
                    name = existingPin.name
                    type = existingPin.type
                    details = existingPin.details
                    phone = existingPin.phone
                    latitude = String(format: "%.6f", existingPin.latitude)
                    longitude = String(format: "%.6f", existingPin.longitude)
                } else if let initialCoordinate {
                    latitude = String(format: "%.6f", initialCoordinate.latitude)
                    longitude = String(format: "%.6f", initialCoordinate.longitude)
                }
            }
        }
    }
}

private struct SemayServiceEditorSheet: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.dismiss) private var dismiss
    let existingService: SemayServiceDirectoryEntry

    @State private var name = ""
    @State private var category = ""
    @State private var details = ""
    @State private var phone = ""
    @State private var website = ""
    @State private var error: String?

    var body: some View {
        NavigationStack {
            Form {
                TextField(listingString("semay.listing.editor.field.name", "Listing Name"), text: $name)
                TextField(listingString("semay.listing.editor.field.category", "Category"), text: $category)
                TextField(listingString("semay.listing.editor.field.details", "Description"), text: $details, axis: .vertical)
                    .lineLimit(3...6)
                TextField(listingString("semay.listing.editor.field.phone", "Phone (Optional)"), text: $phone)
                    .semayPhoneKeyboard()
                TextField(listingString("semay.listing.editor.field.website", "Website (Optional)"), text: $website)
                    .semayDisableAutoCaps()
                    .semayDisableAutocorrection()
                    .textInputAutocapitalization(.never)
                    .keyboardType(.URL)

                if let error {
                    Text(error)
                        .foregroundStyle(.red)
                }
            }
            .navigationTitle(listingString("semay.listing.editor.title", "Edit Listing"))
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button(listingString("semay.listing.action.cancel", "Cancel")) {
                        dismiss()
                    }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button(listingString("semay.listing.action.save", "Save")) {
                        let trimmedName = name.trimmingCharacters(in: .whitespacesAndNewlines)
                        let trimmedCategory = category.trimmingCharacters(in: .whitespacesAndNewlines)
                        if trimmedName.isEmpty || trimmedCategory.isEmpty {
                            error = listingString(
                                "semay.listing.editor.error.required_name_category",
                                "Name and category are required."
                            )
                            return
                        }

                        var updated = existingService
                        updated.name = trimmedName
                        updated.category = trimmedCategory
                        updated.details = details.trimmingCharacters(in: .whitespacesAndNewlines)
                        updated.phone = phone.trimmingCharacters(in: .whitespacesAndNewlines)
                        updated.website = website.trimmingCharacters(in: .whitespacesAndNewlines)
                        dataStore.updateServiceDirectoryEntry(updated)
                        dismiss()
                    }
                }
            }
            .onAppear {
                name = existingService.name
                category = existingService.category
                details = existingService.details
                phone = existingService.phone
                website = existingService.website
            }
        }
    }

    private func listingString(_ key: String, _ fallback: String) -> String {
        NSLocalizedString(key, tableName: nil, bundle: .main, value: fallback, comment: "")
    }
}

private struct SemayBusinessTabView: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.openURL) private var openURL

    @State private var showRegisterBusiness = false
    @State private var qrBusiness: BusinessProfile?
    @State private var editingBusiness: BusinessProfile?
    @State private var showQRScanner = false
    @State private var settlementPromise: PromiseNote?
    @State private var promiseQR: PromiseNote?
    @State private var promiseRespond: PromiseNote?

    var body: some View {
        NavigationStack {
            List {
                Section("Businesses") {
                    if dataStore.businesses.isEmpty {
                        Text("Register your first business profile.")
                    } else {
                        ForEach(dataStore.businesses) { business in
                            VStack(alignment: .leading, spacing: 8) {
                                Text(business.name).font(.headline)
                                Text("\(business.category) • \(business.eAddress)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                if !business.plusCode.isEmpty {
                                    Text(business.plusCode)
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                                if !business.phone.isEmpty {
                                    Text("Call: \(business.phone)")
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                                Text(business.details)
                                    .font(.subheadline)
                                if !business.lightningLink.isEmpty || !business.cashuLink.isEmpty {
                                    HStack(spacing: 10) {
                                        if !business.lightningLink.isEmpty {
                                            PaymentChipView(paymentType: .lightning(business.lightningLink))
                                        }
                                        if !business.cashuLink.isEmpty {
                                            PaymentChipView(paymentType: .cashu(business.cashuLink))
                                        }
                                    }
                                }
                                HStack {
                                    if let url = telURL(for: business.phone) {
                                        Button("Call") {
                                            openURL(url)
                                        }
                                        .buttonStyle(.borderedProminent)
                                    }
                                    Button("Directions") {
                                        openDirections(latitude: business.latitude, longitude: business.longitude, name: business.name)
                                    }
                                    .buttonStyle(.bordered)

                                    ShareLink(item: business.qrPayload) {
                                        Label("Share", systemImage: "square.and.arrow.up")
                                    }
                                    .buttonStyle(.bordered)

                                    Button {
                                        qrBusiness = business
                                    } label: {
                                        Label("QR", systemImage: "qrcode")
                                    }
                                    .buttonStyle(.bordered)

                                    if business.ownerPubkey.lowercased() == dataStore.currentUserPubkey() {
                                        Button("Edit") {
                                            editingBusiness = business
                                        }
                                        .buttonStyle(.bordered)
                                    }
                                }
                            }
                        }
                    }
                }

                Section("Promises") {
                    if dataStore.promises.isEmpty {
                        Text("No promises yet.")
                    } else {
                        ForEach(dataStore.promises) { promise in
                            let business = dataStore.businesses.first(where: { $0.businessID == promise.merchantID })
                            let isMerchantOwner = (business?.ownerPubkey.lowercased() == dataStore.currentUserPubkey())
                            let isPayer = (promise.payerPubkey.lowercased() == dataStore.currentUserPubkey())

                            VStack(alignment: .leading, spacing: 6) {
                                Text("\(promise.amountMsat / 1000) sats")
                                    .font(.headline)
                                if let business {
                                    Text("To: \(business.name) • \(business.category)")
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                } else {
                                    Text("To: \(promise.merchantID)")
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                                Text("\(promise.status.rawValue.capitalized) • Expires \(Date(timeIntervalSince1970: TimeInterval(promise.expiresAt)).formatted())")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                HStack {
                                    if isPayer {
                                        Button("Show QR") {
                                            promiseQR = promise
                                        }
                                        .buttonStyle(.bordered)
                                    }

                                    if isMerchantOwner, promise.status == .pending || promise.status == .accepted || promise.status == .rejected {
                                        Button("Respond") {
                                            promiseRespond = promise
                                        }
                                        .buttonStyle(.bordered)
                                    }

                                    if promise.status == .accepted {
                                        Button("Record Settlement") {
                                            settlementPromise = promise
                                        }
                                        .buttonStyle(.borderedProminent)
                                    }
                                }
                            }
                        }
                    }
                }
            }
            .navigationTitle("Business")
            .toolbar {
                ToolbarItem(placement: .primaryAction) {
                    Button {
                        showRegisterBusiness = true
                    } label: {
                        Label("Register", systemImage: "plus")
                    }
                }
                #if os(iOS)
                ToolbarItem(placement: .topBarTrailing) {
                    Button {
                        showQRScanner = true
                    } label: {
                        Image(systemName: "qrcode.viewfinder")
                    }
                }
                #endif
            }
            .sheet(isPresented: $showRegisterBusiness) {
                BusinessEditorSheet(existingBusiness: nil)
                    .environmentObject(dataStore)
            }
            .sheet(item: $qrBusiness) { business in
                SemayBusinessQRSheet(business: business)
            }
            .sheet(item: $editingBusiness) { business in
                BusinessEditorSheet(existingBusiness: business)
                    .environmentObject(dataStore)
            }
            .sheet(item: $settlementPromise) { promise in
                SemaySettlementSheet(promise: promise)
                    .environmentObject(dataStore)
            }
            .sheet(item: $promiseQR) { promise in
                SemayPromiseQRSheet(promise: promise)
                    .environmentObject(dataStore)
            }
            .sheet(item: $promiseRespond) { promise in
                SemayPromiseRespondSheet(promise: promise)
                    .environmentObject(dataStore)
            }
            #if os(iOS)
            .sheet(isPresented: $showQRScanner) {
                SemayQRScanSheet(isPresented: $showQRScanner)
            }
            #endif
        }
    }

    private func telURL(for rawPhone: String) -> URL? {
        let cleaned = rawPhone
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .filter { "+0123456789".contains($0) }
        guard !cleaned.isEmpty else { return nil }
        return URL(string: "tel:\(cleaned)")
    }

    private func openDirections(latitude: Double, longitude: Double, name: String) {
        guard latitude != 0 || longitude != 0 else { return }
        let q = name.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? ""
        if let url = URL(string: "http://maps.apple.com/?ll=\(latitude),\(longitude)&q=\(q)") {
            openURL(url)
        }
    }

}

private struct SemayBusinessQRSheet: View {
    let business: BusinessProfile
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            VStack(spacing: 14) {
                Text(business.name)
                    .font(.headline)

                QRCodeImage(data: business.qrPayload, size: 240)

                VStack(alignment: .leading, spacing: 6) {
                    Text("\(business.category) • \(business.eAddress)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    if !business.plusCode.isEmpty {
                        Text(business.plusCode)
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                    if !business.lightningLink.isEmpty {
                        Text("Lightning: \(business.lightningLink)")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                    if !business.cashuLink.isEmpty {
                        Text("Cashu: \(business.cashuLink)")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                    Text(business.qrPayload)
                        .font(.system(.caption2, design: .monospaced))
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .padding(12)
                .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 12))
                .padding(.horizontal, 16)

                Spacer()
            }
            .padding(.top, 16)
            .navigationTitle("Share Business")
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}

private struct BusinessEditorSheet: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.dismiss) private var dismiss
    let existingBusiness: BusinessProfile?
    @StateObject private var locationState = LocationStateManager.shared

    private struct PendingBusinessPhoto: Identifiable {
        let id: String
        let imageData: Data
        let exifLatitude: Double?
        let exifLongitude: Double?
        let geoSource: String
    }

    private struct PendingPhotoDraft {
        let imageData: Data
        let coordinate: CLLocationCoordinate2D
    }

    @State private var name = ""
    @State private var category = "shop"
    @State private var details = ""
    @State private var phone = ""
    @State private var lightningLink = ""
    @State private var cashuLink = ""
    @State private var latitude = "15.3229"
    @State private var longitude = "38.9251"
    @State private var error: String?
    @State private var showCoordinateEditor = false
    @State private var existingPhotoRefs: [SemayServicePhotoRef] = []
    @State private var pendingPhotos: [PendingBusinessPhoto] = []
    @State private var selectedPrimaryPhotoID = ""
    @State private var pendingPhotoDraft: PendingPhotoDraft?
    @State private var showPhotoLocationPrompt = false
    @State private var resolvedServiceID: String?
    #if os(iOS)
    @State private var showImagePicker = false
    @State private var imagePickerSourceType: UIImagePickerController.SourceType = .photoLibrary
    #endif

    private var parsedCoordinate: CLLocationCoordinate2D? {
        guard let lat = Double(latitude), let lon = Double(longitude) else { return nil }
        return CLLocationCoordinate2D(latitude: lat, longitude: lon)
    }

    private var computedAddress: (plus: String, e: String) {
        guard let coord = parsedCoordinate else { return ("", "") }
        let a = SemayAddress.eAddress(latitude: coord.latitude, longitude: coord.longitude)
        return (a.plusCode, a.eAddress)
    }

    private var totalPhotoCount: Int {
        existingPhotoRefs.count + pendingPhotos.count
    }

    private var canAddPhoto: Bool {
        totalPhotoCount < 3
    }

    var body: some View {
        NavigationStack {
            Form {
                TextField("Business Name", text: $name)
                TextField("Category", text: $category)
                TextField("Description", text: $details)
                TextField("Phone (Optional)", text: $phone)
                    .semayPhoneKeyboard()

                Section("Payments (Optional)") {
                    TextField("Lightning (e.g., lightning:...)", text: $lightningLink)
                        .semayDisableAutoCaps()
                        .semayDisableAutocorrection()
                    TextField("Cashu (optional)", text: $cashuLink)
                        .semayDisableAutoCaps()
                        .semayDisableAutocorrection()
                }

                Section("Photos") {
                    if existingPhotoRefs.isEmpty && pendingPhotos.isEmpty {
                        Text("No photos yet.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(existingPhotoRefs) { ref in
                            HStack(spacing: 10) {
                                businessPhotoThumbnail(serviceID: ref.serviceID, photoID: ref.photoID, pendingData: nil)
                                VStack(alignment: .leading, spacing: 2) {
                                    Text(ref.remoteURL == nil ? "Local photo" : "Photo metadata only")
                                        .font(.caption)
                                    Text("Updated \(Date(timeIntervalSince1970: TimeInterval(ref.updatedAt)).formatted(date: .abbreviated, time: .omitted))")
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                                Button(selectedPrimaryPhotoID == ref.photoID ? "Primary" : "Set Primary") {
                                    selectedPrimaryPhotoID = ref.photoID
                                }
                                .buttonStyle(.bordered)
                                Button("Remove", role: .destructive) {
                                    dataStore.removeServicePhotoRef(
                                        serviceID: ref.serviceID,
                                        photoID: ref.photoID,
                                        emitServiceUpdate: true
                                    )
                                    existingPhotoRefs = dataStore.servicePhotoRefs(serviceID: ref.serviceID)
                                    if selectedPrimaryPhotoID == ref.photoID {
                                        selectedPrimaryPhotoID = existingPhotoRefs.first(where: { $0.primary })?.photoID
                                            ?? existingPhotoRefs.first?.photoID
                                            ?? pendingPhotos.first?.id
                                            ?? ""
                                    }
                                }
                                .buttonStyle(.bordered)
                            }
                        }

                        ForEach(pendingPhotos) { photo in
                            HStack(spacing: 10) {
                                businessPhotoThumbnail(serviceID: nil, photoID: nil, pendingData: photo.imageData)
                                VStack(alignment: .leading, spacing: 2) {
                                    Text("Pending photo")
                                        .font(.caption)
                                    Text(photo.geoSource == "exif_confirmed" ? "Using photo location" : "Using current/manual location")
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                                Button(selectedPrimaryPhotoID == photo.id ? "Primary" : "Set Primary") {
                                    selectedPrimaryPhotoID = photo.id
                                }
                                .buttonStyle(.bordered)
                                Button("Remove", role: .destructive) {
                                    pendingPhotos.removeAll { $0.id == photo.id }
                                    if selectedPrimaryPhotoID == photo.id {
                                        selectedPrimaryPhotoID = existingPhotoRefs.first(where: { $0.primary })?.photoID
                                            ?? existingPhotoRefs.first?.photoID
                                            ?? pendingPhotos.first?.id
                                            ?? ""
                                    }
                                }
                                .buttonStyle(.bordered)
                            }
                        }
                    }

                    #if os(iOS)
                    HStack {
                        Button("Choose Photo") {
                            imagePickerSourceType = .photoLibrary
                            showImagePicker = true
                        }
                        .disabled(!canAddPhoto)
                        .buttonStyle(.borderedProminent)

                        Button("Take Photo") {
                            imagePickerSourceType = .camera
                            showImagePicker = true
                        }
                        .disabled(!canAddPhoto || !UIImagePickerController.isSourceTypeAvailable(.camera))
                        .buttonStyle(.bordered)
                    }
                    #else
                    Text("Photo evidence is currently available on iOS.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    #endif
                    Text("Up to 3 photos per listing. Photos stay local unless you choose to share metadata.")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }

                Section("Location") {
                    if let coord = parsedCoordinate {
                        Text(String(format: "Lat %.6f, Lon %.6f", coord.latitude, coord.longitude))
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        Text("Location not set")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Button("Use Current Location") {
                        if locationState.permissionState != .authorized {
                            locationState.enableLocationChannels()
                            error = "Enable location access to use your current position."
                            return
                        }
                        guard let loc = locationState.lastKnownLocation else {
                            locationState.refreshChannels()
                            error = "Getting your location… please try again in a moment."
                            return
                        }
                        latitude = String(format: "%.6f", loc.coordinate.latitude)
                        longitude = String(format: "%.6f", loc.coordinate.longitude)
                        error = nil
                    }

                    Toggle("Edit Coordinates", isOn: $showCoordinateEditor)

                    if showCoordinateEditor {
                        TextField("Latitude", text: $latitude)
                        TextField("Longitude", text: $longitude)
                    }
                }

                if !computedAddress.plus.isEmpty {
                    Section("Address") {
                        Text("Plus Code: \(computedAddress.plus)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text("E-Address: \(computedAddress.e)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }

                if let error {
                    Text(error)
                        .foregroundStyle(.red)
                }
            }
            .navigationTitle(existingBusiness == nil ? "Register Business" : "Update Business")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") {
                        guard let coord = parsedCoordinate else {
                            error = "Invalid latitude/longitude."
                            return
                        }

                        if let existingBusiness {
                            let updated = dataStore.updateBusiness(
                                businessID: existingBusiness.businessID,
                                name: name,
                                category: category,
                                details: details,
                                latitude: coord.latitude,
                                longitude: coord.longitude,
                                phone: phone,
                                lightningLink: lightningLink,
                                cashuLink: cashuLink
                            )
                            if updated == nil {
                                error = "Only the business owner can update this profile right now."
                                return
                            }
                            persistPhotosAfterSave(businessID: existingBusiness.businessID)
                            dismiss()
                            return
                        }

                        let created = dataStore.registerBusiness(
                            name: name,
                            category: category,
                            details: details,
                            latitude: coord.latitude,
                            longitude: coord.longitude,
                            phone: phone,
                            lightningLink: lightningLink,
                            cashuLink: cashuLink
                        )
                        persistPhotosAfterSave(businessID: created.businessID)
                        dismiss()
                    }
                    .disabled(name.isEmpty || category.isEmpty || details.isEmpty || parsedCoordinate == nil)
                }
            }
            .onAppear {
                if let existingBusiness {
                    name = existingBusiness.name
                    category = existingBusiness.category
                    details = existingBusiness.details
                    phone = existingBusiness.phone
                    lightningLink = existingBusiness.lightningLink
                    cashuLink = existingBusiness.cashuLink
                    latitude = String(format: "%.6f", existingBusiness.latitude)
                    longitude = String(format: "%.6f", existingBusiness.longitude)
                    if let serviceID = dataStore.linkedServiceID(entityType: "business", entityID: existingBusiness.businessID) {
                        resolvedServiceID = serviceID
                        existingPhotoRefs = dataStore.servicePhotoRefs(serviceID: serviceID)
                        selectedPrimaryPhotoID = existingPhotoRefs.first(where: { $0.primary })?.photoID
                            ?? existingPhotoRefs.first?.photoID
                            ?? ""
                    }
                } else if locationState.permissionState == .authorized,
                          let loc = locationState.lastKnownLocation {
                    latitude = String(format: "%.6f", loc.coordinate.latitude)
                    longitude = String(format: "%.6f", loc.coordinate.longitude)
                }
            }
            .confirmationDialog("Use photo location?", isPresented: $showPhotoLocationPrompt, titleVisibility: .visible) {
                Button("Use Photo Location") {
                    guard let draft = pendingPhotoDraft else { return }
                    latitude = String(format: "%.6f", draft.coordinate.latitude)
                    longitude = String(format: "%.6f", draft.coordinate.longitude)
                    commitPendingPhotoDraft(geoSource: "exif_confirmed", includeCoordinate: true)
                }
                Button("Keep Current Pin") {
                    commitPendingPhotoDraft(geoSource: "none", includeCoordinate: false)
                }
                Button("Set Manually On Map") {
                    showCoordinateEditor = true
                    commitPendingPhotoDraft(geoSource: "manual", includeCoordinate: false)
                }
                Button("Cancel", role: .cancel) {
                    pendingPhotoDraft = nil
                }
            } message: {
                if let draft = pendingPhotoDraft {
                    Text(String(format: "Photo suggests %.5f, %.5f", draft.coordinate.latitude, draft.coordinate.longitude))
                }
            }
            #if os(iOS)
            .fullScreenCover(isPresented: $showImagePicker) {
                ImagePickerView(sourceType: imagePickerSourceType) { image in
                    showImagePicker = false
                    guard let image else { return }
                    guard let data = image.jpegData(compressionQuality: 0.96) else {
                        error = "Unable to read selected photo."
                        return
                    }
                    handleIncomingPhotoData(data)
                }
                .ignoresSafeArea()
            }
            #endif
        }
    }

    private func handleIncomingPhotoData(_ data: Data) {
        guard canAddPhoto else {
            error = "You can attach up to 3 photos per listing."
            return
        }
        if let gps = SemayPhotoMetadataExtractor.extractGPS(from: data) {
            pendingPhotoDraft = PendingPhotoDraft(imageData: data, coordinate: gps.coordinate)
            showPhotoLocationPrompt = true
            return
        }
        appendPendingPhoto(data: data, exifLatitude: nil, exifLongitude: nil, geoSource: "none")
    }

    private func commitPendingPhotoDraft(geoSource: String, includeCoordinate: Bool) {
        guard let draft = pendingPhotoDraft else { return }
        defer { pendingPhotoDraft = nil }
        appendPendingPhoto(
            data: draft.imageData,
            exifLatitude: includeCoordinate ? draft.coordinate.latitude : nil,
            exifLongitude: includeCoordinate ? draft.coordinate.longitude : nil,
            geoSource: geoSource
        )
    }

    private func appendPendingPhoto(data: Data, exifLatitude: Double?, exifLongitude: Double?, geoSource: String) {
        guard canAddPhoto else {
            error = "You can attach up to 3 photos per listing."
            return
        }
        let photoID = UUID().uuidString.lowercased()
        pendingPhotos.append(
            PendingBusinessPhoto(
                id: photoID,
                imageData: data,
                exifLatitude: exifLatitude,
                exifLongitude: exifLongitude,
                geoSource: geoSource
            )
        )
        if selectedPrimaryPhotoID.isEmpty {
            selectedPrimaryPhotoID = photoID
        }
    }

    private func persistPhotosAfterSave(businessID: String) {
        guard let serviceID = dataStore.linkedServiceID(entityType: "business", entityID: businessID) else { return }
        resolvedServiceID = serviceID

        for pending in pendingPhotos {
            _ = dataStore.addServicePhotoFromImageData(
                serviceID: serviceID,
                imageData: pending.imageData,
                exifLatitude: pending.exifLatitude,
                exifLongitude: pending.exifLongitude,
                geoSource: pending.geoSource,
                isPrimary: false,
                preferredPhotoID: pending.id
            )
        }

        var refs = dataStore.servicePhotoRefs(serviceID: serviceID)
        if !selectedPrimaryPhotoID.isEmpty, refs.contains(where: { $0.photoID == selectedPrimaryPhotoID }) {
            let now = Int(Date().timeIntervalSince1970)
            refs = refs.map { ref in
                SemayServicePhotoRef(
                    photoID: ref.photoID,
                    serviceID: ref.serviceID,
                    sha256: ref.sha256,
                    mimeType: ref.mimeType,
                    width: ref.width,
                    height: ref.height,
                    bytesFull: ref.bytesFull,
                    bytesThumb: ref.bytesThumb,
                    takenAt: ref.takenAt,
                    exifLatitude: ref.exifLatitude,
                    exifLongitude: ref.exifLongitude,
                    geoSource: ref.geoSource,
                    primary: ref.photoID == selectedPrimaryPhotoID,
                    remoteURL: ref.remoteURL,
                    createdAt: ref.createdAt,
                    updatedAt: now
                )
            }
            dataStore.upsertServicePhotoRefs(serviceID: serviceID, refs: refs, emitServiceUpdate: true)
        }
    }

    @ViewBuilder
    private func businessPhotoThumbnail(serviceID: String?, photoID: String?, pendingData: Data?) -> some View {
        #if os(iOS)
        if let pendingData, let image = UIImage(data: pendingData) {
            Image(uiImage: image)
                .resizable()
                .scaledToFill()
                .frame(width: 56, height: 56)
                .clipShape(RoundedRectangle(cornerRadius: 8))
        } else if let serviceID, let photoID,
                  let thumbURL = dataStore.servicePhotoThumbURL(serviceID: serviceID, photoID: photoID),
                  let image = UIImage(contentsOfFile: thumbURL.path) {
            Image(uiImage: image)
                .resizable()
                .scaledToFill()
                .frame(width: 56, height: 56)
                .clipShape(RoundedRectangle(cornerRadius: 8))
        } else {
            ZStack {
                RoundedRectangle(cornerRadius: 8)
                    .fill(.ultraThinMaterial)
                Image(systemName: "photo")
                    .foregroundStyle(.secondary)
            }
            .frame(width: 56, height: 56)
        }
        #else
        if let pendingData, let image = NSImage(data: pendingData) {
            Image(nsImage: image)
                .resizable()
                .scaledToFill()
                .frame(width: 56, height: 56)
                .clipShape(RoundedRectangle(cornerRadius: 8))
        } else if let serviceID, let photoID,
                  let thumbURL = dataStore.servicePhotoThumbURL(serviceID: serviceID, photoID: photoID),
                  let image = NSImage(contentsOf: thumbURL) {
            Image(nsImage: image)
                .resizable()
                .scaledToFill()
                .frame(width: 56, height: 56)
                .clipShape(RoundedRectangle(cornerRadius: 8))
        } else {
            ZStack {
                RoundedRectangle(cornerRadius: 8)
                    .fill(.ultraThinMaterial)
                Image(systemName: "photo")
                    .foregroundStyle(.secondary)
            }
            .frame(width: 56, height: 56)
        }
        #endif
    }
}

		private struct SemayMeTabView: View {
		    @EnvironmentObject private var dataStore: SemayDataStore
		    @EnvironmentObject private var seedService: SeedPhraseService
		    @Environment(\.openURL) private var openURL
		    @AppStorage("semay.settings.advanced") private var advancedSettingsEnabled = false
            @AppStorage("semay.icloud_backup_enabled") private var iCloudBackupEnabled = false
            @AppStorage("semay.translation.offline_enabled") private var offlineTranslationEnabled = true
            @AppStorage("semay.offline_maps.require_signed_packs") private var requireSignedOfflinePacks = false
            @AppStorage("semay.map.country_packs.enabled") private var countryPacksEnabled = false
		    @StateObject private var safety = SafetyModeService.shared
		    @StateObject private var envelopeSync = SemayEnvelopeSyncService.shared
		    @StateObject private var tileStore = OfflineTileStore.shared
		    @StateObject private var reachability = NetworkReachabilityService.shared
            @StateObject private var mapEngine = MapEngineCoordinator.shared
	
	    @State private var revealPhrase = false
	    @State private var showRestoreSeed = false
	    @State private var hubBaseURL = ""
	    @State private var hubToken = ""
    @State private var loadingHubMetrics = false
    @State private var hubMetricsSummary = ""
    @State private var hubMetricsError = ""
	    @State private var discoveringHub = false
	    @State private var hubDiscoveryNotice = ""
	    @State private var showPackInfo = false
	    @State private var installingOfflineMaps = false
	    @State private var offlineMapsNotice = ""
	    @State private var offlineMapsError = ""
	            @State private var aboutTapCount = 0
	            @State private var aboutNotice = ""
	            @State private var cloudBackupBusy = false
	            @State private var cloudBackupNotice = ""
            @State private var cloudBackupError = ""

            private var iCloudBackupAvailable: Bool {
                seedService.isICloudBackupAvailable()
            }
                @State private var legalSupportNotice = ""
	
	    var body: some View {
	        NavigationStack {
	            Form {
                Section("Security") {
                    Toggle("Safe Mode", isOn: Binding(
                        get: { safety.safeModeEnabled },
                        set: { safety.setSafeModeEnabled($0) }
                    ))

                    Toggle("Read Receipts", isOn: Binding(
                        get: { safety.readReceiptsEnabled },
                        set: { safety.setReadReceiptsEnabled($0) }
                    ))

                    Toggle("Presence Heartbeats", isOn: Binding(
                        get: { safety.presenceEnabled },
                        set: { safety.setPresenceEnabled($0) }
                    ))
                }

                Section("Offline Translation") {
                    Toggle("Offline Dictionary", isOn: $offlineTranslationEnabled)
                    Text("Disable only if you want to turn off installed offline language support.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text("Translation currently applies to chat/listing text. Base-map labels come from your installed map pack language.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

	                Section("About") {
	                    Button {
	                        handleAboutTap()
	                    } label: {
                        HStack {
                            Text("Version")
                            Spacer()
                            Text(appVersionString)
                                .foregroundStyle(.secondary)
                        }
                    }
                    .buttonStyle(.plain)

                    if !aboutNotice.isEmpty {
                        Text(aboutNotice)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }

                    if advancedSettingsEnabled {
                        Toggle("Advanced Settings", isOn: $advancedSettingsEnabled)
                        Text("Advanced settings are for operators running nodes and debugging sync.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }

                Section("Backup") {
                    if revealPhrase {
                        Text(seedService.getOrCreatePhrase())
                            .font(.system(.body, design: .monospaced))
                            .textSelection(.enabled)
                    } else {
                        Text("Seed phrase hidden")
                    }

                    Button(revealPhrase ? "Hide Phrase" : "Reveal Phrase") {
                        revealPhrase.toggle()
                    }

                    Button(role: .destructive) {
                        showRestoreSeed = true
                    } label: {
                        Text("Restore From Seed (Replace Identity)")
                    }
                }

                Section("iCloud Backup (Optional)") {
                    Toggle("Enable iCloud Backup", isOn: $iCloudBackupEnabled)
                        .disabled(!iCloudBackupAvailable)
                    Text("Backs up private Semay settings and identity metadata encrypted with your seed-derived key. No plaintext seed is uploaded.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    if !iCloudBackupAvailable {
                        Text("iCloud backup is unavailable for this build. Manual seed backup remains available.")
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }

                    if iCloudBackupEnabled && iCloudBackupAvailable {
                        Button(cloudBackupBusy ? "Syncing..." : "Backup Now") {
                            Task {
                                await uploadCloudBackup()
                            }
                        }
                        .disabled(cloudBackupBusy)

                        Button(cloudBackupBusy ? "Restoring..." : "Restore Backup") {
                            Task {
                                await restoreCloudBackup()
                            }
                        }
                        .disabled(cloudBackupBusy)
                    }

                    if !cloudBackupNotice.isEmpty {
                        Text(cloudBackupNotice)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    if !cloudBackupError.isEmpty {
                        Text(cloudBackupError)
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                }

                if advancedSettingsEnabled {
                    Section("Reliability (Advanced)") {
                        Text("Pending outbox: \(dataStore.pendingOutboxCount())")
                        Text("Failed outbox: \(dataStore.failedOutboxCount())")
                        if let lastSyncAt = envelopeSync.lastSyncAt {
                            Text("Last sync: \(lastSyncAt.formatted())")
                        }
                        if let lastSummary = envelopeSync.lastSummary, !lastSummary.isEmpty {
                            Text(lastSummary)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        if let lastError = envelopeSync.lastError, !lastError.isEmpty {
                            Text(lastError)
                                .font(.caption)
                                .foregroundStyle(.orange)
                        }
                        Button(envelopeSync.isSyncing ? "Syncing..." : "Sync Now") {
                            Task {
                                await envelopeSync.syncNow()
                            }
                        }
                        .disabled(envelopeSync.isSyncing)
                    }
                }

                if advancedSettingsEnabled {
                    Section("Map Engine (Advanced)") {
                        Menu("Preferred Engine: \(mapEngine.selectedEngine.label)") {
                            ForEach(MapEngine.allCases) { engine in
                                Button {
                                    mapEngine.setPreferredEngine(engine)
                                } label: {
                                    if mapEngine.selectedEngine == engine {
                                        Label(engine.label, systemImage: "checkmark")
                                    } else {
                                        Text(engine.label)
                                    }
                                }
                                .disabled(engine == .maplibre && !mapEngine.mapLibreAllowed)
                            }
                        }

                        let effectiveLabel = mapEngine.effectiveEngine.label
                        Text("Effective engine: \(effectiveLabel)")
                            .font(.caption)
                            .foregroundStyle(.secondary)

                        if mapEngine.selectedEngine == .maplibre && mapEngine.mapLibreRemoteDisabled {
                            Text("Remote policy disabled Semay Map for this node; app is using MapKit.")
                                .font(.caption)
                                .foregroundStyle(.orange)
                        } else if mapEngine.sessionFallbackToMapKit {
                            Text("Semay Map failed this session and fell back to MapKit.")
                                .font(.caption)
                                .foregroundStyle(.orange)
                        }

                        let stability = mapEngine.mapLibreStabilitySnapshot
                        Text(
                            "Fallback-free sessions (\(stability.windowDays)d): \(stability.successfulSessions)/\(stability.mapLibreSessions) (\(formatPercent(stability.fallbackFreeRate)))"
                        )
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        Text("Observed days: \(stability.observedDays)/\(stability.windowDays)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text(
                            stability.meetsGate
                                ? "Stability gate met (\(formatPercent(stability.targetRate)) target)."
                                : "Stability gate not met (\(formatPercent(stability.targetRate)) target)."
                        )
                        .font(.caption)
                        .foregroundStyle(stability.meetsGate ? .green : .orange)

                        Button("Reset Map Engine Metrics") {
                            mapEngine.clearMapLibreStabilityMetrics()
                        }
                    }
                }

                Section("Offline Maps") {
                    if advancedSettingsEnabled {
                        Toggle("Enable Country Pack Catalog", isOn: $countryPacksEnabled)
                        Text("Feature flag for Eritrea/Ethiopia country-pack cards and direct install flow.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    if let pack = tileStore.availablePack {
                        Text("Installed: \(pack.name)")
                        Text("Size: \(formatSize(pack.sizeBytes))")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        if tileStore.isBundledStarterSelected {
                            Text("Starter pack is limited. Connect to a network to download full offline maps.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Button(installingOfflineMaps ? "Upgrading..." : "Upgrade Offline Maps") {
                                Task {
                                    await installOfflineMaps()
                                }
                            }
                            .disabled(!reachability.isOnline || installingOfflineMaps)
                        }
                        if advancedSettingsEnabled {
                            Text("Zoom: \(pack.minZoom)–\(pack.maxZoom)")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            if let bounds = pack.bounds {
	                                Text("Bounds: \(format(bounds.minLat)),\(format(bounds.minLon)) → \(format(bounds.maxLat)),\(format(bounds.maxLon))")
	                                    .font(.caption)
	                                    .foregroundStyle(.secondary)
	                            }
	                        }
	                        if advancedSettingsEnabled {
	                            Button("Manage Offline Maps") {
	                                showPackInfo = true
	                            }
	                        } else {
	                            Button(role: .destructive) {
	                                tileStore.deleteAllPacks()
	                            } label: {
	                                Text("Remove Offline Maps")
	                            }
	                        }
                    } else {
                        Text("Not installed")
                            .foregroundStyle(.secondary)
                        Text(reachability.isOnline
                             ? "Install once so Semay stays useful when the internet is down."
                             : "Install the starter map now, and upgrade later when online.")
                            .font(.caption)
                            .foregroundStyle(.secondary)

                        Button(installingOfflineMaps ? "Installing..." : "Install Offline Maps") {
                            Task {
                                await installOfflineMaps()
                            }
                        }
                        .disabled((!reachability.isOnline && !tileStore.canInstallBundledStarterPack) || installingOfflineMaps)
	
	                        if !offlineMapsNotice.isEmpty {
	                            Text(offlineMapsNotice)
	                                .font(.caption)
	                                .foregroundStyle(.secondary)
	                        }
	                        if !offlineMapsError.isEmpty {
	                            Text(offlineMapsError)
	                                .font(.caption)
	                                .foregroundStyle(.orange)
	                        }
	
	                        if advancedSettingsEnabled {
	                            Button("Manage Offline Maps") {
	                                showPackInfo = true
	                            }
	                        }
	                    }
	                }

                    Section("Legal & Support") {
                        Button("Privacy Policy") {
                            openConfiguredURL(infoKey: "SemayPrivacyPolicyURL", label: "Privacy Policy")
                        }
                        Button("Terms of Use") {
                            openConfiguredURL(infoKey: "SemayTermsOfUseURL", label: "Terms of Use")
                        }
                        Button("Community Moderation Policy") {
                            openConfiguredURL(infoKey: "SemayModerationPolicyURL", label: "Community Moderation Policy")
                        }
                        Button("Contact Support") {
                            openSupport()
                        }
                        Button("Report an Issue") {
                            openConfiguredURL(infoKey: "SemaySupportURL", label: "Issue tracker")
                        }

                        if !legalSupportNotice.isEmpty {
                            Text(legalSupportNotice)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }

		                if advancedSettingsEnabled {
		                    Section("Node (Advanced)") {
		                        Text("Leave this blank to auto-detect a nearby node. Set it only if you're operating your own Semay node.")
		                            .font(.caption)
		                            .foregroundStyle(.secondary)
                        Toggle("Require Signed Offline Packs", isOn: $requireSignedOfflinePacks)
                        Text("When enabled, imports and node installs must include valid hash + signature metadata.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
		                        if let nodeName = tileStore.activeNodeName, !nodeName.isEmpty {
		                            Text("Connected node: \(nodeName)")
		                                .font(.caption)
	                                .foregroundStyle(.secondary)
	                        }
	                        if let activeURL = tileStore.activeMapSourceBaseURL, !activeURL.isEmpty {
	                            Text("Node URL: \(activeURL)")
	                                .font(.caption2)
	                                .foregroundStyle(.secondary)
	                        }
	                        TextField("Node Base URL (Optional)", text: $hubBaseURL)
	                            .semayDisableAutoCaps()
	                            .semayDisableAutocorrection()
	                        SecureField("Node Token (Optional)", text: $hubToken)
	                            .semayDisableAutoCaps()
	                            .semayDisableAutocorrection()
                        Button(discoveringHub ? "Detecting Node..." : "Auto-Detect Node") {
                            Task {
                                discoveringHub = true
                                hubDiscoveryNotice = ""
                                hubMetricsError = ""
                                defer { discoveringHub = false }
                                do {
                                    let discovered = try await SemayNodeDiscoveryService.shared.discoverBaseURLString()
                                    hubBaseURL = discovered
                                    dataStore.saveHubConfig(baseURL: discovered, token: hubToken)
                                    tileStore.reloadSourceConfig()
                                    hubDiscoveryNotice = "Connected to \(discovered)"
                                } catch {
                                    hubMetricsError = error.localizedDescription
                                }
                            }
                        }
	                        .disabled(discoveringHub)
	                        Button("Reset To Auto") {
	                            hubBaseURL = ""
	                            hubToken = ""
	                            dataStore.saveHubConfig(baseURL: "", token: "")
	                            tileStore.reloadSourceConfig()
	                        }
	                        Button("Save Node Settings") {
	                            dataStore.saveHubConfig(baseURL: hubBaseURL, token: hubToken)
	                            tileStore.reloadSourceConfig()
	                        }
                        Button("Refresh") {
                            dataStore.refreshAll()
                        }
                        Button(loadingHubMetrics ? "Loading Node Metrics..." : "Load Node Metrics") {
                            Task {
                                loadingHubMetrics = true
                                hubMetricsError = ""
                                defer { loadingHubMetrics = false }
                                do {
                                    let metrics = try await dataStore.fetchHubMetrics()
                                    let latency = metrics.spool.deliveryLatencyMs
                                    hubMetricsSummary = "Rejected (\(metrics.windowSeconds)s): \(metrics.ingest.rejectedTotal) | Category P:\(metrics.ingest.rejectedByCategory.protocolInvalid) Policy:\(metrics.ingest.rejectedByCategory.policyRejected) | Spool Pending:\(metrics.spool.pendingTotal) Retry:\(metrics.spool.retryTotal) Failed:\(metrics.spool.failedTotal) Delivered:\(metrics.spool.deliveredTotal) | Latency avg:\(latency.avg)ms min:\(latency.min)ms max:\(latency.max)ms"
                                } catch {
                                    hubMetricsError = error.localizedDescription
                                }
                            }
                        }
                        .disabled(loadingHubMetrics)
                        if !hubMetricsSummary.isEmpty {
                            Text(hubMetricsSummary)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        if !hubDiscoveryNotice.isEmpty {
                            Text(hubDiscoveryNotice)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        if !hubMetricsError.isEmpty {
                            Text(hubMetricsError)
                                .font(.caption)
                                .foregroundStyle(.orange)
                        }
                    }
                }
            }
            .navigationTitle("Me")
            .onAppear {
                hubBaseURL = dataStore.hubBaseURLString()
                hubToken = dataStore.hubIngestToken()
                tileStore.refresh()
                mapEngine.refreshRuntimeState()
                SemayTranslationService.shared.setTranslationEnabled(offlineTranslationEnabled)
                SemayTranslationService.shared.setQualityMode(.strict)
                if !iCloudBackupAvailable {
                    iCloudBackupEnabled = false
                }
            }
            .onChange(of: offlineTranslationEnabled) { enabled in
                SemayTranslationService.shared.setTranslationEnabled(enabled)
            }
            .onChange(of: iCloudBackupEnabled) { enabled in
                if enabled && !iCloudBackupAvailable {
                    iCloudBackupEnabled = false
                    cloudBackupError = "iCloud backup is unavailable for this build."
                }
            }
            .sheet(isPresented: $showRestoreSeed) {
                SemayRestoreSeedSheet(isPresented: $showRestoreSeed)
                    .environmentObject(seedService)
                    .environmentObject(dataStore)
            }
            #if os(iOS)
            .sheet(isPresented: $showPackInfo) {
                TilePackInfoSheet(
                    isPresented: $showPackInfo,
                    tileStore: tileStore,
                    useOfflineTiles: Binding.constant(true),
                    useOSMBaseMap: Binding.constant(false)
                )
            }
            #endif
        }
    }

    private func format(_ value: Double) -> String {
        String(format: "%.4f", value)
    }

    private func formatPercent(_ value: Double) -> String {
        String(format: "%.2f%%", value)
    }

    private func formatSize(_ bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB]
        formatter.countStyle = .file
        return formatter.string(fromByteCount: bytes)
    }

    private func installOfflineMaps() async {
        installingOfflineMaps = true
        offlineMapsNotice = ""
        offlineMapsError = ""
        defer { installingOfflineMaps = false }

        do {
            if reachability.isOnline {
                do {
                    let installed: OfflineTilePack
                    let countryPacks = countryPacksEnabled ? (try await tileStore.featuredCountryPacks()) : []
                    if let preferred = countryPacks.first(where: { $0.isFeatured ?? false }) ?? countryPacks.first {
                        let packID = (preferred.packID ?? preferred.id).trimmingCharacters(in: .whitespacesAndNewlines)
                        guard !packID.isEmpty else {
                            throw NSError(
                                domain: "OfflineTileStore",
                                code: 61,
                                userInfo: [NSLocalizedDescriptionKey: "Pack identifier is required."]
                            )
                        }
                        installed = try await tileStore.installCountryPack(packID: packID)
                    } else {
                        installed = try await tileStore.installRecommendedPack()
                    }
                    offlineMapsNotice = "Downloaded offline maps: \(installed.name)."
                    return
                } catch {
                    let reason = userFacingOfflineMapError(error)
                    if isSignedPackPolicyError(error) {
                        offlineMapsError = "Couldn't download offline maps (\(reason))."
                        return
                    }
                    if tileStore.availablePack == nil, tileStore.canInstallBundledStarterPack {
                        let installed = try tileStore.installBundledStarterPack()
                        offlineMapsNotice = "Couldn't download full offline maps (\(reason)). Installed starter offline maps: \(installed.name)."
                        return
                    }
                    offlineMapsError = "Couldn't download full offline maps (\(reason))."
                    return
                }
            }

            let installed = try tileStore.installBundledStarterPack()
            offlineMapsNotice = "Installed \(installed.name)."
        } catch {
            offlineMapsError = userFacingOfflineMapError(error)
        }
    }

    private func uploadCloudBackup() async {
        cloudBackupBusy = true
        cloudBackupNotice = ""
        cloudBackupError = ""
        defer { cloudBackupBusy = false }
        guard seedService.isICloudBackupAvailable() else {
            iCloudBackupEnabled = false
            cloudBackupError = "iCloud backup is unavailable for this build."
            return
        }
        #if canImport(CloudKit)
        do {
            try await seedService.uploadEncryptedBackupToICloud()
            cloudBackupNotice = "Backup uploaded to iCloud."
        } catch {
            cloudBackupError = error.localizedDescription
        }
        #else
        cloudBackupError = "iCloud backup is unavailable on this platform."
        #endif
    }

    private func restoreCloudBackup() async {
        cloudBackupBusy = true
        cloudBackupNotice = ""
        cloudBackupError = ""
        defer { cloudBackupBusy = false }
        guard seedService.isICloudBackupAvailable() else {
            iCloudBackupEnabled = false
            cloudBackupError = "iCloud backup is unavailable for this build."
            return
        }
        #if canImport(CloudKit)
        do {
            let restored = try await seedService.restoreEncryptedBackupFromICloud()
            if restored {
                cloudBackupNotice = "Backup restored from iCloud."
            } else {
                cloudBackupNotice = "No backup changes were applied."
            }
        } catch {
            cloudBackupError = error.localizedDescription
        }
        #else
        cloudBackupError = "iCloud backup is unavailable on this platform."
        #endif
    }

    private var appVersionString: String {
        let version = Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String ?? "?"
        let build = Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") as? String ?? "?"
        return "\(version) (\(build))"
    }

    private func handleAboutTap() {
        guard !advancedSettingsEnabled else { return }
        aboutTapCount += 1
        if aboutTapCount >= 7 {
            advancedSettingsEnabled = true
            aboutNotice = "Advanced settings enabled."
            aboutTapCount = 0
        }
    }

    private func openSupport() {
        if let email = bundleString(for: "SemaySupportEmail"),
           !email.isEmpty,
           var components = URLComponents(string: "mailto:\(email)") {
            components.queryItems = [
                URLQueryItem(name: "subject", value: "Semay Support")
            ]
            if let mailtoURL = components.url {
                openURL(mailtoURL)
                legalSupportNotice = ""
                return
            }
        }

        openConfiguredURL(infoKey: "SemaySupportURL", label: "Support URL")
    }

    private func openConfiguredURL(infoKey: String, label: String) {
        guard let urlString = bundleString(for: infoKey),
              let url = URL(string: urlString),
              let scheme = url.scheme,
              !scheme.isEmpty else {
            legalSupportNotice = "\(label) is not configured."
            return
        }
        openURL(url)
        legalSupportNotice = ""
    }

    private func bundleString(for key: String) -> String? {
        guard let value = Bundle.main.object(forInfoDictionaryKey: key) as? String else {
            return nil
        }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}

private struct SemayRestoreSeedSheet: View {
    @Binding var isPresented: Bool
    @EnvironmentObject private var seedService: SeedPhraseService
    @EnvironmentObject private var dataStore: SemayDataStore

    @State private var phrase = ""
    @State private var error: String?
    @State private var confirmErase = false

    var body: some View {
        NavigationStack {
            Form {
                Section("Restore From Seed") {
                    Text("This replaces your identity on this device. It also clears local Semay data (pins, businesses, promises, outbox). Offline map packs stay installed.")
                        .font(.caption)
                        .foregroundStyle(.secondary)

                    TextField("Enter 12 words", text: $phrase, axis: .vertical)
                        .semayDisableAutoCaps()
                        .semayDisableAutocorrection()
                }

                Section {
                    Toggle("I understand this will erase local Semay data", isOn: $confirmErase)
                }

                if let error {
                    Section("Error") {
                        Text(error)
                            .foregroundStyle(.orange)
                    }
                }
            }
            .navigationTitle("Restore")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { isPresented = false }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Restore") {
                        guard confirmErase else {
                            error = "Please confirm the erase toggle to continue."
                            return
                        }
                        do {
                            try seedService.restoreFromPhrase(phrase)
                            dataStore.wipeLocalDatabaseForRestore()
                            isPresented = false
                        } catch {
                            self.error = error.localizedDescription
                        }
                    }
                    .disabled(phrase.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }
            }
        }
    }
}
