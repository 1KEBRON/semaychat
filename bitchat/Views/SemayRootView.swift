import SwiftUI
import MapKit
#if os(iOS)
import UIKit
import SQLite3
import UniformTypeIdentifiers
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

struct SemayRootView: View {
    @EnvironmentObject var viewModel: ChatViewModel
    @StateObject private var seedService = SeedPhraseService.shared
    @StateObject private var dataStore = SemayDataStore.shared
    @StateObject private var navigation = SemayNavigationState.shared

    @State private var selectedTab: Tab = .map
    @State private var showOnboarding = false

    enum Tab {
        case map
        case chat
        case business
        case me
    }

    var body: some View {
        TabView(selection: $selectedTab) {
            SemayMapTabView()
                .environmentObject(dataStore)
                .tag(Tab.map)
                .tabItem {
                    Label("Map", systemImage: "map")
                }

            ContentView()
                .environmentObject(viewModel)
                .tag(Tab.chat)
                .tabItem {
                    Label("Chat", systemImage: "message")
                }

            SemayBusinessTabView()
                .environmentObject(dataStore)
                .tag(Tab.business)
                .tabItem {
                    Label("Business", systemImage: "building.2")
                }

            SemayMeTabView()
                .environmentObject(dataStore)
                .environmentObject(seedService)
                .tag(Tab.me)
                .tabItem {
                    Label("Me", systemImage: "person.crop.circle")
                }
        }
        .onAppear {
            dataStore.refreshAll()
            showOnboarding = seedService.needsOnboarding()
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
    }

    private func handleSemayDeepLink(_ url: URL) {
        guard url.scheme == "semay" else { return }

        let host = (url.host ?? "").lowercased()
        let parts = url.pathComponents.filter { $0 != "/" }
        guard let first = parts.first, !first.isEmpty else { return }

        switch host {
        case "business":
            navigation.selectedPinID = nil
            navigation.selectedBusinessID = first
            navigation.pendingFocus = true
            selectedTab = .map
        case "pin", "place":
            navigation.selectedBusinessID = nil
            navigation.selectedPinID = first
            navigation.pendingFocus = true
            selectedTab = .map
        default:
            break
        }
    }
}

private struct SeedBackupOnboardingView: View {
    @EnvironmentObject private var seedService: SeedPhraseService
    @Binding var isPresented: Bool

    @State private var phrase: String = ""
    @State private var challenge: SeedPhraseService.BackupChallenge = .init(firstIndex: 2, secondIndex: 9)
    @State private var firstWord = ""
    @State private var secondWord = ""
    @State private var error: String?

    var body: some View {
        NavigationStack {
            Form {
                Section("Start Using Semay") {
                    Text("Write down your 12-word seed. This is your account forever.")
                    Text(phrase)
                        .font(.system(.body, design: .monospaced))
                        .textSelection(.enabled)
                }

                Section("Backup Check") {
                    TextField("Word #\(challenge.firstIndex)", text: $firstWord)
                        .semayDisableAutoCaps()
                        .semayDisableAutocorrection()
                    TextField("Word #\(challenge.secondIndex)", text: $secondWord)
                        .semayDisableAutoCaps()
                        .semayDisableAutocorrection()

                    if let error {
                        Text(error)
                            .foregroundStyle(.red)
                    }
                }
            }
            .navigationTitle("Secure Backup")
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Continue") {
                        guard seedService.verifyChallenge(challenge, firstWord: firstWord, secondWord: secondWord) else {
                            error = "Words do not match. Please check and try again."
                            return
                        }

                        seedService.completeBackup()
                        isPresented = false
                    }
                }
            }
            .onAppear {
                phrase = seedService.getOrCreatePhrase()
                challenge = seedService.createChallenge()
            }
        }
    }
}

private struct SemayMapTabView: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.openURL) private var openURL
    @StateObject private var tileStore = OfflineTileStore.shared
    @StateObject private var libraryStore = LibraryPackStore.shared
    @StateObject private var reachability = NetworkReachabilityService.shared
    @StateObject private var locationState = LocationStateManager.shared
    @ObservedObject private var navigation = SemayNavigationState.shared
    @AppStorage("semay.settings.advanced") private var advancedSettingsEnabled = false

    @State private var region = MKCoordinateRegion(
        center: CLLocationCoordinate2D(latitude: 15.3229, longitude: 38.9251),
        span: MKCoordinateSpan(latitudeDelta: 2.6, longitudeDelta: 2.6)
    )
    @State private var showAddPin = false
    @State private var editingPin: SemayMapPin?
    @State private var pinEditorCoordinate: CLLocationCoordinate2D?
    @State private var editingBusiness: BusinessProfile?
    @State private var useOSMBaseMap = false
    @State private var useOfflineTiles = false
    @State private var showTileImporter = false
    @State private var tileImportMessage: String?
    @State private var mapActionMessage: String?
    @State private var lastAutoPackPath: String?
    @State private var showExplore = false
    @State private var installingCommunityPack = false
    @State private var dismissedOfflineMapBanner = false

    var body: some View {
        NavigationStack {
            ZStack(alignment: .bottom) {
                #if os(iOS)
                SemayMapView(
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
                    useOSMBaseMap: $useOSMBaseMap,
                    offlinePack: tileStore.availablePack,
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

                #if os(iOS)
                if useOfflineTiles, let pack = tileStore.availablePack {
                    VStack(alignment: .leading, spacing: 4) {
                        Text("Offline map: \(pack.name)")
                            .font(.caption2)
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
                    Text("© OpenStreetMap contributors")
                        .font(.caption2)
                        .padding(.horizontal, 8)
                        .padding(.vertical, 4)
                        .background(.ultraThinMaterial, in: Capsule())
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                        .padding(.leading, 12)
                        .padding(.top, 10)
                }

                if tileStore.availablePack == nil, !dismissedOfflineMapBanner {
                    HStack(spacing: 10) {
                        Image(systemName: reachability.isOnline ? "map" : "wifi.slash")
                            .foregroundStyle(.orange)
                        VStack(alignment: .leading, spacing: 2) {
                            Text(reachability.isOnline ? "Offline maps not installed" : "Offline maps not installed")
                                .font(.caption)
                                .fontWeight(.semibold)
                            Text(reachability.isOnline ? "One tap download for Eritrea + Ethiopia." : "Connect to a network to download offline maps.")
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                        Spacer()
                        Button(installingCommunityPack ? "Installing..." : "Install") {
                            Task {
                                await installCommunityPack()
                            }
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(!reachability.isOnline || installingCommunityPack)

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
                #endif

                VStack {
                    HStack {
                        Button {
                            showExplore = true
                        } label: {
                            HStack(spacing: 8) {
                                Image(systemName: "magnifyingglass")
                                Text("Search places, businesses, plus codes")
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
                    }
                    .padding(.horizontal, 12)
                    .padding(.top, (tileStore.availablePack == nil && !dismissedOfflineMapBanner) ? 74 : 12)

                    Spacer()
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)

                if let selected = selectedBusiness {
                    VStack(alignment: .leading, spacing: 8) {
                        HStack {
                            Text(selected.name).font(.headline)
                            Spacer()
                            Text("Business")
                                .font(.caption)
                                .foregroundStyle(.secondary)
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
                        HStack {
                            if let telURL = telURL(for: selected.phone) {
                                Button("Call") {
                                    openURL(telURL)
                                }
                                .buttonStyle(.borderedProminent)
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
                } else if let selected = selectedPin {
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
                            if let telURL = telURL(for: selected.phone) {
                                Button("Call") {
                                    openURL(telURL)
                                }
                                .buttonStyle(.bordered)
                            }
                            Button("Directions") {
                                openDirections(latitude: selected.latitude, longitude: selected.longitude, name: selected.name)
                            }
                            .buttonStyle(.bordered)
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
                } else {
                    Text("Tap a pin to view details. Use + to add places.")
                        .font(.caption)
                        .padding(10)
                        .background(.ultraThinMaterial, in: Capsule())
                        .padding()
                }
            }
            .navigationTitle("Map")
            .toolbar {
                #if os(iOS)
                ToolbarItem(placement: .topBarLeading) {
                    Menu("Jump") {
                        Button("Asmara") {
                            centerMap(latitude: 15.3229, longitude: 38.9251, zoomDelta: 0.18)
                        }
                        Button("Addis Ababa") {
                            centerMap(latitude: 8.9806, longitude: 38.7578, zoomDelta: 0.18)
                        }
                        Button("Fit All Pins") {
                            fitMapToPins()
                        }
                    }
                }
                #else
                ToolbarItem(placement: .navigation) {
                    Menu("Jump") {
                        Button("Asmara") {
                            centerMap(latitude: 15.3229, longitude: 38.9251, zoomDelta: 0.18)
                        }
                        Button("Addis Ababa") {
                            centerMap(latitude: 8.9806, longitude: 38.7578, zoomDelta: 0.18)
                        }
                        Button("Fit All Pins") {
                            fitMapToPins()
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
            }
            .sheet(isPresented: $showAddPin) {
                    AddPinSheet(isPresented: $showAddPin, existingPin: editingPin, initialCoordinate: pinEditorCoordinate)
                        .environmentObject(dataStore)
            }
            .sheet(item: $editingBusiness) { business in
                BusinessEditorSheet(existingBusiness: business)
                    .environmentObject(dataStore)
            }
            .sheet(isPresented: $showExplore) {
                SemayExploreSheet(
                    isPresented: $showExplore,
                    region: $region,
                    pins: dataStore.pins,
                    businesses: dataStore.businesses,
                    libraryStore: libraryStore,
                    selectedPinID: Binding(
                        get: { navigation.selectedPinID },
                        set: { navigation.selectedPinID = $0 }
                    ),
                    selectedBusinessID: Binding(
                        get: { navigation.selectedBusinessID },
                        set: { navigation.selectedBusinessID = $0 }
                    )
                )
            }
            .onAppear {
                tileStore.refresh()
                // Default UX: use native MapKit when online; fall back to offline tiles when offline.
                updateBaseLayerForConnectivity()
                fitMapToPins()
            }
            .onChange(of: reachability.isOnline) { _ in
                updateBaseLayerForConnectivity()
            }
            .alert("Semay", isPresented: Binding(
                get: { mapActionMessage != nil },
                set: { if !$0 { mapActionMessage = nil } }
            )) {
                Button("OK") { mapActionMessage = nil }
            } message: {
                if let mapActionMessage {
                    Text(mapActionMessage)
                }
            }
            #if os(iOS)
            .fileImporter(
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
                        tileImportMessage = "Failed to import tiles: \(error.localizedDescription)"
                    }
                case .failure(let error):
                    tileImportMessage = "Failed to import tiles: \(error.localizedDescription)"
                }
            }
            .alert("Offline Map", isPresented: Binding(
                get: { tileImportMessage != nil },
                set: { if !$0 { tileImportMessage = nil } }
            )) {
                Button("OK") { tileImportMessage = nil }
            } message: {
                if let tileImportMessage {
                    Text(tileImportMessage)
                }
            }
            #endif
            .onChange(of: useOfflineTiles) { newValue in
                if newValue {
                    useOSMBaseMap = false
                }
            }
            .onChange(of: useOSMBaseMap) { newValue in
                if newValue {
                    useOfflineTiles = false
                }
            }
            .onChange(of: region.center.latitude) { _ in
                autoSelectPackIfNeeded()
            }
            .onChange(of: region.center.longitude) { _ in
                autoSelectPackIfNeeded()
            }
            .onChange(of: dataStore.pins.count) { _ in
                if !navigation.pendingFocus {
                    fitMapToPins()
                }
            }
            .onChange(of: navigation.selectedBusinessID) { _ in
                guard navigation.pendingFocus else { return }
                guard let id = navigation.selectedBusinessID else { return }
                if let b = dataStore.businesses.first(where: { $0.businessID == id }) {
                    centerMap(latitude: b.latitude, longitude: b.longitude, zoomDelta: 0.12)
                    navigation.pendingFocus = false
                }
            }
            .onChange(of: navigation.selectedPinID) { _ in
                guard navigation.pendingFocus else { return }
                guard let id = navigation.selectedPinID else { return }
                if let pin = dataStore.pins.first(where: { $0.pinID == id }) {
                    centerMap(latitude: pin.latitude, longitude: pin.longitude, zoomDelta: 0.12)
                    navigation.pendingFocus = false
                }
            }
            .onChange(of: dataStore.businesses.count) { _ in
                guard navigation.pendingFocus else { return }
                if let id = navigation.selectedBusinessID,
                   let b = dataStore.businesses.first(where: { $0.businessID == id }) {
                    centerMap(latitude: b.latitude, longitude: b.longitude, zoomDelta: 0.12)
                    navigation.pendingFocus = false
                }
            }
        }
    }

    private var selectedPin: SemayMapPin? {
        guard let id = navigation.selectedPinID else { return nil }
        return dataStore.pins.first(where: { $0.pinID == id })
    }

    private var selectedBusiness: BusinessProfile? {
        guard let id = navigation.selectedBusinessID else { return nil }
        return dataStore.businesses.first(where: { $0.businessID == id })
    }

    private func centerMap(latitude: Double, longitude: Double, zoomDelta: Double) {
        region = MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: latitude, longitude: longitude),
            span: MKCoordinateSpan(latitudeDelta: zoomDelta, longitudeDelta: zoomDelta)
        )
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
        let latDelta = max(0.08, (maxLat - minLat) * 1.6)
        let lonDelta = max(0.08, (maxLon - minLon) * 1.6)
        region = MKCoordinateRegion(center: center, span: MKCoordinateSpan(latitudeDelta: latDelta, longitudeDelta: lonDelta))
    }

    private func autoSelectPackIfNeeded() {
        guard useOfflineTiles else { return }
        guard let best = tileStore.bestPack(forLatitude: region.center.latitude, longitude: region.center.longitude) else { return }
        guard best.path != tileStore.availablePack?.path else { return }
        guard best.path != lastAutoPackPath else { return }
        lastAutoPackPath = best.path
        tileStore.selectPack(best)
    }

    private func updateBaseLayerForConnectivity() {
        // Keep the default map looking native whenever we can.
        // Offline tiles are only used when offline (and a pack is installed).
        if !reachability.isOnline, tileStore.availablePack != nil {
            useOfflineTiles = true
            useOSMBaseMap = false
            autoSelectPackIfNeeded()
        } else {
            useOfflineTiles = false
            useOSMBaseMap = false
        }
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
            let installed = try await tileStore.installRecommendedPack()
            _ = installed
            updateBaseLayerForConnectivity()
            tileImportMessage = "Installed \(installed.name)."
        } catch {
            tileImportMessage = error.localizedDescription
        }
    }
}

#if os(iOS)
private struct TilePackInfoSheet: View {
    @Binding var isPresented: Bool
    @ObservedObject var tileStore: OfflineTileStore
    @Binding var useOfflineTiles: Bool
    @Binding var useOSMBaseMap: Bool
    @AppStorage("semay.settings.advanced") private var advancedSettingsEnabled = false
    @State private var hubPacks: [HubTilePack] = []
    @State private var loadingHubPacks = false
    @State private var downloadingPackID: String?
    @State private var publishingPack = false
    @State private var hubError = ""
    @State private var hubNotice = ""

    private var selectedPack: OfflineTilePack? {
        tileStore.availablePack
    }

    var body: some View {
        NavigationStack {
            Form {
                Section("Selected Pack") {
                    if let pack = selectedPack {
                        LabeledContent("Name", value: pack.name)
                        LabeledContent("Zoom", value: "\(pack.minZoom)–\(pack.maxZoom)")
                        if let bounds = pack.bounds {
                            LabeledContent("Bounds", value: "\(format(bounds.minLat)),\(format(bounds.minLon)) → \(format(bounds.maxLat)),\(format(bounds.maxLon))")
                        } else {
                            LabeledContent("Bounds", value: "Unknown")
                        }
                        LabeledContent("Size", value: formatSize(pack.sizeBytes))
                        LabeledContent("Attribution", value: pack.attribution)
                    } else {
                        Text("No offline pack selected.")
                            .foregroundStyle(.secondary)
                    }
                }

                Section("Available Packs") {
                    if tileStore.packs.isEmpty {
                        Text("No packs installed.")
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(tileStore.packs, id: \.path) { pack in
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
                                    }
                                    Spacer()
                                    if pack.path == selectedPack?.path {
                                        Image(systemName: "checkmark.circle.fill")
                                            .foregroundStyle(.green)
                                    }
                                }
                            }
                        }
                    }
                }

                if let pack = selectedPack {
                    Section("Manage") {
                        if advancedSettingsEnabled {
                            Button(publishingPack ? "Publishing..." : "Publish Selected Pack to Source") {
                                Task {
                                    await publishPack(pack)
                                }
                            }
                            .disabled(publishingPack)
                        }
                        Button(role: .destructive) {
                            removePack(pack)
                        } label: {
                            Text("Delete Pack")
                        }
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

                        if hubPacks.isEmpty {
                            Text("No packs loaded from node yet.")
                                .foregroundStyle(.secondary)
                        } else {
                            ForEach(hubPacks, id: \.id) { pack in
                                HStack(alignment: .center, spacing: 12) {
                                    VStack(alignment: .leading, spacing: 4) {
                                        Text(pack.name)
                                        Text("Zoom \(pack.minZoom)-\(pack.maxZoom) • \(formatSize(pack.sizeBytes))")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                    }
                                    Spacer()
                                    if selectedPack?.path.lowercased().hasSuffix(pack.fileName.lowercased()) == true {
                                        Image(systemName: "checkmark.circle.fill")
                                            .foregroundStyle(.green)
                                    } else {
                                        Button(downloadingPackID == pack.id ? "Installing..." : "Install") {
                                            Task {
                                                await installHubPack(pack)
                                            }
                                        }
                                        .disabled(downloadingPackID == pack.id)
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
        }
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
            let installed = try await tileStore.installHubPack(pack)
            useOfflineTiles = true
            useOSMBaseMap = false
            hubNotice = "Installed \(installed.name) from node."
        } catch {
            hubError = error.localizedDescription
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
            hubError = error.localizedDescription
        }
    }

    private func removePack(_ pack: OfflineTilePack) {
        let fm = FileManager.default
        do {
            try fm.removeItem(atPath: pack.path)
        } catch {
            // Ignore delete failures; refresh anyway.
        }
        tileStore.refresh()
        if tileStore.availablePack == nil {
            useOfflineTiles = false
            useOSMBaseMap = false
        }
    }

    private func format(_ value: Double) -> String {
        String(format: "%.4f", value)
    }

    private func formatSize(_ bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB]
        formatter.countStyle = .file
        return formatter.string(fromByteCount: bytes)
    }
}
#endif

#if os(iOS)
private struct SemayMapView: UIViewRepresentable {
    @Binding var region: MKCoordinateRegion
    let pins: [SemayMapPin]
    @Binding var selectedPinID: String?
    let businesses: [BusinessProfile]
    @Binding var selectedBusinessID: String?
    @Binding var useOSMBaseMap: Bool
    let offlinePack: OfflineTilePack?
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
            offlinePack: offlinePack,
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
            offlinePack: offlinePack,
            useOfflineTiles: useOfflineTiles
        )
        context.coordinator.syncAnnotations(on: mapView, pins: pins, businesses: businesses)
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    final class Coordinator: NSObject, MKMapViewDelegate {
        private let parent: SemayMapView
        private let osmOverlay = MKTileOverlay(
            urlTemplate: "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        )
        private var offlineOverlay: MBTilesOverlay?
        private var offlineOverlayPath: String?

        init(_ parent: SemayMapView) {
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
            offlinePack: OfflineTilePack?,
            useOfflineTiles: Bool
        ) {
            if useOfflineTiles, let pack = offlinePack {
                ensureOfflineOverlay(pack: pack)
                if let offlineOverlay, !mapView.overlays.contains(where: { $0 === offlineOverlay }) {
                    mapView.addOverlay(offlineOverlay, level: .aboveLabels)
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

            if let offlineOverlay, mapView.overlays.contains(where: { $0 === offlineOverlay }) {
                mapView.removeOverlay(offlineOverlay)
            }
        }

        private func ensureOfflineOverlay(pack: OfflineTilePack) {
            if offlineOverlayPath == pack.path, offlineOverlay != nil {
                return
            }
            offlineOverlayPath = pack.path
            offlineOverlay = MBTilesOverlay(path: pack.path, minZoom: pack.minZoom, maxZoom: pack.maxZoom)
            offlineOverlay?.canReplaceMapContent = true
        }

        func syncAnnotations(on mapView: MKMapView, pins: [SemayMapPin], businesses: [BusinessProfile]) {
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
        }

        func mapView(_ mapView: MKMapView, didSelect view: MKAnnotationView) {
            if let pin = view.annotation as? SemayPinAnnotation {
                parent.selectedBusinessID = nil
                parent.selectedPinID = pin.pinID
                return
            }
            if let business = view.annotation as? SemayBusinessAnnotation {
                parent.selectedPinID = nil
                parent.selectedBusinessID = business.businessID
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

            return nil
        }

        func mapView(_ mapView: MKMapView, rendererFor overlay: MKOverlay) -> MKOverlayRenderer {
            if let tileOverlay = overlay as? MKTileOverlay {
                return MKTileOverlayRenderer(tileOverlay: tileOverlay)
            }
            return MKOverlayRenderer(overlay: overlay)
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
    @Binding var isPresented: Bool
    @Binding var region: MKCoordinateRegion
    let pins: [SemayMapPin]
    let businesses: [BusinessProfile]
    @ObservedObject var libraryStore: LibraryPackStore
    @Binding var selectedPinID: String?
    @Binding var selectedBusinessID: String?

    @State private var query: String = ""
    @State private var segment: Segment = .places
    @State private var installingLibraryPack = false
    @State private var libraryError: String?
    @State private var readerItem: SemayLibraryItem?

    private enum Segment: String, CaseIterable, Identifiable {
        case places = "Places"
        case businesses = "Businesses"
        case library = "Library"
        case routes = "Routes"

        var id: String { rawValue }
    }

    var body: some View {
        NavigationStack {
            VStack(spacing: 12) {
                VStack(spacing: 10) {
                    TextField("Search name, category, plus code, or E-address", text: $query)
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
                    switch segment {
                    case .places:
                        placesSection
                    case .businesses:
                        businessesSection
                    case .library:
                        librarySection
                    case .routes:
                        Section {
                            Text("Curated routes coming soon.")
                                .foregroundStyle(.secondary)
                        }
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
        }
    }

    private var normalizedQuery: String {
        query.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private var filteredPins: [SemayMapPin] {
        let q = normalizedQuery
        if q.isEmpty { return pins }
        return pins.filter { pin in
            pin.name.lowercased().contains(q)
                || pin.type.lowercased().contains(q)
                || pin.details.lowercased().contains(q)
                || pin.eAddress.lowercased().contains(q)
                || pin.plusCode.lowercased().contains(q)
        }
    }

    private var filteredBusinesses: [BusinessProfile] {
        let q = normalizedQuery
        if q.isEmpty { return businesses }
        return businesses.filter { b in
            b.name.lowercased().contains(q)
                || b.category.lowercased().contains(q)
                || b.details.lowercased().contains(q)
                || b.eAddress.lowercased().contains(q)
                || b.plusCode.lowercased().contains(q)
                || b.phone.lowercased().contains(q)
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
                        selectedPinID = pin.pinID
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
                        selectedBusinessID = business.businessID
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
    private var librarySection: some View {
        Section {
            if libraryStore.packs.isEmpty {
                Text("Library not installed.")
                    .foregroundStyle(.secondary)
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
                .disabled(installingLibraryPack)

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

    private func focus(latitude: Double, longitude: Double) {
        region = MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: latitude, longitude: longitude),
            span: MKCoordinateSpan(latitudeDelta: 0.06, longitudeDelta: 0.06)
        )
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

private struct SemayBusinessTabView: View {
    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.openURL) private var openURL

    @State private var showRegisterBusiness = false
    @State private var qrBusiness: BusinessProfile?
    @State private var editingBusiness: BusinessProfile?

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
                            VStack(alignment: .leading, spacing: 6) {
                                Text("\(promise.amountMsat) msat")
                                    .font(.headline)
                                Text("\(promise.status.rawValue.capitalized) • Expires \(Date(timeIntervalSince1970: TimeInterval(promise.expiresAt)).formatted())")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                HStack {
                                    if promise.status == .pending {
                                        Button("Accept") {
                                            _ = dataStore.updatePromiseStatus(promise.promiseID, status: .accepted)
                                        }
                                        .buttonStyle(.borderedProminent)

                                        Button("Reject") {
                                            _ = dataStore.updatePromiseStatus(promise.promiseID, status: .rejected)
                                        }
                                        .buttonStyle(.bordered)
                                    }

                                    if promise.status == .accepted {
                                        Button("Settle") {
                                            _ = dataStore.submitSettlement(
                                                promiseID: promise.promiseID,
                                                proofType: .lightningPaymentHash,
                                                proofValue: UUID().uuidString.replacingOccurrences(of: "-", with: ""),
                                                submittedBy: .merchant
                                            )
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

    @State private var name = ""
    @State private var category = "shop"
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
                TextField("Business Name", text: $name)
                TextField("Category", text: $category)
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
                                phone: phone
                            )
                            if updated == nil {
                                error = "Only the business owner can update this profile right now."
                                return
                            }
                            dismiss()
                            return
                        }

                        _ = dataStore.registerBusiness(
                            name: name,
                            category: category,
                            details: details,
                            latitude: coord.latitude,
                            longitude: coord.longitude,
                            phone: phone
                        )
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
                    latitude = String(format: "%.6f", existingBusiness.latitude)
                    longitude = String(format: "%.6f", existingBusiness.longitude)
                } else if locationState.permissionState == .authorized,
                          let loc = locationState.lastKnownLocation {
                    latitude = String(format: "%.6f", loc.coordinate.latitude)
                    longitude = String(format: "%.6f", loc.coordinate.longitude)
                }
            }
        }
    }
}

	private struct SemayMeTabView: View {
	    @EnvironmentObject private var dataStore: SemayDataStore
	    @EnvironmentObject private var seedService: SeedPhraseService
	    @AppStorage("semay.settings.advanced") private var advancedSettingsEnabled = false
	    @StateObject private var safety = SafetyModeService.shared
	    @StateObject private var envelopeSync = SemayEnvelopeSyncService.shared
	    @StateObject private var tileStore = OfflineTileStore.shared
	    @StateObject private var reachability = NetworkReachabilityService.shared
	
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

                Section("Settings") {
                    Toggle("Advanced Settings", isOn: $advancedSettingsEnabled)
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

	                Section("Offline Maps") {
	                    if let pack = tileStore.availablePack {
	                        Text("Installed: \(pack.name)")
	                        Text("Size: \(formatSize(pack.sizeBytes))")
	                            .font(.caption)
	                            .foregroundStyle(.secondary)
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
	                        Text("Install once so Semay stays useful when the internet is down.")
	                            .font(.caption)
	                            .foregroundStyle(.secondary)
	
	                        Button(installingOfflineMaps ? "Installing..." : "Install Offline Maps") {
	                            Task {
	                                await installOfflineMaps()
	                            }
	                        }
	                        .disabled(!reachability.isOnline || installingOfflineMaps)
	
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

	                if advancedSettingsEnabled {
	                    Section("Node (Advanced)") {
	                        Text("Leave this blank to auto-detect a nearby node. Set it only if you're operating your own Semay node.")
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
                                    let discovered = try await tileStore.discoverMapSourceURL()
                                    hubBaseURL = discovered
                                    dataStore.saveHubConfig(baseURL: discovered, token: hubToken)
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
	            let installed = try await tileStore.installRecommendedPack()
	            offlineMapsNotice = "Installed \(installed.name)."
	        } catch {
	            offlineMapsError = error.localizedDescription
	        }
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
