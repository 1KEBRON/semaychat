import Testing
@testable import bitchat

struct SemayMapSurfacePolicyTests {
    @Test func onlineWithoutOfflinePackShowsOnlineMap() {
        let mode = SemayMapSurfaceMode.resolve(isOnline: true, hasUsableOfflinePack: false)
        #expect(mode == .onlineOnly)
    }

    @Test func offlineWithoutUsablePackHidesMap() {
        let mode = SemayMapSurfaceMode.resolve(isOnline: false, hasUsableOfflinePack: false)
        #expect(mode == .hidden)
    }

    @Test func offlineWithUsablePackShowsOfflineMap() {
        let mode = SemayMapSurfaceMode.resolve(isOnline: false, hasUsableOfflinePack: true)
        #expect(mode == .offlineAvailable)
    }

    @Test func selectedMapTabFallsBackToChatWhenMapHidden() {
        let adjusted = SemayRootView.adjustedTabSelection(.map, for: .hidden)
        #expect(adjusted == .chat)
    }

    @Test func nonMapTabStaysSelectedWhenMapHidden() {
        let adjusted = SemayRootView.adjustedTabSelection(.business, for: .hidden)
        #expect(adjusted == .business)
    }

    @Test func onlineWeakCoveragePrefersOnlineBaseLayer() {
        let mode = SemayMapBaseLayerPolicy.resolve(
            isOnline: true,
            isBundledStarterSelected: false,
            bestPackCoverageRatio: 0.20
        )
        #expect(mode == .online)
    }

    @Test func onlineStrongCoveragePrefersOfflineBaseLayer() {
        let mode = SemayMapBaseLayerPolicy.resolve(
            isOnline: true,
            isBundledStarterSelected: false,
            bestPackCoverageRatio: 0.95
        )
        #expect(mode == .offline)
    }

    @Test func offlineWithCoverageUsesOfflineBaseLayer() {
        let mode = SemayMapBaseLayerPolicy.resolve(
            isOnline: false,
            isBundledStarterSelected: false,
            bestPackCoverageRatio: 0.95
        )
        #expect(mode == .offline)
    }

    @Test func offlineWithoutCoverageUsesNoBaseLayer() {
        let mode = SemayMapBaseLayerPolicy.resolve(
            isOnline: false,
            isBundledStarterSelected: false,
            bestPackCoverageRatio: nil
        )
        #expect(mode == .none)
    }

    @Test func bundledStarterNeverForcesOfflineTilesWhenOnline() {
        let mode = SemayMapBaseLayerPolicy.resolve(
            isOnline: true,
            isBundledStarterSelected: true,
            bestPackCoverageRatio: 1.0
        )
        #expect(mode == .online)
    }

    @Test func installCTASuppressedWhenCatalogUnreachable() {
        let visible = SemayMapInstallPromptPolicy.canShowInstallCTA(
            isOnline: true,
            hubCatalogReachable: false,
            communityPackDownloadAvailable: true,
            canInstallBundledStarterPack: false
        )
        #expect(!visible)
    }

    @Test func installCTAVisibleWhenOfflineStarterCanInstall() {
        let visible = SemayMapInstallPromptPolicy.canShowInstallCTA(
            isOnline: false,
            hubCatalogReachable: false,
            communityPackDownloadAvailable: false,
            canInstallBundledStarterPack: true
        )
        #expect(visible)
    }

    @Test func statusBannerResolvesOnline() {
        let mode = SemayMapStatusBannerMode.resolve(isOnline: true, hasActiveOfflinePack: false)
        #expect(mode == .online)
    }

    @Test func statusBannerResolvesOfflinePack() {
        let mode = SemayMapStatusBannerMode.resolve(isOnline: true, hasActiveOfflinePack: true)
        #expect(mode == .offlinePack)
    }

    @Test func statusBannerResolvesOfflineUnavailable() {
        let mode = SemayMapStatusBannerMode.resolve(isOnline: false, hasActiveOfflinePack: false)
        #expect(mode == .offlineUnavailable)
    }
}
