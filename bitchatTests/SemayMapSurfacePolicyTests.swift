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
}
