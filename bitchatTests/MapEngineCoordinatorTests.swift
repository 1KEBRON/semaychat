import Foundation
import Testing
@testable import bitchat

@Suite(.serialized)
@MainActor
struct MapEngineCoordinatorTests {
    @Test func recordsSingleFallbackPerSession() throws {
        let (defaults, suiteName) = makeIsolatedDefaults(testName: "singleFallback")
        defer { defaults.removePersistentDomain(forName: suiteName) }
        defaults.set(MapEngine.maplibre.rawValue, forKey: "semay.map.engine.preferred")

        let now = Date(timeIntervalSince1970: 1_736_000_000)
        let coordinator = MapEngineCoordinator(
            userDefaults: defaults,
            nowProvider: { now },
            mapLibreBuildAvailableOverride: true
        )
        #expect(coordinator.mapLibreStabilitySnapshot.mapLibreSessions == 1)
        #expect(coordinator.mapLibreStabilitySnapshot.mapLibreFallbackSessions == 0)

        coordinator.markMapLibreFailure("runtime-failure")
        #expect(coordinator.mapLibreStabilitySnapshot.mapLibreFallbackSessions == 1)

        coordinator.markMapLibreFailure("duplicate-runtime-failure")
        #expect(coordinator.mapLibreStabilitySnapshot.mapLibreFallbackSessions == 1)
    }

    @Test func prunesMetricsOutsideFiveDayWindow() throws {
        let (defaults, suiteName) = makeIsolatedDefaults(testName: "rollingWindow")
        defer { defaults.removePersistentDomain(forName: suiteName) }
        defaults.set(MapEngine.maplibre.rawValue, forKey: "semay.map.engine.preferred")

        var now = Date(timeIntervalSince1970: 1_736_000_000)
        let coordinator = MapEngineCoordinator(
            userDefaults: defaults,
            nowProvider: { now },
            mapLibreBuildAvailableOverride: true
        )
        let calendar = Calendar(identifier: .gregorian)

        for _ in 0..<5 {
            now = calendar.date(byAdding: .day, value: 1, to: now) ?? now
            coordinator.setPreferredEngine(.mapkit)
            coordinator.setPreferredEngine(.maplibre)
        }

        let snapshot = coordinator.mapLibreStabilitySnapshot
        #expect(snapshot.windowDays == 5)
        #expect(snapshot.observedDays == 5)
        #expect(snapshot.mapLibreSessions == 5)
    }

    @Test func stabilityGateRequiresFiveObservedDaysAndTargetRate() throws {
        let (defaults, suiteName) = makeIsolatedDefaults(testName: "stabilityGate")
        defer { defaults.removePersistentDomain(forName: suiteName) }
        defaults.set(MapEngine.maplibre.rawValue, forKey: "semay.map.engine.preferred")

        var now = Date(timeIntervalSince1970: 1_736_000_000)
        let coordinator = MapEngineCoordinator(
            userDefaults: defaults,
            nowProvider: { now },
            mapLibreBuildAvailableOverride: true
        )
        let calendar = Calendar(identifier: .gregorian)

        #expect(!coordinator.mapLibreStabilitySnapshot.meetsGate)

        for _ in 0..<4 {
            now = calendar.date(byAdding: .day, value: 1, to: now) ?? now
            coordinator.setPreferredEngine(.mapkit)
            coordinator.setPreferredEngine(.maplibre)
        }

        #expect(coordinator.mapLibreStabilitySnapshot.mapLibreSessions == 5)
        #expect(coordinator.mapLibreStabilitySnapshot.meetsGate)

        coordinator.markMapLibreFailure("runtime-failure")
        #expect(coordinator.mapLibreStabilitySnapshot.mapLibreFallbackSessions == 1)
        #expect(!coordinator.mapLibreStabilitySnapshot.meetsGate)
    }

    private func makeIsolatedDefaults(testName: String) -> (UserDefaults, String) {
        let suiteName = "MapEngineCoordinatorTests.\(testName).\(UUID().uuidString)"
        guard let defaults = UserDefaults(suiteName: suiteName) else {
            fatalError("Could not create isolated UserDefaults suite \(suiteName)")
        }
        defaults.removePersistentDomain(forName: suiteName)
        return (defaults, suiteName)
    }
}
