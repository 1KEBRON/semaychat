import BitLogger
import Combine
import Foundation

#if canImport(MapLibre)
private let semayMapLibreBuildAvailable = true
#else
private let semayMapLibreBuildAvailable = false
#endif

enum MapEngine: String, CaseIterable, Identifiable {
    case mapkit
    case maplibre

    var id: String { rawValue }

    var label: String {
        switch self {
        case .mapkit:
            return "MapKit"
        case .maplibre:
            return "Semay Map"
        }
    }
}

struct MapLibreStabilitySnapshot: Equatable {
    let windowDays: Int
    let observedDays: Int
    let mapLibreSessions: Int
    let mapLibreFallbackSessions: Int
    let fallbackFreeRate: Double
    let targetRate: Double

    var successfulSessions: Int {
        max(0, mapLibreSessions - mapLibreFallbackSessions)
    }

    var meetsGate: Bool {
        observedDays >= windowDays && mapLibreSessions > 0 && fallbackFreeRate >= targetRate
    }
}

@MainActor
final class MapEngineCoordinator: ObservableObject {
    static let shared = MapEngineCoordinator()

    @Published private(set) var selectedEngine: MapEngine
    @Published private(set) var sessionFallbackToMapKit = false
    @Published private(set) var mapLibreStabilitySnapshot: MapLibreStabilitySnapshot

    private let preferredEngineKey = "semay.map.engine.preferred"
    private let remoteMapLibreDisabledKey = "semay.map.engine.remote_disable_maplibre"
    private let sessionFallbackLoggedKey = "semay.map.engine.session_fallback_logged"
    private let mapLibreDailyMetricsKey = "semay.map.engine.maplibre.daily.v1"
    private let mapLibreMetricsWindowDays = 5
    private let mapLibreStabilityTargetRate = 99.8

    private let userDefaults: UserDefaults
    private let nowProvider: () -> Date
    private let mapLibreBuildAvailableOverride: Bool?
    private var didRecordMapLibreSessionStart = false
    private var didRecordMapLibreSessionFailure = false

    private static let dayFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }()

    init(
        userDefaults: UserDefaults = .standard,
        nowProvider: @escaping () -> Date = Date.init,
        mapLibreBuildAvailableOverride: Bool? = nil
    ) {
        self.userDefaults = userDefaults
        self.nowProvider = nowProvider
        self.mapLibreBuildAvailableOverride = mapLibreBuildAvailableOverride

        let stored = userDefaults.string(forKey: preferredEngineKey) ?? ""
        selectedEngine = MapEngine(rawValue: stored) ?? .mapkit
        mapLibreStabilitySnapshot = MapLibreStabilitySnapshot(
            windowDays: mapLibreMetricsWindowDays,
            observedDays: 0,
            mapLibreSessions: 0,
            mapLibreFallbackSessions: 0,
            fallbackFreeRate: 100,
            targetRate: mapLibreStabilityTargetRate
        )

        mapLibreStabilitySnapshot = loadMapLibreStabilitySnapshot()
        recordMapLibreSessionStartIfNeeded()
    }

    var mapLibreBuildAvailable: Bool {
        mapLibreBuildAvailableOverride ?? semayMapLibreBuildAvailable
    }

    var mapLibreRemoteDisabled: Bool {
        if userDefaults.object(forKey: remoteMapLibreDisabledKey) == nil {
            return false
        }
        return userDefaults.bool(forKey: remoteMapLibreDisabledKey)
    }

    var mapLibreAllowed: Bool {
        mapLibreBuildAvailable && !mapLibreRemoteDisabled
    }

    var effectiveEngine: MapEngine {
        guard selectedEngine == .maplibre else { return .mapkit }
        guard mapLibreAllowed else { return .mapkit }
        if sessionFallbackToMapKit {
            return .mapkit
        }
        return .maplibre
    }

    func setPreferredEngine(_ engine: MapEngine) {
        selectedEngine = engine
        userDefaults.set(engine.rawValue, forKey: preferredEngineKey)

        if engine == .mapkit {
            sessionFallbackToMapKit = false
            didRecordMapLibreSessionStart = false
            didRecordMapLibreSessionFailure = false
            userDefaults.removeObject(forKey: sessionFallbackLoggedKey)
            return
        }

        recordMapLibreSessionStartIfNeeded()
    }

    func setRemoteMapLibreDisabled(_ disabled: Bool) {
        userDefaults.set(disabled, forKey: remoteMapLibreDisabledKey)
        if !disabled {
            recordMapLibreSessionStartIfNeeded()
        }
    }

    func markMapLibreFailure(_ reason: String) {
        guard selectedEngine == .maplibre else { return }

        recordMapLibreSessionStartIfNeeded()
        recordMapLibreSessionFailureIfNeeded()
        guard !sessionFallbackToMapKit else { return }

        sessionFallbackToMapKit = true
        if !userDefaults.bool(forKey: sessionFallbackLoggedKey) {
            SecureLogger.warning("MapLibre runtime fallback to MapKit: \(reason)", category: .session)
            userDefaults.set(true, forKey: sessionFallbackLoggedKey)
        }
    }

    func resetSessionFallback() {
        sessionFallbackToMapKit = false
        userDefaults.removeObject(forKey: sessionFallbackLoggedKey)
    }

    func refreshRuntimeState() {
        mapLibreStabilitySnapshot = loadMapLibreStabilitySnapshot()
        recordMapLibreSessionStartIfNeeded()
    }

    func clearMapLibreStabilityMetrics() {
        userDefaults.removeObject(forKey: mapLibreDailyMetricsKey)
        didRecordMapLibreSessionStart = false
        didRecordMapLibreSessionFailure = false
        mapLibreStabilitySnapshot = loadMapLibreStabilitySnapshot()
    }

    private struct DailyMapLibreMetric: Codable {
        var sessions: Int
        var fallbacks: Int

        static let zero = DailyMapLibreMetric(sessions: 0, fallbacks: 0)
    }

    private func dayKey(for date: Date) -> String {
        Self.dayFormatter.string(from: date)
    }

    private func parseDayKey(_ key: String) -> Date? {
        Self.dayFormatter.date(from: key)
    }

    private func loadDailyMetrics() -> [String: DailyMapLibreMetric] {
        guard let data = userDefaults.data(forKey: mapLibreDailyMetricsKey),
              let decoded = try? JSONDecoder().decode([String: DailyMapLibreMetric].self, from: data) else {
            return [:]
        }
        return decoded
    }

    private func saveDailyMetrics(_ metrics: [String: DailyMapLibreMetric]) {
        guard let encoded = try? JSONEncoder().encode(metrics) else { return }
        userDefaults.set(encoded, forKey: mapLibreDailyMetricsKey)
    }

    private func prunedDailyMetrics(_ metrics: [String: DailyMapLibreMetric], now: Date) -> [String: DailyMapLibreMetric] {
        let calendar = Calendar(identifier: .gregorian)
        let today = calendar.startOfDay(for: now)
        guard let cutoff = calendar.date(byAdding: .day, value: -(mapLibreMetricsWindowDays - 1), to: today) else {
            return metrics
        }

        var pruned: [String: DailyMapLibreMetric] = [:]
        for (key, metric) in metrics {
            guard let date = parseDayKey(key) else { continue }
            let normalizedDate = calendar.startOfDay(for: date)
            guard normalizedDate >= cutoff && normalizedDate <= today else { continue }
            let normalizedFallbacks = min(max(metric.fallbacks, 0), max(metric.sessions, 0))
            pruned[key] = DailyMapLibreMetric(
                sessions: max(metric.sessions, 0),
                fallbacks: normalizedFallbacks
            )
        }
        return pruned
    }

    private func mutateDailyMetrics(_ update: (_ metrics: inout [String: DailyMapLibreMetric], _ todayKey: String) -> Void) {
        let now = nowProvider()
        var metrics = prunedDailyMetrics(loadDailyMetrics(), now: now)
        let todayKey = dayKey(for: now)
        update(&metrics, todayKey)
        saveDailyMetrics(metrics)
        mapLibreStabilitySnapshot = snapshot(from: metrics)
    }

    private func loadMapLibreStabilitySnapshot() -> MapLibreStabilitySnapshot {
        let now = nowProvider()
        let pruned = prunedDailyMetrics(loadDailyMetrics(), now: now)
        saveDailyMetrics(pruned)
        return snapshot(from: pruned)
    }

    private func snapshot(from metrics: [String: DailyMapLibreMetric]) -> MapLibreStabilitySnapshot {
        let sessions = metrics.values.reduce(0) { $0 + max(0, $1.sessions) }
        let fallbacks = metrics.values.reduce(0) { partial, metric in
            partial + min(max(metric.fallbacks, 0), max(metric.sessions, 0))
        }
        let observedDays = metrics.values.filter { $0.sessions > 0 || $0.fallbacks > 0 }.count
        let fallbackFreeRate: Double
        if sessions == 0 {
            fallbackFreeRate = 100
        } else {
            fallbackFreeRate = (Double(max(0, sessions - fallbacks)) / Double(sessions)) * 100
        }

        return MapLibreStabilitySnapshot(
            windowDays: mapLibreMetricsWindowDays,
            observedDays: observedDays,
            mapLibreSessions: sessions,
            mapLibreFallbackSessions: fallbacks,
            fallbackFreeRate: fallbackFreeRate,
            targetRate: mapLibreStabilityTargetRate
        )
    }

    private func recordMapLibreSessionStartIfNeeded() {
        guard selectedEngine == .maplibre else { return }
        guard mapLibreAllowed else { return }
        guard !didRecordMapLibreSessionStart else { return }

        didRecordMapLibreSessionStart = true
        didRecordMapLibreSessionFailure = false
        mutateDailyMetrics { metrics, todayKey in
            var today = metrics[todayKey] ?? .zero
            today.sessions += 1
            if today.fallbacks > today.sessions {
                today.fallbacks = today.sessions
            }
            metrics[todayKey] = today
        }
    }

    private func recordMapLibreSessionFailureIfNeeded() {
        guard didRecordMapLibreSessionStart else { return }
        guard !didRecordMapLibreSessionFailure else { return }

        didRecordMapLibreSessionFailure = true
        mutateDailyMetrics { metrics, todayKey in
            var today = metrics[todayKey] ?? .zero
            today.sessions = max(today.sessions, 1)
            today.fallbacks = min(today.fallbacks + 1, today.sessions)
            metrics[todayKey] = today
        }
    }
}
