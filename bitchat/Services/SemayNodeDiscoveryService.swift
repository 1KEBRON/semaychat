import Foundation

private let hubBaseURLKey = "semay.hub.base_url"
private let hubIngestTokenKey = "semay.hub.ingest_token"

/// Resolve a Semay "backbone node" base URL without requiring normal users to configure anything.
///
/// This service probes `GET /api/node` on a short candidate list and persists the first reachable URL.
@MainActor
final class SemayNodeDiscoveryService: ObservableObject {
    static let shared = SemayNodeDiscoveryService()

    @Published private(set) var activeBaseURL: String?
    @Published private(set) var activeNodeName: String?

    private var cachedDescriptor: SemayNodeDescriptor?

    private let defaultCandidates = [
        "https://hub.semay.app",
        "http://semayhub.local:5000",
        "http://semayhub.local:5055",
        "http://localhost:5000",
        "http://localhost:5055",
        "http://127.0.0.1:5000",
        "http://127.0.0.1:5055",
    ]

    private init() {
        activeBaseURL = configuredBaseURL()?.absoluteString
    }

    func resolveBaseURL(forceDiscovery: Bool = false) async throws -> URL {
        if !forceDiscovery, let configured = configuredBaseURL() {
            if let descriptor = await probeNodeDescriptor(baseURL: configured) {
                cacheActive(baseURL: configured, descriptor: descriptor, persist: false)
                return configured
            }
        }

        var seen = Set<String>()
        var candidates: [URL] = []

        if let configured = configuredBaseURL() {
            candidates.append(configured)
            seen.insert(configured.absoluteString)
        }
        for raw in defaultCandidates {
            guard let url = URL(string: raw) else { continue }
            if seen.contains(url.absoluteString) { continue }
            seen.insert(url.absoluteString)
            candidates.append(url)
        }

        for candidate in candidates {
            if let descriptor = await probeNodeDescriptor(baseURL: candidate) {
                cacheActive(baseURL: candidate, descriptor: descriptor, persist: true)
                return candidate
            }
        }

        throw NSError(
            domain: "semay.node",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "No Semay node is reachable right now. You can keep using Semay offline."]
        )
    }

    func discoverBaseURLString() async throws -> String {
        let url = try await resolveBaseURL(forceDiscovery: true)
        return url.absoluteString
    }

    private func configuredBaseURL() -> URL? {
        let raw = UserDefaults.standard.string(forKey: hubBaseURLKey)?.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let raw, !raw.isEmpty else { return nil }
        return URL(string: raw)
    }

    private func hubIngestToken() -> String {
        UserDefaults.standard.string(forKey: hubIngestTokenKey)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    }

    private func authorizedRequest(url: URL, method: String) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.timeoutInterval = 6
        let token = hubIngestToken()
        if !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        return request
    }

    private func probeNodeDescriptor(baseURL: URL) async -> SemayNodeDescriptor? {
        let endpoint = baseURL.appendingPathComponent("api/node")
        let request = authorizedRequest(url: endpoint, method: "GET")
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else { return nil }
            guard (200...299).contains(http.statusCode) else { return nil }
            let decoded = try JSONDecoder().decode(SemayNodeDescriptor.self, from: data)
            return decoded
        } catch {
            return nil
        }
    }

    private func cacheActive(baseURL: URL, descriptor: SemayNodeDescriptor, persist: Bool) {
        cachedDescriptor = descriptor
        _ = cachedDescriptor
        activeBaseURL = baseURL.absoluteString
        activeNodeName = descriptor.nodeName
        if persist {
            UserDefaults.standard.set(baseURL.absoluteString, forKey: hubBaseURLKey)
        }
    }
}
