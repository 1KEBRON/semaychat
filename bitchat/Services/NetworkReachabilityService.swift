import Foundation
import Network

@MainActor
final class NetworkReachabilityService: ObservableObject {
    static let shared = NetworkReachabilityService()

    @Published private(set) var isOnline: Bool = true
    @Published private(set) var isExpensive: Bool = false
    @Published private(set) var isConstrained: Bool = false

    private let monitor: NWPathMonitor
    private let queue = DispatchQueue(label: "semay.network.reachability")
    private var started = false

    private init() {
        monitor = NWPathMonitor()
        start()
    }

    private func start() {
        guard !started else { return }
        started = true
        monitor.pathUpdateHandler = { [weak self] path in
            Task { @MainActor in
                self?.isOnline = path.status == .satisfied
                self?.isExpensive = path.isExpensive
                self?.isConstrained = path.isConstrained
            }
        }
        monitor.start(queue: queue)
    }
}

