import Foundation

@MainActor
final class SemayNavigationState: ObservableObject {
    static let shared = SemayNavigationState()

    struct PendingCenter: Equatable {
        let latitude: Double
        let longitude: Double
        let zoomDelta: Double
    }

    @Published var selectedPinID: String?
    @Published var selectedBusinessID: String?
    @Published var selectedRouteID: String?
    @Published var selectedServiceID: String?
    @Published var pendingFocus: Bool = false
    @Published var pendingCenter: PendingCenter?
    @Published var inboundEnvelope: SemayEventEnvelope?

    private init() {}
}
