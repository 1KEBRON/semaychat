import Foundation

@MainActor
final class SemayNavigationState: ObservableObject {
    static let shared = SemayNavigationState()

    @Published var selectedPinID: String?
    @Published var selectedBusinessID: String?
    @Published var pendingFocus: Bool = false

    private init() {}
}

