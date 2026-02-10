import SwiftUI

protocol SemayModule {
    var moduleID: String { get }
    var tabTitle: String { get }
    func rootView() -> AnyView
    func handle(_ envelope: SemayEventEnvelope) async
}
