import Foundation
import SwiftUI
import Combine
import RxSwift

/// A `ViewStore` is an object that can observe state changes and send actions. They are most
/// commonly used in views, such as SwiftUI views, UIView or UIViewController, but they can be
/// used anywhere it makes sense to observe state and send actions.
///
/// In SwiftUI applications, a `ViewStore` is accessed most commonly using the `WithViewStore` view.
/// It can be initialized with a store and a closure that is handed a view store and must return a
/// view to be rendered:
///
///     var body: some View {
///       WithViewStore(self.store) { viewStore in
///         VStack {
///           Text("Current count: \(viewStore.count)")
///           Button("Increment") { viewStore.send(.incrementButtonTapped) }
///         }
///       }
///     }
///
/// In UIKit applications a `ViewStore` can be created from a `Store` and then subscribed to for
/// state updates:
///
///     let store: Store<State, Action>
///     let viewStore: ViewStore<State, Action>
///
///     init(store: Store<State, Action>) {
///       self.store = store
///       self.viewStore = ViewStore(store)
///     }
///
///     func viewDidLoad() {
///       super.viewDidLoad()
///
///       self.viewStore.publisher.count
///         .sink { [weak self] in self?.countLabel.text = $0 }
///         .store(in: &self.cancellables)
///     }
///
///     @objc func incrementButtonTapped() {
///       self.viewStore.send(.incrementButtonTapped)
///     }
///
@dynamicMemberLookup
public final class ViewStore<State, Action> {
  /// An observable on state.
  public let driver: StoreDriver<State>

  private let disposeBag = DisposeBag()

  // N.B. `ViewStore` does not use a `@Published` property, so `objectWillChange`
  // won't be synthesized automatically. To work around issues on iOS 13 we explicitly declare it.
  @available(iOS 13, macOS 10.15, tvOS 13.0, watchOS 6.0, *)
  public private(set) lazy var objectWillChange = ObservableObjectPublisher()

  /// Initializes a view store from a store.
  ///
  /// - Parameters:
  ///   - store: A store.
  ///   - isDuplicate: A function to determine when two `State` values are equal. When values are
  ///     equal, repeat view computations are removed.
  public init(
    _ store: Store<State, Action>,
    removeDuplicates isDuplicate: @escaping (State, State) -> Bool
  ) {
    let observable = store.stateRelay.distinctUntilChanged(isDuplicate)
    self.driver = StoreDriver(observable)
    self.state = store.state
    self._send = store.send
    observable
      .subscribe(onNext: { [weak self] in self?.state = $0 })
      .disposed(by: disposeBag)
  }

  /// The current state.
  public private(set) var state: State {
    willSet {
      if #available(iOS 13.0, macOS 10.15, tvOS 13.0, watchOS 6.0, *) {
        objectWillChange.send()
      }
    }
  }

  let _send: (Action) -> Void

  /// Returns the resulting value of a given key path.
  public subscript<LocalState>(dynamicMember keyPath: KeyPath<State, LocalState>) -> LocalState {
    self.state[keyPath: keyPath]
  }

  /// Sends an action to the store.
  ///
  /// `ViewStore` is not thread safe and you should only send actions to it from the main thread.
  /// If you are wanting to send actions on background threads due to the fact that the reducer
  /// is performing computationally expensive work, then a better way to handle this is to wrap
  /// that work in an `Effect` that is performed on a background thread so that the result can
  /// be fed back into the store.
  ///
  /// - Parameter action: An action.
  public func send(_ action: Action) {
    self._send(action)
  }
}

extension ViewStore where State: Equatable {
  public convenience init(_ store: Store<State, Action>) {
    self.init(store, removeDuplicates: ==)
  }
}

extension ViewStore where State == Void {
  public convenience init(_ store: Store<Void, Action>) {
    self.init(store, removeDuplicates: ==)
  }
}
