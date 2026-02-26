import Foundation
import RxSwift
import RxRelay
import RxCocoa

/// A store represents the runtime that powers the application. It is the object that you will pass
/// around to views that need to interact with the application.
///
/// You will typically construct a single one of these at the root of your application, and then use
/// the `scope` method to derive more focused stores that can be passed to subviews.
public final class Store<State, Action> {
  let stateRelay: BehaviorRelay<State>
  public internal(set) var state: State {
    get {
      stateRelay.value
    }
    set {
      stateRelay.accept(newValue)
    }
  }

  var effectDisposeBags: [UUID: DisposeBag] = [:]

  private var isSending = false
  private let disposeBag = DisposeBag()
  private let reducer: (inout State, Action) -> Effect<Action, Never>
  private var synchronousActionsToSend: [Action] = []
  private var bufferedActions: [Action] = []

  /// Initializes a store from an initial state, a reducer, and an environment.
  ///
  /// - Parameters:
  ///   - initialState: The state to start the application in.
  ///   - reducer: The reducer that powers the business logic of the application.
  ///   - environment: The environment of dependencies for the application.
  public convenience init<Environment>(
    initialState: State,
    reducer: Reducer<State, Action, Environment>,
    environment: Environment
  ) {
    self.init(
      initialState: initialState,
      reducer: { reducer.run(&$0, $1, environment) }
    )
  }

  /// A version of scoping where the state may not be transformable into local state.
  /// Meant to be private for now. Could be set public if need be.
  /// (ifLet should be enough in most cases)
  func scope<LocalState, LocalAction>(
    initialLocalState: LocalState,
    state toLocalState: @escaping (State) -> LocalState?,
    action fromLocalAction: @escaping (LocalAction) -> Action
  ) -> Store<LocalState, LocalAction> {

    let localStore = Store<LocalState, LocalAction>(
      initialState: initialLocalState,
      reducer: { localState, localAction in
        self.send(fromLocalAction(localAction))
        if let newState = toLocalState(self.state) {
          localState = newState
        }
        return .none
      }
    )

    self.stateRelay
          .distinctUntilChanged({ old, new in
              isEqual(old, new)
          })
      .subscribe(onNext: { [weak localStore] newValue in
        if let newState = toLocalState(newValue) {
          localStore?.state = newState
        }
      }).disposed(by: localStore.disposeBag)
    return localStore
  }

  /// Scopes the store to one that exposes local state.
  ///
  /// - Parameter toLocalState: A function that transforms `State` into `LocalState`.
  /// - Returns: A new store with its domain (state and action) transformed.
  public func scope<LocalState>(
    state toLocalState: @escaping (State) -> LocalState
  ) -> Store<LocalState, Action> {
    scope(state: toLocalState, action: { $0 })
  }

  /// Scopes the store to a publisher of stores of more local state and local actions.
  ///
  /// - Parameters:
  ///   - toLocalState: A function that transforms a publisher of `State` into a publisher of
  ///     `LocalState`.
  ///   - fromLocalAction: A function that transforms `LocalAction` into `Action`.
  /// - Returns: A publisher of stores with its domain (state and action) transformed.
  public func observableScope<O: ObservableType, LocalState, LocalAction>(
    state toLocalState: @escaping (Observable<State>) -> O,
    action fromLocalAction: @escaping (LocalAction) -> Action
  ) -> Observable<Store<LocalState, LocalAction>>
  where O.Element == LocalState {

    func extractLocalState(_ state: State) -> LocalState? {
      var localState: LocalState?
      _ = toLocalState(Observable.just(state))
          .subscribe(onNext: { localState = $0 })
      return localState
    }

      return toLocalState(stateRelay.asObservable())
      .map { localState in
        let localStore = Store<LocalState, LocalAction>(
          initialState: localState,
          reducer: { (localState, localAction) -> Effect<LocalAction, Never> in
            self.send(fromLocalAction(localAction))
            localState = extractLocalState(self.state) ?? localState
            return .none
          })

        self.stateRelay
          .subscribe(onNext: { [weak localStore] state in
            guard let localStore = localStore else { return }
            localStore.state = extractLocalState(state) ?? localStore.state
          })
          .disposed(by: localStore.disposeBag)
        return localStore
      }.asObservable()
  }

  /// Scopes the store to a publisher of stores of more local state and local actions.
  ///
  /// - Parameter toLocalState: A function that transforms a publisher of `State` into a publisher
  ///   of `LocalState`.
  /// - Returns: A publisher of stores with its domain (state and action)
  ///   transformed.
  public func observableScope<O: ObservableType, LocalState>(
    state toLocalState: @escaping (Observable<State>) -> O
  ) -> Observable<Store<LocalState, Action>>
    where O.Element == LocalState {
      self.observableScope(state: toLocalState, action: { $0 })
  }

  /// Sends an action to the store.
  ///
  /// `Store` is not thread safe and you should only send actions to it from the main thread.
  /// If you are wanting to send actions on background threads due to the fact that the reducer
  /// is performing computationally expensive work, then a better way to handle this is to wrap
  /// that work in an `Effect` that is performed on a background thread so that the result can
  /// be fed back into the store.
  ///
  /// - Parameter action: An action.
  public func send(_ action: Action) {
    if !self.isSending {
      self.synchronousActionsToSend.append(action)
    } else {
      self.bufferedActions.append(action)
      return
    }

    while !self.synchronousActionsToSend.isEmpty || !self.bufferedActions.isEmpty {
      let action =
        !self.synchronousActionsToSend.isEmpty
        ? self.synchronousActionsToSend.removeFirst()
        : self.bufferedActions.removeFirst()

      self.isSending = true
      let effect = self.reducer(&self.state, action)
      self.isSending = false

      var didComplete = false
      let uuid = UUID()

      var isProcessingEffects = true
      let effectDisposeBag = DisposeBag()
      effect.subscribe(
        onNext: { [weak self] action in
          if isProcessingEffects {
            self?.synchronousActionsToSend.append(action)
          } else {
            self?.send(action)
          }
        },
        onCompletion: { [weak self] _ in
          didComplete = true
          self?.effectDisposeBags[uuid] = nil
        }
      ).disposed(by: effectDisposeBag)
      isProcessingEffects = false

      if !didComplete {
        self.effectDisposeBags[uuid] = effectDisposeBag
      }
    }
  }

  /// Returns a "stateless" store by erasing state to `Void`.
  public var stateless: Store<Void, Action> {
    self.scope(state: { _ in () })
  }

  /// Returns an "actionless" store by erasing action to `Never`.
  public var actionless: Store<State, Never> {
    func absurd<A>(_ never: Never) -> A {}
    return self.scope(state: { $0 }, action: absurd)
  }

  private init(
    initialState: State,
    reducer: @escaping (inout State, Action) -> Effect<Action, Never>
  ) {
    self.reducer = reducer
    self.stateRelay = BehaviorRelay(value: initialState)
  }
}

extension Store {
  /// Scopes the store to one that exposes local state and actions.
  ///
  /// This can be useful for deriving new stores to hand to child views in an application. For
  /// example:
  ///
  ///     // Application state made from local states.
  ///     struct AppState { var login: LoginState, ... }
  ///     struct AppAction { case login(LoginAction), ... }
  ///
  ///     // A store that runs the entire application.
  ///     let store = Store(initialState: AppState(), reducer: appReducer, environment: ())
  ///     let store = Store(
  ///       initialState: AppState(),
  ///       reducer: appReducer,
  ///       environment: AppEnvironment()
  ///     )
  ///
  ///     // Construct a login view by scoping the store to one that works with only login domain.
  ///     let loginView = LoginView(
  ///     LoginView(
  ///       store: store.scope(
  ///         state: { $0.login },
  ///         action: { AppAction.login($0) }
  ///       )
  ///     )
  ///
  /// Scoping in this fashion allows you to better modularize your application. In this case,
  /// `LoginView` could be extracted to a module that has no access to `AppState` or `AppAction`.
  ///
  /// Scoping also gives a view the opportunity to focus on just the state and actions it cares
  /// about, even if its feature domain is larger.
  ///
  /// For example, the above login domain could model a two screen login flow: a login form followed
  /// by a two-factor authentication screen. The second screen's domain might be nested in the
  /// first:
  ///
  ///     struct LoginState: Equatable {
  ///       var email = ""
  ///       var password = ""
  ///       var twoFactorAuth: TwoFactorAuthState?
  ///     }
  ///
  ///     enum LoginAction: Equatable {
  ///       case emailChanged(String)
  ///       case loginButtonTapped
  ///       case loginResponse(Result<TwoFactorAuthState, LoginError>)
  ///       case passwordChanged(String)
  ///       case twoFactorAuth(TwoFactorAuthAction)
  ///     }
  ///
  /// The login view holds onto a store of this domain:
  ///
  ///     struct LoginView: View {
  ///       let store: Store<LoginState, LoginAction>
  ///
  ///       var body: some View { ... }
  ///     }
  ///
  /// If its body were to use a view store of the same domain, this would introduce a number of
  /// problems:
  ///
  /// * The login view would be able to read from `twoFactorAuth` state. This state is only intended
  ///   to be read from the two-factor auth screen.
  ///
  /// * Even worse, changes to `twoFactorAuth` state would now cause SwiftUI to recompute
  ///   `LoginView`'s body unnecessarily.
  ///
  /// * The login view would be able to send `twoFactorAuth` actions. These actions are only
  ///   intended to be sent from the two-factor auth screen (and reducer).
  ///
  /// * The login view would be able to send non user-facing login actions, like `loginResponse`.
  ///   These actions are only intended to be used in the login reducer to feed the results of
  ///   effects back into the store.
  ///
  /// To avoid these issues, one can introduce a view-specific domain that slices off the subset of
  /// state and actions that a view cares about:
  ///
  ///     extension LoginView {
  ///       struct State: Equatable {
  ///         var email: String
  ///         var password: String
  ///       }
  ///
  ///       enum Action: Equatable {
  ///         case emailChanged(String)
  ///         case loginButtonTapped
  ///         case passwordChanged(String)
  ///       }
  ///     }
  ///
  /// One can also introduce a couple helpers that transform feature state into view state and
  /// transform view actions into feature actions.
  ///
  ///     extension LoginState {
  ///       var view: LoginView.State {
  ///         .init(email: self.email, password: self.password)
  ///       }
  ///     }
  ///
  ///     extension LoginView.Action {
  ///       var feature: LoginAction {
  ///         switch self {
  ///         case let .emailChanged(email)
  ///           return .emailChanged(email)
  ///         case .loginButtonTapped:
  ///           return .loginButtonTapped
  ///         case let .passwordChanged(password)
  ///           return .passwordChanged(password)
  ///         }
  ///       }
  ///     }
  ///
  /// With these helpers defined, `LoginView` can now scope its store's feature domain into its view
  /// domain:
  ///
  ///     var body: some View {
  ///       WithViewStore(
  ///         self.store.scope(state: { $0.view }, action: { $0.feature })
  ///       ) { viewStore in
  ///         ...
  ///       }
  ///     }
  ///
  /// This view store is now incapable of reading any state but view state (and will not recompute
  /// when non-view state changes), and is incapable of sending any actions but view actions.
  ///
  public func scope<LocalState, LocalAction>(
    state toLocalState: @escaping (State) -> LocalState,
    action fromLocalAction: @escaping (LocalAction) -> Action
  ) -> Store<LocalState, LocalAction> {
    scope(initialLocalState: toLocalState(state),
          state: toLocalState,
          action: fromLocalAction)
  }
}

extension Store: TCAIdentifiable where State: TCAIdentifiable {
  public var id: State.ID {
    ViewStore(self, removeDuplicates: {_, _ in false }).id
  }
}

extension Store {
  /// Allows observation of State from the store.
  /// New event is not fired for duplicated state (using `isDuplicate` to detect duplicates)
  /// Useful for UIKit controllers or coordinators as we avoid the boilerplate of having both ViewStore and Store to manage.
  /// - Parameter isDuplicate: Returns true if both states should be considered equal
  /// - Returns: A driver on the state
  public func driver(removeDuplicates isDuplicate: @escaping (State, State) -> Bool) -> StoreDriver<State> {
    .init(stateRelay.distinctUntilChanged(isDuplicate))
  }
}

extension Store where State: Equatable {
  /// Allows observation of State from the store.
  /// New event is not fired for duplicated state.
  /// Useful for UIKit controllers or coordinators as we avoid the boilerplate of having both ViewStore and Store to manage.
  public var driver: StoreDriver<State> {
    .init(stateRelay.distinctUntilChanged())
  }
}


/// The goal of this structure is to be able to perform a subscript as a quick way of mapping & avoid duplicates. As it comes from the ViewStore, the Driver trait is the more appropriate
@dynamicMemberLookup
public struct StoreDriver<State>: SharedSequenceConvertibleType {

  public typealias Element = State

  private let upstream: Observable<State>

  public func subscribe<Observer: ObserverType>(_ observer: Observer) -> Disposable where Observer.Element == State {
    upstream.subscribe(observer)
  }

  public init<O: ObservableType>(_ observable: O) where O.Element == State {
    self.upstream = observable.asObservable()
  }

  public func asSharedSequence() -> Driver<State> {
    upstream
      .do(onError: { error in assertionFailure("An error occurred in the stream handled by a store driver : \(error)") })
      .asDriver(onErrorDriveWith: .empty())
  }

  /// Returns the resulting driver of a given key path.
  public subscript<LocalState>(
    dynamicMember keyPath: KeyPath<State, LocalState>
  ) -> StoreDriver<LocalState>
  where LocalState: Equatable {
    .init(upstream.map { $0[keyPath: keyPath] }
      .distinctUntilChanged())
  }
}

extension Store {
  /// Delays state updates that match the given condition
  /// - Parameters:
  ///   - condition: A condition on the state. Any state that matches it will be delayed
  ///   - delay: Delay of states matching condition
  ///   - scheduler: Scheduler on which delaying occurs (`MainScheduler` by default)
  /// - Returns: A new store with some states delayed
  public func delayed(
    when condition: @escaping (State) -> Bool,
    by delay: DispatchTimeInterval,
    using scheduler: SchedulerType = MainScheduler()
  ) -> Store {
    let delayedStore = Store(
      initialState: state,
      reducer: { state, action -> Effect in
        self.send(action)
        if !condition(self.state) {
          state = self.state
        }
        return .none
      })

    stateRelay
      .flatMapLatest { newState -> Observable<State> in
        if condition(newState) {
          return .just(newState).delay(delay, scheduler: scheduler)
        } else {
          return .just(newState)
        }
      }
      .subscribe(onNext: { [weak delayedStore] newState in
        delayedStore?.state = newState
      }).disposed(by: delayedStore.disposeBag)
    
    return delayedStore
  }
}

extension Equatable {
  func equals(_ any: some Any) -> Bool { self == any as? Self }
}

func isEqual(_ lhs: some Any, _ rhs: some Any) -> Bool {
  (lhs as? any Equatable)?.equals(rhs) ?? false
}
