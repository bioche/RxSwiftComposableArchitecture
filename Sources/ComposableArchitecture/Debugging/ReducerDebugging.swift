//import CasePaths
import Dispatch

/// Set this to true so that the debug on Reducer prints the results.
/// By default : true if framework is built on DEBUG & false otherwise
///
/// For SPM users you should not need this, for Carthage users you should condition this to the DEBUG flag on the client app :
/// ```
///   #if DEBUG
///   ComposableArchitecture.debuggingActivationFlag = true
///   #else
///   ComposableArchitecture.debuggingActivationFlag = false
///   #endif
/// ```
public var debuggingActivationFlag: Bool = {
  #if DEBUG
  return true
  #else
  return false
  #endif
}()

/// Determines how the string description of an action should be printed when using the `.debug()`
/// higher-order reducer.
public enum ActionFormat {
  /// Prints the action in a single line by only specifying the labels of the associated values:
  ///
  ///     Action.screenA(.row(index:, action: .textChanged(query:)))
  case labelsOnly
  /// Prints the action in a multiline, pretty-printed format, including all the labels of
  /// any associated values, as well as the data held in the associated values:
  ///
  ///     Action.screenA(
  ///       ScreenA.row(
  ///         index: 1,
  ///         action: RowAction.textChanged(
  ///           query: "Hi"
  ///         )
  ///       )
  ///     )
  case prettyPrint
}

extension Reducer {
  /// Prints debug messages describing all received actions and state mutations.
  ///
  /// Printing is only done if `debuggingActivationFlag` is set to true.
  /// (true by default in debug builds & false in release builds.
  ///
  /// - Parameters:
  ///   - prefix: A string with which to prefix all debug messages.
  ///   - toDebugEnvironment: A function that transforms an environment into a debug environment by
  ///     describing a print function and a queue to print from. Defaults to a function that ignores
  ///     the environment and returns a default `DebugEnvironment` that uses Swift's `print`
  ///     function and a background queue.
  /// - Returns: A reducer that prints debug messages for all received actions.
  public func debug(
    _ prefix: String = "",
    actionFormat: ActionFormat = .prettyPrint,
    environment toDebugEnvironment: @escaping (Environment) -> DebugEnvironment = { _ in
      DebugEnvironment()
    }
  ) -> Reducer {
    self.debug(
      prefix,
      state: { $0 },
      action: .self,
      actionFormat: actionFormat,
      environment: toDebugEnvironment
    )
  }

  /// Prints debug messages describing all received actions.
  ///
  /// Printing is only done if `debuggingActivationFlag` is set to true.
  /// (true by default in debug builds & false in release builds.
  ///
  /// - Parameters:
  ///   - prefix: A string with which to prefix all debug messages.
  ///   - toDebugEnvironment: A function that transforms an environment into a debug environment by
  ///     describing a print function and a queue to print from. Defaults to a function that ignores
  ///     the environment and returns a default `DebugEnvironment` that uses Swift's `print`
  ///     function and a background queue.
  /// - Returns: A reducer that prints debug messages for all received actions.
  public func debugActions(
    _ prefix: String = "",
    actionFormat: ActionFormat = .prettyPrint,
    environment toDebugEnvironment: @escaping (Environment) -> DebugEnvironment = { _ in
      DebugEnvironment()
    }
  ) -> Reducer {
    self.debug(
      prefix,
      state: { _ in () },
      action: .self,
      actionFormat: actionFormat,
      environment: toDebugEnvironment
    )
  }

  /// Prints debug messages describing all received local actions and local state mutations.
  ///
  /// Printing is only done if `debuggingActivationFlag` is set to true.
  /// (true by default in debug builds & false in release builds.
  ///
  /// - Parameters:
  ///   - prefix: A string with which to prefix all debug messages.
  ///   - toLocalState: A function that filters state to be printed.
  ///   - toLocalAction: A case path that filters actions that are printed.
  ///   - toDebugEnvironment: A function that transforms an environment into a debug environment by
  ///     describing a print function and a queue to print from. Defaults to a function that ignores
  ///     the environment and returns a default `DebugEnvironment` that uses Swift's `print`
  ///     function and a background queue.
  /// - Returns: A reducer that prints debug messages for all received actions.
  public func debug<LocalState, LocalAction>(
    _ prefix: String = "",
    state toLocalState: @escaping (State) -> LocalState,
    action toLocalAction: CasePath<Action, LocalAction>,
    actionFormat: ActionFormat = .prettyPrint,
    environment toDebugEnvironment: @escaping (Environment) -> DebugEnvironment = { _ in
      DebugEnvironment()
    }
  ) -> Reducer {
    if debuggingActivationFlag {
      return .init { state, action, environment in
        let previousState = toLocalState(state)
        let effects = self.run(&state, action, environment)
        guard let localAction = toLocalAction.extract(from: action) else { return effects }
        let nextState = toLocalState(state)
        let debugEnvironment = toDebugEnvironment(environment)
        return .merge(
          .fireAndForget {
            debugEnvironment.execute {
              let actionOutput =
                actionFormat == .prettyPrint
                ? debugOutput(localAction).indent(by: 2)
                : debugCaseOutput(localAction).indent(by: 2)
              let stateOutput =
                LocalState.self == Void.self
                ? ""
                : debugDiff(previousState, nextState).map { "\($0)\n" } ?? "  (No state changes)\n"
              debugEnvironment.printer(
                """
                \(prefix.isEmpty ? "" : "\(prefix): ")received action:
                \(actionOutput)
                \(stateOutput)
                """
              )
            }
          },
          effects
        )
      }
    } else {
      return self
    }
  }
}

/// An environment for debug-printing reducers.
public struct DebugEnvironment {
  
  /// Describe on which queue printing should be performed
  public enum Dispatching {
    /// Should be performed synchronously (on current queue)
    case sync
    /// Should be performed asynchronously (on specified queue)
    case async(DispatchQueue)
  }
  
  public var printer: (String) -> Void
  public var dispatching: Dispatching

  public init(
    printer: @escaping (String) -> Void = { print($0) },
    dispatching: Dispatching
  ) {
    self.printer = printer
    self.dispatching = dispatching
  }

  public init(
    printer: @escaping (String) -> Void = { print($0) }
  ) {
    self.init(printer: printer, dispatching: .async(_queue))
  }
  
  func execute(_ block: @escaping () -> Void) {
    switch dispatching {
    case .sync:
      block()
    case .async(let queue):
      queue.async {
        block()
      }
    }
  }
}

private let _queue = DispatchQueue(
  label: "co.pointfree.ComposableArchitecture.DebugEnvironment",
  qos: .background
)
