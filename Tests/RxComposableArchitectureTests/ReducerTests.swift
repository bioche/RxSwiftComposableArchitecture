import RxSwift
import RxTest
import RxComposableArchitecture
import XCTest
import os.signpost

final class ReducerTests: XCTestCase {
  var disposeBag = DisposeBag()

  func testCallableAsFunction() {
    let reducer = Reducer<Int, Void, Void> { state, _, _ in
      state += 1
      return .none
    }

    var state = 0
    _ = reducer.run(&state, (), ())
    XCTAssertEqual(state, 1)
  }

  func testCombine_EffectsAreMerged() {
    typealias Scheduler = RxTest.TestScheduler
    enum Action: Equatable {
      case increment
    }

    var fastValue: Int?
    let fastReducer = Reducer<Int, Action, Scheduler> { state, _, scheduler in
      state += 1
      return Effect<Action, Never>.fireAndForget { fastValue = 42 }
        .delay(.seconds(1), scheduler: scheduler)
        .eraseToEffect()
    }

    var slowValue: Int?
    let slowReducer = Reducer<Int, Action, Scheduler> { state, _, scheduler in
      state += 1
      return Effect<Action, Never>.fireAndForget { slowValue = 1729 }
        .delay(.seconds(1), scheduler: scheduler)
        .eraseToEffect()
    }

    let scheduler = Scheduler.defaultTestScheduler()
    let store = TestStore(
      initialState: 0,
      reducer: .combine(fastReducer, slowReducer),
      environment: scheduler
    )

    store.send(.increment) {
      $0 = 2
    }
    // Waiting a second causes the fast effect to fire.
    scheduler.advance(by: 1)
    XCTAssertEqual(fastValue, 42)
    // Waiting one more second causes the slow effect to fire. This proves that the effects
    // are merged together, as opposed to concatenated.
    scheduler.advance(by: 1)
    XCTAssertEqual(slowValue, 1729)
  }

  func testCombine() {
    enum Action: Equatable {
      case increment
    }

    var childEffectExecuted = false
    let childReducer = Reducer<Int, Action, Void> { state, _, _ in
      state += 1
      return Effect.fireAndForget { childEffectExecuted = true }
    }

    var mainEffectExecuted = false
    let mainReducer = Reducer<Int, Action, Void> { state, _, _ in
      state += 1
      return Effect.fireAndForget { mainEffectExecuted = true }
    }
    .combined(with: childReducer)

    let store = TestStore(
      initialState: 0,
      reducer: mainReducer,
      environment: ()
    )

    store.send(.increment) {
      $0 = 2
    }

    XCTAssertTrue(childEffectExecuted)
    XCTAssertTrue(mainEffectExecuted)
  }

  func testDebug() {
    enum Action: Equatable { case incr, noop }
    struct State: Equatable { var count = 0 }

    var logs: [String] = []
    let logsExpectation = self.expectation(description: "logs")
    logsExpectation.expectedFulfillmentCount = 2

    let reducer = Reducer<State, Action, Void> { state, action, _ in
      switch action {
      case .incr:
        state.count += 1
        return .none
      case .noop:
        return .none
      }
    }
    .debug("[prefix]") { _ in
      DebugEnvironment(
        printer: {
          logs.append($0)
          logsExpectation.fulfill()
        }
      )
    }

    let store = TestStore(
      initialState: State(),
      reducer: reducer,
      environment: ()
    )
    store.send(.incr) { $0.count = 1 }
    store.send(.noop)

    self.wait(for: [logsExpectation], timeout: 2)

    XCTAssertEqual(
      logs,
      [
        #"""
        [prefix]: received action:
          Action.incr
          State(
        −   count: 0
        +   count: 1
          )

        """#,
        #"""
        [prefix]: received action:
          Action.noop
          (No state changes)

        """#,
      ]
    )
  }

  func testDebug_ActionFormat_OnlyLabels() {
    enum Action: Equatable { case incr(Bool) }
    struct State: Equatable { var count = 0 }

    var logs: [String] = []
    let logsExpectation = self.expectation(description: "logs")

    let reducer = Reducer<State, Action, Void> { state, action, _ in
      switch action {
      case let .incr(bool):
        state.count += bool ? 1 : 0
        return .none
      }
    }
    .debug("[prefix]", actionFormat: .labelsOnly) { _ in
      DebugEnvironment(
        printer: {
          logs.append($0)
          logsExpectation.fulfill()
        }
      )
    }

    let viewStore = ViewStore(
      Store(
        initialState: State(),
        reducer: reducer,
        environment: ()
      )
    )
    viewStore.send(.incr(true))

    self.wait(for: [logsExpectation], timeout: 2)

    XCTAssertEqual(
      logs,
      [
        #"""
        [prefix]: received action:
          Action.incr
          State(
        −   count: 0
        +   count: 1
          )

        """#
      ]
    )
  }

  @available(iOS 12, macOS 10.14, tvOS 12.0, watchOS 5.0, *)
  func testDefaultSignpost() {
    let reducer = Reducer<Int, Void, Void>.empty.signpost(log: .default)
    var n = 0
    let effect = reducer.run(&n, (), ())
    let expectation = self.expectation(description: "effect")
    effect
      .subscribe(onCompleted: { expectation.fulfill() })
      .disposed(by: disposeBag)
    self.wait(for: [expectation], timeout: 0.1)
  }

  @available(iOS 12, macOS 10.14, tvOS 12.0, watchOS 5.0, *)
  func testDisabledSignpost() {
    let reducer = Reducer<Int, Void, Void>.empty.signpost(log: .disabled)
    var n = 0
    let effect = reducer.run(&n, (), ())
    let expectation = self.expectation(description: "effect")
    effect
      .subscribe(onCompleted: { expectation.fulfill() })
      .disposed(by: disposeBag)
    self.wait(for: [expectation], timeout: 0.1)
  }
}
