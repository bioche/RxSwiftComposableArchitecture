import RxSwift
import RxTest
import XCTest

@testable import RxComposableArchitecture

final class EffectThrottleTests: XCTestCase {
  var disposeBag = DisposeBag()
  let scheduler = RxTest.TestScheduler.defaultTestScheduler()

  func testThrottleLatest() {
    var values: [Int] = []
    var effectRuns = 0

    func runThrottledEffect(value: Int) {
      struct CancelToken: Hashable {}

      Observable.deferred { () -> Observable<Int> in
        effectRuns += 1
        return Observable.just(value)
      }
      .eraseToEffect(failureType: Never.self)
      .throttle(id: CancelToken(), for: .seconds(1), scheduler: scheduler, latest: true)
    .debug("test throttle")
      .subscribe(onNext: { values.append($0) })
      .disposed(by: disposeBag)
    }

    runThrottledEffect(value: 1)
    
    // A value emits right away.
    XCTAssertEqual(values, [1])
    
    runThrottledEffect(value: 2)
    
    // A second value is throttled.
    XCTAssertEqual(values, [1])

    scheduler.advance(by: 0.25)

    runThrottledEffect(value: 3)

    scheduler.advance(by: 0.25)

    runThrottledEffect(value: 4)

    scheduler.advance(by: 0.25)

    runThrottledEffect(value: 5)

    // A third value is throttled.
    XCTAssertEqual(values, [1])

    scheduler.advance(by: 0.25)

    // The latest value emits.
    XCTAssertEqual(values, [1, 5])
  }

  func testThrottleFirst() {
    var values: [Int] = []
    var effectRuns = 0

    func runThrottledEffect(value: Int) {
      struct CancelToken: Hashable {}

      Observable.deferred { () -> Observable<Int> in
        effectRuns += 1
        return Observable.just(value)
      }
      .eraseToEffect(failureType: Never.self)
      .throttle(
        id: CancelToken(), for: .seconds(1), scheduler: scheduler, latest: false
      )
      .subscribe(onNext: { values.append($0) })
      .disposed(by: disposeBag)
    }

    runThrottledEffect(value: 1)

    // A value emits right away.
    XCTAssertEqual(values, [1])

    runThrottledEffect(value: 2)

    // A second value is throttled.
    XCTAssertEqual(values, [1])

    scheduler.advance(by: 0.25)

    runThrottledEffect(value: 3)

    scheduler.advance(by: 0.25)

    runThrottledEffect(value: 4)

    scheduler.advance(by: 0.25)

    runThrottledEffect(value: 5)

    // A third value is throttled.
    XCTAssertEqual(values, [1])

    scheduler.advance(by: 0.25)

    // The first throttled value emits.
    XCTAssertEqual(values, [1, 2])
  }

  func testThrottleAfterInterval() {
    var values: [Int] = []
    var effectRuns = 0

    func runThrottledEffect(value: Int) {
      struct CancelToken: Hashable {}

      Observable.deferred { () -> Observable<Int> in
        effectRuns += 1
        return Observable.just(value)
      }
      .eraseToEffect(failureType: Never.self)
      .throttle(id: CancelToken(), for: .seconds(1), scheduler: scheduler, latest: true)
      .subscribe(onNext: { values.append($0) })
      .disposed(by: disposeBag)
    }

    runThrottledEffect(value: 1)

    // A value emits right away.
    XCTAssertEqual(values, [1])

    scheduler.advance(by: 2)

    runThrottledEffect(value: 2)
    
    // A second value is emitted right away.
    XCTAssertEqual(values, [1, 2])
  }
}
