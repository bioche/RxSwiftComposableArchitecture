import RxSwift
import RxTest
import RxComposableArchitecture
import XCTest

final class TimerTests: XCTestCase {
  var disposeBag = DisposeBag()

  func testTimer() {
    let scheduler = RxTest.TestScheduler.defaultTestScheduler()

    var count = 0

    Effect<Int, Never>.timer(id: 1, every: .seconds(1), on: scheduler).debug("ohanas")
      .subscribe(onNext: { _ in count += 1 })
      .disposed(by: disposeBag)

    scheduler.advance(by: 1)
    XCTAssertEqual(count, 1)

    scheduler.advance(by: 1)
    XCTAssertEqual(count, 2)

    scheduler.advance(by: 1)
    XCTAssertEqual(count, 3)

    scheduler.advance(by: 3)
    XCTAssertEqual(count, 6)
  }

  func testInterleavingTimer() {
    let scheduler = RxTest.TestScheduler.defaultTestScheduler()

    var count2 = 0
    var count3 = 0

    Effect<Int, Error>.merge(
      Effect<Int, Never>.timer(id: 1, every: .seconds(2), on: scheduler)
        .do(onNext: { _ in count2 += 1 })
        .eraseToEffect(),
      Effect<Int, Never>.timer(id: 2, every: .seconds(3), on: scheduler)
        .do(onNext: { _ in count3 += 1 })
        .eraseToEffect()
    )
      .subscribe(onNext: { _ in })
      .disposed(by: disposeBag)

    scheduler.advance(by: 1)
    XCTAssertEqual(count2, 0)
    XCTAssertEqual(count3, 0)
    scheduler.advance(by: 1)
    XCTAssertEqual(count2, 1)
    XCTAssertEqual(count3, 0)
    scheduler.advance(by: 1)
    XCTAssertEqual(count2, 1)
    XCTAssertEqual(count3, 1)
    scheduler.advance(by: 1)
    XCTAssertEqual(count2, 2)
    XCTAssertEqual(count3, 1)
  }

  func testTimerCancellation() {
    let scheduler = RxTest.TestScheduler.defaultTestScheduler()

    var count2 = 0
    var count3 = 0

    struct CancelToken: Hashable {}

    Effect<Int, Error>.merge(
      Effect<Int, Never>.timer(id: CancelToken(), every: .seconds(2), on: scheduler)
        .do(onNext: { _ in count2 += 1 })
        .eraseToEffect(),
      Effect<Int, Never>.timer(id: CancelToken(), every: .seconds(3), on: scheduler)
        .do(onNext: { _ in count3 += 1 })
        .eraseToEffect(),
      Observable.just(())
        .delay(.seconds(31), scheduler: scheduler)
        .flatMap { Effect<Int, Error>.cancel(id: CancelToken()) }
        .eraseToEffect()
    )
      .subscribe(onNext: { _ in })
      .disposed(by: disposeBag)

    scheduler.advance(by: 1)

    XCTAssertEqual(count2, 0)
    XCTAssertEqual(count3, 0)

    scheduler.advance(by: 1)

    XCTAssertEqual(count2, 1)
    XCTAssertEqual(count3, 0)

    scheduler.advance(by: 1)

    XCTAssertEqual(count2, 1)
    XCTAssertEqual(count3, 1)

    scheduler.advance(by: 1)

    XCTAssertEqual(count2, 2)
    XCTAssertEqual(count3, 1)

    scheduler.run()

    XCTAssertEqual(count2, 15)
    XCTAssertEqual(count3, 10)
  }

  func testTimerCompletion() {
    let scheduler = RxTest.TestScheduler.defaultTestScheduler()

    var count = 0

    Effect<Int, Never>.timer(id: 1, every: .seconds(1), on: scheduler)
      .take(3)
      .subscribe(onNext: { _ in count += 1 })
      .disposed(by: disposeBag)

    scheduler.advance(by: 1)
    XCTAssertEqual(count, 1)

    scheduler.advance(by: 1)
    XCTAssertEqual(count, 2)

    scheduler.advance(by: 1)
    XCTAssertEqual(count, 3)

    scheduler.run()
    XCTAssertEqual(count, 3)
  }
}
