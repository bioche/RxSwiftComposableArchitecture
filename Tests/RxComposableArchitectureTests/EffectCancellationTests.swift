import RxSwift
import RxTest
import XCTest

@testable import RxComposableArchitecture

final class EffectCancellationTests: XCTestCase {
  struct CancelToken: Hashable {}
  var disposeBag = DisposeBag()

  override func tearDown() {
    super.tearDown()
    self.disposeBag = DisposeBag()
  }

  func testCancellation() {
    var values: [Int] = []

    let subject = PublishSubject<Int>()
  
    let effect = Effect<Int, Error>(subject)
      .cancellable(id: CancelToken())

    effect
      .subscribe(onNext: { values.append($0) })
      .disposed(by: disposeBag)

    XCTAssertEqual(values, [])
    subject.onNext(1)
    XCTAssertEqual(values, [1])
    subject.onNext(2)
    XCTAssertEqual(values, [1, 2])

    Effect<Never, Never>.cancel(id: CancelToken())
      .subscribe(onNext: { _ in })
      .disposed(by: disposeBag)

    subject.onNext(3)
    XCTAssertEqual(values, [1, 2])
  }

  func testCancelInFlight() {
    var values: [Int] = []

    let subject = PublishSubject<Int>()
    Effect<Int, Never>(subject)
      .cancellable(id: CancelToken(), cancelInFlight: true)
      .subscribe(onNext: { values.append($0) })
      .disposed(by: disposeBag)

    XCTAssertEqual(values, [])
    subject.onNext(1)
    XCTAssertEqual(values, [1])
    subject.onNext(2)
    XCTAssertEqual(values, [1, 2])

    Effect<Int, Never>(subject)
      .cancellable(id: CancelToken(), cancelInFlight: true)
      .subscribe(onNext: { values.append($0) })
      .disposed(by: disposeBag)

    subject.onNext(3)
    XCTAssertEqual(values, [1, 2, 3])
    subject.onNext(4)
    XCTAssertEqual(values, [1, 2, 3, 4])
  }

  func testCancellationAfterDelay() {
    var value: Int?

    Observable.just(1)
      .delay(.milliseconds(150), scheduler: MainScheduler.instance)
      .eraseToEffect(failureType: Never.self)
      .cancellable(id: CancelToken())
      .subscribe(onNext: { value = $0 })
      .disposed(by: disposeBag)

    XCTAssertEqual(value, nil)

    DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
      Effect<Never, Never>.cancel(id: CancelToken())
        .subscribe(onNext: { _ in })
        .disposed(by: self.disposeBag)
    }

    _ = XCTWaiter.wait(for: [self.expectation(description: "")], timeout: 0.3)

    XCTAssertEqual(value, nil)
  }

  func testCancellationAfterDelay_WithTestScheduler() {
    let scheduler = RxTest.TestScheduler.defaultTestScheduler()
    var value: Int?

    Observable.just(1)
      .delay(.seconds(2), scheduler: scheduler)
      .eraseToEffect(failureType: Never.self)
      .cancellable(id: CancelToken())
      .subscribe(onNext: { value = $0 })
      .disposed(by: disposeBag)

    XCTAssertEqual(value, nil)

    scheduler.advance(by: 1)
    Effect<Never, Never>.cancel(id: CancelToken())
      .subscribe(onNext: { _ in })
      .disposed(by: self.disposeBag)

    scheduler.run()

    XCTAssertEqual(value, nil)
  }

  func testCancellablesCleanUp_OnComplete() {
    var result: Int?
    Observable.just(1)
      .eraseToEffect(failureType: Never.self)
      .cancellable(id: 1)
      .subscribe(onNext: { result = $0 })
      .disposed(by: self.disposeBag)

    XCTAssertEqual(result, 1)
    XCTAssertEqual([:], cancellationDisposables)
  }

  func testCancellablesCleanUp_OnCancel() {
    let scheduler = RxTest.TestScheduler.defaultTestScheduler()
    Observable.just(1)
      .delay(.seconds(1), scheduler: scheduler)
      .eraseToEffect(failureType: Never.self)
      .cancellable(id: 1)
      .subscribe(onNext: { _ in })
      .disposed(by: self.disposeBag)

    Effect<Int, Never>.cancel(id: 1)
      .subscribe(onNext: { _ in })
      .disposed(by: self.disposeBag)

    XCTAssertEqual([:], cancellationDisposables)
  }

  func testDoubleCancellation() {
    var values: [Int] = []

    let subject = PublishSubject<Int>()
    let effect = Effect<Int, Never>(subject)
      .cancellable(id: CancelToken())
      .cancellable(id: CancelToken())

    effect
      .subscribe(onNext: { values.append($0) })
      .disposed(by: disposeBag)

    XCTAssertEqual(values, [])
    subject.onNext(1)
    XCTAssertEqual(values, [1])

    Effect<Never, Never>.cancel(id: CancelToken())
      .subscribe(onNext: { _ in })
      .disposed(by: disposeBag)

    subject.onNext(2)
    XCTAssertEqual(values, [1])
  }

  func testCompleteBeforeCancellation() {
    var values: [Int] = []

    let subject = PublishSubject<Int>()
    let effect = Effect<Int, Never>(subject)
      .cancellable(id: CancelToken())

    effect
      .subscribe(onNext: { values.append($0) })
      .disposed(by: disposeBag)

    subject.onNext(1)
    XCTAssertEqual(values, [1])

    subject.onCompleted()
    XCTAssertEqual(values, [1])

    Effect<Never, Never>.cancel(id: CancelToken())
      .subscribe(onNext: { _ in })
      .disposed(by: disposeBag)

    XCTAssertEqual(values, [1])
  }

  func testConcurrentCancels() {
    let queues = [
      ConcurrentDispatchQueueScheduler(queue: DispatchQueue.main),
      ConcurrentDispatchQueueScheduler(queue: DispatchQueue.global(qos: .background)),
      ConcurrentDispatchQueueScheduler(queue: DispatchQueue.global(qos: .default)),
      ConcurrentDispatchQueueScheduler(queue: DispatchQueue.global(qos: .unspecified)),
      ConcurrentDispatchQueueScheduler(queue: DispatchQueue.global(qos: .userInitiated)),
      ConcurrentDispatchQueueScheduler(queue: DispatchQueue.global(qos: .userInteractive)),
      ConcurrentDispatchQueueScheduler(queue: DispatchQueue.global(qos: .utility))
    ]

    let effect = Effect.merge(
      (1...1_000).map { idx -> Effect<Int, Never> in
        let id = idx % 10

        return Effect.merge(
          Observable.just(idx)
            .delay(
              .milliseconds(Int.random(in: 1...100)), scheduler: queues.randomElement()!
            )
            .eraseToEffect()
            .cancellable(id: id),

          Observable.just(())
            .delay(
              .milliseconds(Int.random(in: 1...100)), scheduler: queues.randomElement()!
            )
            .flatMap { Effect<Int, Never>.cancel(id: id) }
            .eraseToEffect()
        )
      }
    )

    let expectation = self.expectation(description: "wait")
    effect
      .subscribe(onCompleted: { expectation.fulfill() })
      .disposed(by: disposeBag)
    self.wait(for: [expectation], timeout: 999)

    XCTAssertTrue(cancellationDisposables.isEmpty)
  }
  
    func testConcurrentCancelsRepeated() {
      (1...10).forEach { idx in
        print("loop \(idx)")
        testConcurrentCancels()
      }
    }

  func testNestedCancels() {
    var effect = Observable<Void>.never()
      .eraseToEffect(failureType: Never.self)
      .cancellable(id: 1)

    for _ in 1 ... .random(in: 1...1_000) {
      effect = effect.cancellable(id: 1)
    }

    effect
      .subscribe(onNext: { _ in })
      .disposed(by: disposeBag)

    disposeBag = DisposeBag()

    XCTAssertEqual([:], cancellationDisposables)
  }

  func testSharedId() {
    let scheduler = RxTest.TestScheduler.defaultTestScheduler()

    let effect1 = Observable.just(1)
      .delay(.seconds(1), scheduler: scheduler)
      .eraseToEffect(failureType: Never.self)
      .cancellable(id: "id")

    let effect2 = Observable.just(2)
      .delay(.seconds(2), scheduler: scheduler)
      .eraseToEffect(failureType: Never.self)
      .cancellable(id: "id")

    var expectedOutput: [Int] = []
    effect1
      .subscribe(onNext: { expectedOutput.append($0) })
      .disposed(by: disposeBag)
    effect2
      .subscribe(onNext: { expectedOutput.append($0) })
      .disposed(by: disposeBag)

    XCTAssertEqual(expectedOutput, [])
    scheduler.advance(by: 1)
    XCTAssertEqual(expectedOutput, [1])
    scheduler.advance(by: 1)
    XCTAssertEqual(expectedOutput, [1, 2])
  }

  func testImmediateCancellation() {
    let scheduler = RxTest.TestScheduler.defaultTestScheduler()

    var expectedOutput: [Int] = []
    
    let disposable = Observable.deferred { .just(1) }
      .delay(.seconds(1), scheduler: scheduler)
      .eraseToEffect(failureType: Never.self)
      .cancellable(id: "id")
      .subscribe(onNext: { expectedOutput.append($0) })
    // dispose of it immediately
    disposable.dispose()

    XCTAssertEqual(expectedOutput, [])
    scheduler.advance(by: 1)
    XCTAssertEqual(expectedOutput, [])
  }
}
