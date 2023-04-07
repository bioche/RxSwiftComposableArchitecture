import Foundation
import RxSwift
#if canImport(Combine)
import Combine

@available(iOS 13, macOS 10.15, tvOS 13.0, watchOS 6.0, *)
extension Publisher {
  /// Returns an Observable<Output> representing the underlying
  /// Publisher. Upon subscription, the Publisher's sink pushes
  /// events into the Observable. Upon disposing of the subscription,
  /// the sink is cancelled.
  ///
  /// - returns: Observable<Output>
  public func asObservable() -> Observable<Output> {
    Observable<Output>.create { observer in
      let cancellable = self.sink(
        receiveCompletion: { completion in
          switch completion {
          case .finished:
            observer.onCompleted()
          case .failure(let error):
            observer.onError(error)
          }
      },
        receiveValue: { value in
          observer.onNext(value)
      })
      
      return Disposables.create { cancellable.cancel() }
    }
  }
}
#endif
