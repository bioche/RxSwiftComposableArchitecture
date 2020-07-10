//
//  ComposableArchitecture+utils.swift
//  pureairconnect
//
//  Created by sebastien on 09/07/2020.
//  Copyright © 2020 Groupe SEB. All rights reserved.
//

import Foundation
import RxSwift
import ComposableArchitecture

extension Store {
    /// Subscribes to updates when a store containing optional state goes from `nil` to non-`nil` or
    /// non-`nil` to `nil`.
    ///
    /// This is useful for handling navigation in UIKit. The state for a screen that you want to
    /// navigate to can be held as an optional value in the parent, and when that value switches
    /// from `nil` to non-`nil` you want to trigger a navigation and hand the detail view a `Store`
    /// whose domain has been scoped to just that feature:
    ///
    ///     class MasterViewController: UIViewController {
    ///       let store: Store<MasterState, MasterAction>
    ///       var cancellables: Set<AnyCancellable> = []
    ///       ...
    ///       func viewDidLoad() {
    ///         ...
    ///         self.store
    ///           .scope(state: \.optionalDetail, action: MasterAction.detail)
    ///           .ifLet(
    ///             then: { [weak self] detailStore in
    ///               self?.navigationController?.pushViewController(
    ///                 DetailViewController(store: detailStore),
    ///                 animated: true
    ///               )
    ///             },
    ///             else: { [weak self] in
    ///               guard let self = self else { return }
    ///               self.navigationController?.popToViewController(self, animated: true)
    ///             }
    ///           )
    ///           .store(in: &self.cancellables)
    ///       }
    ///     }
    ///
    /// - Parameters:
    ///   - unwrap: A function that is called with a store of non-optional state whenever the store's
    ///     optional state goes from `nil` to non-`nil`.
    ///   - else: A function that is called whenever the store's optional state goes from non-`nil` to
    ///     `nil`.
    /// - Returns: A Disposable associated with the underlying subscription.
    public func ifLet<Wrapped>(
      then unwrap: @escaping (Store<Wrapped, Action>) -> Void,
      else: @escaping () -> Void
    ) -> Disposable where State == Wrapped? {
        self.scope { (state: Observable<Wrapped?>) in
            state
                .distinctUntilChanged { ($0 != nil) == ($1 != nil) }
                .do(onNext: {
                    if $0 == nil {
                        `else`()
                    }
                })
                .compactMap { $0 }
        }.subscribe(onNext: unwrap)
    }
    
    /// An overload of `ifLet(then:else:)` for the times that you do not want to handle the `else`
    /// case.
    ///
    /// - Parameter unwrap: A function that is called with a store of non-optional state whenever the
    ///   store's optional state goes from `nil` to non-`nil`.
    /// - Returns: A Disposable associated with the underlying subscription.
    public func ifLet<Wrapped>(
      then unwrap: @escaping (Store<Wrapped, Action>) -> Void
    ) -> Disposable where State == Wrapped? {
      self.ifLet(then: unwrap, else: {})
    }
}


extension Store where State: Equatable {
    
    /// Subscribes to updates when a store containing a state goes change from or to the given Condition or
    /// condition = true to condition = false.
    /// - Parameters:
    ///   - condition: the condition that you want to observe
    ///   - then: closure that is executed when the condition is true
    ///   - else: closure that is executed when the condition is false
    /// - Returns: A Disposable associated with the underlying subscription.
    public func `if`(
        condition: @escaping (State) -> Bool,
        then: @escaping (Store<State, Action>) -> Void,
        else: @escaping () -> Void = {}
    ) -> Disposable{
        self.scope { (observableState: Observable<State>) in
            observableState
                .distinctUntilChanged()
                .do(onNext: {
                    print("test if new message : \($0)")
                    if !condition($0) {
                        `else`()
                    }
                })
                .filter(condition)
        }.subscribe(onNext: then)
    }
}
