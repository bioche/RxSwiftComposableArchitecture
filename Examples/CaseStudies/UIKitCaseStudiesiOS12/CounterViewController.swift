import UIKit
import SwiftUI
import ComposableArchitecture
import RxSwift

struct CounterState: Equatable {
  var count = 0
}

enum CounterAction: Equatable {
  case decrementButtonTapped
  case incrementButtonTapped
}

struct CounterEnvironment {}

let counterReducer = Reducer<CounterState, CounterAction, CounterEnvironment> { state, action, _ in
  switch action {
  case .decrementButtonTapped:
    state.count -= 1
    return .none
  case .incrementButtonTapped:
    state.count += 1
    return .none
  }
}

class CounterViewController: UIViewController {
  
  let viewStore: ViewStore<CounterState, CounterAction>
  let disposeBag = DisposeBag()
  
  init(store: Store<CounterState, CounterAction>) {
    self.viewStore = ViewStore(store)
    super.init(nibName: nil, bundle: nil)
  }

  required init?(coder: NSCoder) {
    fatalError("init(coder:) has not been implemented")
  }
  
  override func viewDidLoad() {
    super.viewDidLoad()
    
    self.view.backgroundColor = .white

    let decrementButton = UIButton(type: .system)
    decrementButton.addTarget(self, action: #selector(decrementButtonTapped), for: .touchUpInside)
    decrementButton.setTitle("−", for: .normal)

    let countLabel = UILabel()
    countLabel.font = .monospacedDigitSystemFont(ofSize: 17, weight: .regular)

    let incrementButton = UIButton(type: .system)
    incrementButton.addTarget(self, action: #selector(incrementButtonTapped), for: .touchUpInside)
    incrementButton.setTitle("+", for: .normal)

    let rootStackView = UIStackView(arrangedSubviews: [
      decrementButton,
      countLabel,
      incrementButton,
    ])
    rootStackView.translatesAutoresizingMaskIntoConstraints = false
    self.view.addSubview(rootStackView)

    NSLayoutConstraint.activate([
      rootStackView.centerXAnchor.constraint(equalTo: self.view.safeAreaLayoutGuide.centerXAnchor),
      rootStackView.centerYAnchor.constraint(equalTo: self.view.safeAreaLayoutGuide.centerYAnchor),
    ])
    
    viewStore.driver.drive(onNext: { state in
      countLabel.text = "\(state.count)"
    }).disposed(by: disposeBag)
  }
  
  @objc func decrementButtonTapped() {
    self.viewStore.send(.decrementButtonTapped)
  }

  @objc func incrementButtonTapped() {
    self.viewStore.send(.incrementButtonTapped)
  }
}
