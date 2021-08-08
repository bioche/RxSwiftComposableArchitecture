#if canImport(UIKit)
#if !os(watchOS)
import Foundation
import DifferenceKit
import ComposableArchitecture

extension RxFlatCollectionDataSource where ItemModel: TCAIdentifiable {
  public static var differenceKitReloading: ApplyingChanges {
    return { collectionView, datasource, observedEvent in
      let source = datasource.values
      let target = observedEvent.element ?? []
      let changeset = StagedChangeset(source: source, target: target)
      
      collectionView.reload(using: changeset) { data in
        datasource.values = data
      }
    }
  }
}
#endif
#endif
