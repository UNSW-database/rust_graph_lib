extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::map::{SetMap, VecMap};

#[test]
fn test_set_map() {
    let mut label_map = SetMap::<&str>::new();

    assert_eq!(label_map.len(), 0);

    assert_eq!(label_map.add_item("zero"), 0);
    assert_eq!(label_map.add_item("first"), 1);
    assert_eq!(label_map.add_item("zero"), 0);
    assert_eq!(label_map.add_item("first"), 1);

    assert_eq!(label_map.len(), 2);
    assert_eq!(label_map.find_item(0), Some(&"zero"));
    assert_eq!(label_map.find_item(1), Some(&"first"));

    assert_eq!(label_map.find_item(2), None);

    assert!(label_map.contains(&"zero"));
    assert!(!label_map.contains(&"five"));

    assert_eq!(label_map.find_index(&"zero"), Some(0));
    assert_eq!(label_map.find_index(&"first"), Some(1));
    assert_eq!(label_map.find_index(&"five"), None);

    let items: Vec<_> = label_map.items().collect();
    assert_eq!(items, vec![&"zero", &"first"]);
}

#[test]
fn test_vec_map() {
    let mut label_map = VecMap::<&str>::new();

    assert_eq!(label_map.len(), 0);

    assert_eq!(label_map.add_item("zero"), 0);
    assert_eq!(label_map.add_item("first"), 1);
    assert_eq!(label_map.add_item("zero"), 0);
    assert_eq!(label_map.add_item("first"), 1);

    assert_eq!(label_map.len(), 2);
    assert_eq!(label_map.find_item(0), Some(&"zero"));
    assert_eq!(label_map.find_item(1), Some(&"first"));

    assert_eq!(label_map.find_item(2), None);

    assert!(label_map.contains(&"zero"));
    assert!(!label_map.contains(&"five"));

    assert_eq!(label_map.find_index(&"zero"), Some(0));
    assert_eq!(label_map.find_index(&"first"), Some(1));
    assert_eq!(label_map.find_index(&"five"), None);

    let items: Vec<_> = label_map.items().collect();
    assert_eq!(items, vec![&"zero", &"first"]);
}
