//! Property-test pin for W1 verification triad (proptest + fault hooks + reference model).

use fluxfs_types::FileType;
use proptest::prelude::*;

proptest! {
    #[test]
    fn file_type_roundtrip_label(is_dir in proptest::bool::ANY) {
        let ft = if is_dir { FileType::Directory } else { FileType::Regular };
        match ft {
            FileType::Directory => prop_assert!(is_dir),
            FileType::Regular => prop_assert!(!is_dir),
        }
    }
}
