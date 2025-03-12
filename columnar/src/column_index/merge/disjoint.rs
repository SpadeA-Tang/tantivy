use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::column_index::{SerializableColumnIndex, SerializableOptionalIndex};
use crate::iterable::Iterable;
use crate::{Cardinality, ColumnIndex, ColumnValues};

pub fn merge_column_index_disjoint<'a>(
    column_indexes: &'a [ColumnIndex],
    cardinality_after_merge: Cardinality,
) -> SerializableColumnIndex<'a> {
    match cardinality_after_merge {
        Cardinality::Multivalued => unimplemented!(),
        Cardinality::Optional => {
            let num_rows = column_indexes
                .iter()
                .map(|index| {
                    let ColumnIndex::Optional(optional_index) = index else {
                        unimplemented!()
                    };
                    optional_index.num_non_nulls()
                })
                .sum();

            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                non_null_row_ids: Box::new(DisjointRowIndex { column_indexes }),
                num_rows,
            })
        }
    }
}

struct DisjointRowIndex<'a> {
    column_indexes: &'a [ColumnIndex],
}

impl<'a> DisjointRowIndex<'a> {
    // Ord is the array index in column_indexes.
    fn iter_with_ord(&self) -> Box<dyn Iterator<Item = (usize, u32)> + '_> {
        let mut iters = self
            .column_indexes
            .iter()
            .map(|index| {
                let ColumnIndex::Optional(optional_index) = index else {
                    unimplemented!()
                };
                optional_index.iter_rows()
            })
            .collect::<Vec<_>>();

        let mut heap = BinaryHeap::new();

        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(val) = iter.next() {
                heap.push(Reverse((val, idx)));
            }
        }

        Box::new(std::iter::from_fn(move || {
            if let Some(Reverse((value, idx))) = heap.pop() {
                let iter = &mut iters[idx];
                if let Some(val) = iter.next() {
                    heap.push(Reverse((val, idx)));
                }
                Some((idx, value))
            } else {
                None
            }
        }))
    }
}

impl Iterable<u32> for DisjointRowIndex<'_> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        Box::new(self.iter_with_ord().map(|(_, value)| value))
    }
}

pub struct DisjointColumnValues<'a, T> {
    row_index: DisjointRowIndex<'a>,
    column_values: &'a [Option<Arc<dyn ColumnValues<T>>>],
}

impl<'a, T> DisjointColumnValues<'a, T> {
    pub fn new(
        column_indexes: &'a [ColumnIndex],
        column_values: &'a [Option<Arc<dyn ColumnValues<T>>>],
    ) -> Self {
        Self {
            row_index: DisjointRowIndex { column_indexes },
            column_values,
        }
    }
}

// impl<'a, T> DisjointColumnValues<'a, T>
// where T: PartialOrd + 'static
// {
//     pub fn boxed_iter(self) -> Box<dyn Iterator<Item = T> + 'a> {
//         let DisjointColumnValues {
//             row_index,
//             column_values,
//         } = self;

//         let mut row_index_iter = row_index.iter_with_ord();

//         Box::new(std::iter::from_fn(move || {
//             if let Some((ord, row_id)) = row_index_iter.next() {
//                 let column_index = &row_index.column_indexes[ord];
//                 let mut values = column_index
//                     .value_row_ids(row_id)
//                     .map(|value_row_id| column_values[ord].as_ref().unwrap().get_val(value_row_id))
//                     .collect::<Vec<_>>();
//                 assert!(values.len() == 1);
//                 values.pop()
//             } else {
//                 None
//             }
//         }))
//     }
// }
