use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::column_index::DisjointColumnValues;
use crate::iterable::Iterable;
use crate::{ColumnIndex, ColumnValues, MergeRowOrder};

pub(crate) struct MergedColumnValues<'a, T> {
    pub(crate) column_indexes: &'a [ColumnIndex],
    pub(crate) column_values: &'a [Option<Arc<dyn ColumnValues<T>>>],
    pub(crate) merge_row_order: &'a MergeRowOrder,
}

impl<T: Copy + PartialOrd + Debug + 'static> Iterable<T> for MergedColumnValues<'_, T> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        match self.merge_row_order {
            MergeRowOrder::Stack(_) => Box::new(
                self.column_values
                    .iter()
                    .flatten()
                    .flat_map(|column_value| column_value.iter()),
            ),
            MergeRowOrder::Shuffled(shuffle_merge_order) => Box::new(
                shuffle_merge_order
                    .iter_new_to_old_row_addrs()
                    .flat_map(|row_addr| {
                        let column_index = &self.column_indexes[row_addr.segment_ord as usize];
                        let column_values =
                            self.column_values[row_addr.segment_ord as usize].as_ref()?;
                        let value_range = column_index.value_row_ids(row_addr.row_id);
                        Some((value_range, column_values))
                    })
                    .flat_map(|(value_range, column_values)| {
                        value_range
                            .into_iter()
                            .map(|val| column_values.get_val(val))
                    }),
            ),
            MergeRowOrder::Disjoint => {
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

                let column_values = self.column_values.as_ref().clone();
                let column_indexes = self.column_indexes.as_ref().clone();

                let mut heap = BinaryHeap::new();

                for (idx, iter) in iters.iter_mut().enumerate() {
                    if let Some(val) = iter.next() {
                        heap.push(Reverse((val, idx)));
                    }
                }

                Box::new(std::iter::from_fn(move || {
                    if let Some(Reverse((row_id, ord))) = heap.pop() {
                        let iter = &mut iters[ord];
                        if let Some(val) = iter.next() {
                            heap.push(Reverse((val, ord)));
                        }

                        let column_index = &column_indexes[ord];
                        let mut values = column_index
                            .value_row_ids(row_id)
                            .map(|value_row_id| {
                                column_values[ord].as_ref().unwrap().get_val(value_row_id)
                            })
                            .collect::<Vec<_>>();
                        assert!(values.len() == 1);
                        values.pop()
                    } else {
                        None
                    }
                }))
            }
        }
    }
}
