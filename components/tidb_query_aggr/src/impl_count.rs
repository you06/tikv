// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::AggrFunction;
use tidb_query_common::Result;
use tidb_query_datatype::{
    FieldTypeFlag, FieldTypeTp, builder::FieldTypeBuilder, codec::data_type::*, expr::EvalContext,
};
use tidb_query_expr::RpnExpression;
use tipb::{Expr, ExprType, FieldType};

use super::*;

/// The parser for COUNT aggregate function.
pub struct AggrFnDefinitionParserCount;

impl super::AggrDefinitionParser for AggrFnDefinitionParserCount {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Count);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    #[inline]
    fn parse_rpn(
        &self,
        root_expr: Expr,
        exp: RpnExpression,
        _ctx: &mut EvalContext,
        _src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        assert_eq!(root_expr.get_tp(), ExprType::Count);

        // COUNT outputs one column.
        out_schema.push(
            FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(FieldTypeFlag::UNSIGNED)
                .build(),
        );

        out_exp.push(exp);

        Ok(Box::new(AggrFnCount))
    }
}

/// The COUNT aggregate function.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateCount::new())]
pub struct AggrFnCount;

/// The state of the COUNT aggregate function.
#[derive(Debug)]
pub struct AggrFnStateCount {
    count: usize,
}

impl AggrFnStateCount {
    pub fn new() -> Self {
        Self { count: 0 }
    }

    #[inline]
    fn update<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a>,
    {
        if value.is_some() {
            self.count += 1;
        }
        Ok(())
    }

    #[inline]
    fn update_repeat<'a, TT>(
        &mut self,
        _ctx: &mut EvalContext,
        value: Option<TT>,
        repeat_times: usize,
    ) -> Result<()>
    where
        TT: EvaluableRef<'a>,
    {
        // Will be used for expressions like `COUNT(1)`.
        if value.is_some() {
            self.count += repeat_times;
        }
        Ok(())
    }

    #[inline]
    fn update_vector<'a, TT, CC>(
        &mut self,
        _ctx: &mut EvalContext,
        _phantom_data: Option<TT>,
        physical_values: CC,
        logical_rows: &[usize],
    ) -> Result<()>
    where
        TT: EvaluableRef<'a>,
        CC: ChunkRef<'a, TT>,
    {
        // Will be used for expressions like `COUNT(col)`.
        if logical_rows.is_empty() {
            return Ok(());
        }

        let len = logical_rows.len();
        let start = logical_rows[0];
        let end = start + len;

        // Check if logical_rows represents a contiguous range [start, end).
        // This is a O(1) check that is sufficient when logical_rows has no duplicates
        // (which is the typical case in aggregation).
        let is_contiguous =
            len == 1 || (logical_rows[len - 1] == end - 1 && end <= physical_values.get_bit_vec().len());

        if is_contiguous {
            // Fast path: use hardware POPCNT to count non-NULL values in the bitmap.
            self.count += physical_values.get_bit_vec().count_ones_range(start, end);
        } else {
            // Slow path: iterate through each element.
            for physical_index in logical_rows {
                if physical_values.get_option_ref(*physical_index).is_some() {
                    self.count += 1;
                }
            }
        }
        Ok(())
    }
}

// Here we manually implement `AggrFunctionStateUpdatePartial` so that
// `update_repeat` and `update_vector` can be faster. Also note that we support
// all kind of `AggrFunctionStateUpdatePartial` for the COUNT aggregate
// function.

impl<T> super::AggrFunctionStateUpdatePartial<T> for AggrFnStateCount
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    impl_state_update_partial! { T }
}

impl super::AggrFunctionState for AggrFnStateCount {
    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        target[0].push(Some(self.count as Int));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tidb_query_datatype::EvalType;
    use tikv_util::buffer_vec::BufferVec;

    use super::{super::AggrFunction, *};

    #[test]
    fn test_update() {
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);

        update!(state, &mut ctx, Option::<&Real>::None).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);

        update!(state, &mut ctx, Real::new(5.0).ok().as_ref()).unwrap();
        update!(state, &mut ctx, Option::<&Real>::None).unwrap();
        update!(state, &mut ctx, Some(&7i64)).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);

        update_repeat!(state, &mut ctx, Some(&3i64), 4).unwrap();
        update_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(6)]);

        let chunked_vec: ChunkedVecSized<Int> = vec![Some(1i64), None, Some(-1i64)].into();
        update_vector!(state, &mut ctx, chunked_vec, &[1, 2]).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(7)]);
    }

    #[test]
    fn test_update_enum() {
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        update!(state, &mut ctx, Some(EnumRef::new("bbb".as_bytes(), &1))).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(1)]);
    }

    #[test]
    fn test_update_set() {
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        let mut buf = BufferVec::new();
        buf.push("我好强啊");
        buf.push("我太强啦");
        let buf = Arc::new(buf);

        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b11))).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(1)]);
    }

    #[test]
    fn test_update_vector_contiguous() {
        // Test the optimized fast path with contiguous logical_rows
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        // Create a chunked vec with mix of Some and None values
        let chunked_vec: ChunkedVecSized<Int> = vec![
            Some(1i64),
            None,
            Some(2i64),
            Some(3i64),
            None,
            Some(4i64),
            None,
            None,
            Some(5i64),
            Some(6i64),
        ]
        .into();

        // Contiguous rows [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] - fast path
        update_vector!(state, &mut ctx, &chunked_vec, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(6)]); // 6 non-null values
    }

    #[test]
    fn test_update_vector_non_contiguous() {
        // Test the slow path with non-contiguous logical_rows
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        // Create a chunked vec with mix of Some and None values
        let chunked_vec: ChunkedVecSized<Int> = vec![
            Some(1i64), // 0
            None,       // 1
            Some(2i64), // 2
            Some(3i64), // 3
            None,       // 4
            Some(4i64), // 5
            None,       // 6
            None,       // 7
            Some(5i64), // 8
            Some(6i64), // 9
        ]
        .into();

        // Non-contiguous rows [0, 2, 4, 6, 8] - slow path
        update_vector!(state, &mut ctx, &chunked_vec, &[0, 2, 4, 6, 8]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        // Rows 0, 2, 8 have values (Some), rows 4, 6 are None
        assert_eq!(result[0].to_int_vec(), &[Some(3)]);
    }

    #[test]
    fn test_update_vector_partial_contiguous() {
        // Test contiguous rows starting from non-zero index
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        let chunked_vec: ChunkedVecSized<Int> = vec![
            None,       // 0
            Some(1i64), // 1
            Some(2i64), // 2
            None,       // 3
            Some(3i64), // 4
            Some(4i64), // 5
        ]
        .into();

        // Contiguous rows [2, 3, 4] starting from index 2
        update_vector!(state, &mut ctx, &chunked_vec, &[2, 3, 4]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        // Rows 2 and 4 have values, row 3 is None
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);
    }

    #[test]
    fn test_update_vector_large_contiguous() {
        // Test with larger data to exercise multi-word POPCNT
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        // Create a chunked vec with 200 elements, alternating Some/None
        let data: Vec<Option<Int>> = (0..200).map(|i| if i % 3 == 0 { None } else { Some(i) }).collect();
        let chunked_vec: ChunkedVecSized<Int> = data.into();

        // Contiguous rows [0..200]
        let logical_rows: Vec<usize> = (0..200).collect();
        update_vector!(state, &mut ctx, &chunked_vec, &logical_rows).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        // 200 elements, every 3rd is None: 200 - 67 = 133 non-null
        // (indices 0, 3, 6, ..., 198 are None: (198/3)+1 = 67)
        assert_eq!(result[0].to_int_vec(), &[Some(133)]);
    }

    #[test]
    fn test_update_vector_empty() {
        // Test with empty logical_rows
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        let chunked_vec: ChunkedVecSized<Int> = vec![Some(1i64), Some(2i64)].into();

        // Empty logical_rows
        update_vector!(state, &mut ctx, &chunked_vec, &[]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);
    }

    #[test]
    fn test_update_vector_single_element() {
        // Test with single element
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        let chunked_vec: ChunkedVecSized<Int> = vec![None, Some(1i64), None].into();

        // Single element at index 1 (which is Some)
        update_vector!(state, &mut ctx, &chunked_vec, &[1]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(1)]);

        // Reset and test single None element
        let function2 = AggrFnCount;
        let mut state2 = function2.create_state();
        let mut result2 = [VectorValue::with_capacity(0, EvalType::Int)];

        update_vector!(state2, &mut ctx, &chunked_vec, &[0]).unwrap();

        state2.push_result(&mut ctx, &mut result2).unwrap();
        assert_eq!(result2[0].to_int_vec(), &[Some(0)]);
    }
}
