// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::AggrFunction;
use tidb_query_common::Result;
use tidb_query_datatype::{EvalType, codec::data_type::*, expr::EvalContext};
use tidb_query_expr::RpnExpression;
use tipb::{Expr, ExprType, FieldType};

use super::{summable::Summable, *};

/// The parser for SUM aggregate function.
pub struct AggrFnDefinitionParserSum;

impl super::parser::AggrDefinitionParser for AggrFnDefinitionParserSum {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Sum);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    #[inline]
    fn parse_rpn(
        &self,
        mut root_expr: Expr,
        mut exp: RpnExpression,
        _ctx: &mut EvalContext,
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        use tidb_query_datatype::FieldTypeAccessor;

        assert_eq!(root_expr.get_tp(), ExprType::Sum);

        let out_ft = root_expr.take_field_type();
        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));

        // Rewrite expression to insert CAST() if needed.
        super::util::rewrite_exp_for_sum_avg(src_schema, &mut exp)?;

        let rewritten_eval_type =
            EvalType::try_from(exp.ret_field_type(src_schema).as_accessor().tp()).unwrap();
        if out_et != rewritten_eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // SUM outputs one column.
        out_schema.push(out_ft);
        out_exp.push(exp);

        // Choose a type-aware SUM implementation based on the eval type after rewriting
        // exp.
        Ok(match rewritten_eval_type {
            EvalType::Decimal => Box::new(AggrFnSum::<Decimal>::new()),
            EvalType::Real => Box::new(AggrFnSum::<Real>::new()),
            EvalType::Enum => Box::new(AggrFnSumForEnum::new()),
            EvalType::Set => Box::new(AggrFnSumForSet::new()),
            // If we meet unexpected types after rewriting, it is an implementation fault.
            _ => unreachable!(),
        })
    }
}

/// The SUM aggregate function.
///
/// Note that there are `SUM(Decimal) -> Decimal` and `SUM(Double) -> Double`.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateSum::<T>::new())]
pub struct AggrFnSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<T>,
}

impl<T> AggrFnSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// The state of the SUM aggregate function.
#[derive(Debug)]
pub struct AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    sum: T,
    has_value: bool,
}

impl<T> AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            sum: T::zero(),
            has_value: false,
        }
    }

    #[inline]
    fn update_concrete<'a, TT>(&mut self, ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T>,
    {
        match value {
            None => Ok(()),
            Some(value) => {
                self.sum.add_assign(ctx, &value.into_owned_value())?;
                self.has_value = true;
                Ok(())
            }
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = &'static T;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        if !self.has_value {
            target[0].push(None);
        } else {
            target[0].push(Some(self.sum.clone()));
        }
        Ok(())
    }
}

// Specialized SIMD-optimized implementation for SUM(Real).
//
// This provides a custom update_vector_unsafe that uses SIMD instructions
// to accelerate summing f64 values when:
// 1. The logical_rows are contiguous (common case in aggregation)
// 2. AVX2 instructions are available on x86_64
//
// Key optimization: NULL values in ChunkedVecSized are stored as zeroed memory
// (0.0 for f64), so we can safely sum all values without special NULL handling.
// The bitmap is only needed to determine if has_value should be set.
impl super::AggrFunctionStateUpdatePartial<&'static Real> for AggrFnStateSum<Real> {
    #[inline]
    unsafe fn update_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        value: Option<&'static Real>,
    ) -> Result<()> {
        self.update_concrete(ctx, value)
    }

    #[inline]
    unsafe fn update_repeat_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        value: Option<&'static Real>,
        repeat_times: usize,
    ) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                // SAFETY: Multiplying a finite f64 by a positive integer can only produce
                // a finite value or infinity, never NaN. Real guarantees finite values.
                let repeated_sum = Real::new(value.into_inner() * repeat_times as f64).unwrap();
                self.sum.add_assign(ctx, &repeated_sum)?;
                self.has_value = true;
                Ok(())
            }
        }
    }

    #[inline]
    unsafe fn update_vector_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        _phantom_data: Option<&'static Real>,
        physical_values: &'static ChunkedVecSized<Real>,
        logical_rows: &[usize],
    ) -> Result<()> {
        if logical_rows.is_empty() {
            return Ok(());
        }

        let len = logical_rows.len();
        let start = logical_rows[0];
        let end = start + len;

        // Check if logical_rows is contiguous
        let is_contiguous = len == 1
            || (logical_rows[len - 1] == end - 1
                && end <= physical_values.get_bit_vec().len());

        if is_contiguous {
            // Fast path: use SIMD to sum all values in the contiguous range.
            // NULL values are stored as 0.0, so they contribute nothing to the sum.
            let bit_vec = physical_values.get_bit_vec();
            let non_null_count = bit_vec.count_ones_range(start, end);

            if non_null_count > 0 {
                self.has_value = true;

                // Get raw data and sum using SIMD.
                // SAFETY: Real (NotNan<f64>) has the same memory layout as f64.
                let raw_data = physical_values.raw_data();
                let data_slice = &raw_data[start..end];
                let f64_slice: &[f64] =
                    std::slice::from_raw_parts(data_slice.as_ptr() as *const f64, data_slice.len());

                let sum = crate::simd::sum_f64_slice(f64_slice);
                // SAFETY: Adding finite f64 values can only produce finite or infinite
                // results, never NaN. Real guarantees finite values, and NULL values
                // are stored as 0.0 which is also finite.
                self.sum = Real::new(self.sum.into_inner() + sum).unwrap();
            }
        } else {
            // Slow path: iterate through non-contiguous elements
            for physical_index in logical_rows {
                if let Some(value) = physical_values.get_option_ref(*physical_index) {
                    self.sum += *value;
                    self.has_value = true;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateSumForEnum::new())]
pub struct AggrFnSumForEnum
where
    VectorValue: VectorValueExt<Decimal>,
{
    _phantom: std::marker::PhantomData<Decimal>,
}

impl AggrFnSumForEnum
where
    VectorValue: VectorValueExt<Decimal>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AggrFnStateSumForEnum
where
    VectorValue: VectorValueExt<Decimal>,
{
    sum: Decimal,
    has_value: bool,
}

impl AggrFnStateSumForEnum
where
    VectorValue: VectorValueExt<Decimal>,
{
    pub fn new() -> Self {
        Self {
            sum: Decimal::zero(),
            has_value: false,
        }
    }

    /// # Notes
    ///
    /// Functions such as SUM() or AVG() that expect a numeric argument cast the
    /// argument to a number if necessary. For ENUM values, the index number is
    /// used in the calculation.
    ///
    /// ref: https://dev.mysql.com/doc/refman/8.0/en/enum.html
    #[inline]
    fn update_concrete(&mut self, ctx: &mut EvalContext, value: Option<EnumRef<'_>>) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                self.sum.add_assign(ctx, &Decimal::from(value.value()))?;
                self.has_value = true;
                Ok(())
            }
        }
    }
}

impl super::ConcreteAggrFunctionState for AggrFnStateSumForEnum
where
    VectorValue: VectorValueExt<Decimal>,
{
    type ParameterType = EnumRef<'static>;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        let result = if self.has_value { Some(self.sum) } else { None };

        target[0].push(result);
        Ok(())
    }
}

#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateSumForSet::new())]
pub struct AggrFnSumForSet
where
    VectorValue: VectorValueExt<Decimal>,
{
    _phantom: std::marker::PhantomData<Decimal>,
}

impl AggrFnSumForSet
where
    VectorValue: VectorValueExt<Decimal>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AggrFnStateSumForSet
where
    VectorValue: VectorValueExt<Decimal>,
{
    sum: Decimal,
    has_value: bool,
}

impl AggrFnStateSumForSet
where
    VectorValue: VectorValueExt<Decimal>,
{
    pub fn new() -> Self {
        Self {
            sum: Decimal::zero(),
            has_value: false,
        }
    }

    /// # Notes
    ///
    /// Functions such as SUM() or AVG() that expect a numeric argument cast the
    /// argument to a number if necessary. For ENUM values, the index number is
    /// used in the calculation.
    ///
    /// ref: https://dev.mysql.com/doc/refman/8.0/en/enum.html
    #[inline]
    fn update_concrete(&mut self, ctx: &mut EvalContext, value: Option<SetRef<'_>>) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                self.sum.add_assign(ctx, &Decimal::from(value.value()))?;
                self.has_value = true;
                Ok(())
            }
        }
    }
}

impl super::ConcreteAggrFunctionState for AggrFnStateSumForSet
where
    VectorValue: VectorValueExt<Decimal>,
{
    type ParameterType = SetRef<'static>;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        let result = if self.has_value { Some(self.sum) } else { None };

        target[0].push(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tidb_query_datatype::{
        FieldTypeAccessor, FieldTypeTp,
        codec::batch::{LazyBatchColumn, LazyBatchColumnVec},
    };
    use tikv_util::buffer_vec::BufferVec;
    use tipb_helper::ExprDefBuilder;

    use super::*;
    use crate::parser::AggrDefinitionParser;

    #[test]
    fn test_sum_enum() {
        let mut ctx = EvalContext::default();
        let function = AggrFnSumForEnum::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Decimal)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_decimal_vec(), &[None]);

        update!(state, &mut ctx, Some(EnumRef::new("aaa".as_bytes(), &2))).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_decimal_vec(), vec![Some(Decimal::from(2))]);

        update!(state, &mut ctx, Some(EnumRef::new("bbb".as_bytes(), &1))).unwrap();
        update!(state, &mut ctx, Some(EnumRef::new("aaa".as_bytes(), &2))).unwrap();
        update!(state, &mut ctx, Some(EnumRef::new("aaa".as_bytes(), &2))).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_decimal_vec(), vec![Some(Decimal::from(7))]);
    }

    #[test]
    fn test_sum_set() {
        let mut ctx = EvalContext::default();
        let function = AggrFnSumForSet::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Decimal)];

        let mut buf = BufferVec::new();
        buf.push("我好强啊");
        buf.push("我太强啦");
        let buf = Arc::new(buf);

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_decimal_vec(), &[None]);

        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b10))).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_decimal_vec(), vec![Some(Decimal::from(2))]);

        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b01))).unwrap();
        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b10))).unwrap();
        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b10))).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_decimal_vec(), vec![Some(Decimal::from(7))]);
    }

    /// SUM(Bytes) should produce (Real).
    #[test]
    fn test_integration() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Sum, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::VarString))
            .build();
        AggrFnDefinitionParserSum.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::VarString.into()];
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Bytes);
            col.mut_decoded().push_bytes(Some(b"12.5".to_vec()));
            col.mut_decoded().push_bytes(None);
            col.mut_decoded().push_bytes(Some(b"10000.0".to_vec()));
            col.mut_decoded().push_bytes(Some(b"42.0".to_vec()));
            col.mut_decoded().push_bytes(None);
            col
        }]);
        let logical_rows = vec![0, 1, 3, 4];

        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();

        let aggr_fn = AggrFnDefinitionParserSum
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].as_accessor().tp(), FieldTypeTp::Double);

        assert_eq!(exp.len(), 1);

        let mut state = aggr_fn.create_state();

        let exp_result = exp[0]
            .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 4)
            .unwrap();
        let exp_result = exp_result.vector_value().unwrap();
        let vec = exp_result.as_ref().to_real_vec();
        let chunked_vec: ChunkedVecSized<Real> = vec.into();
        update_vector!(state, &mut ctx, chunked_vec, exp_result.logical_rows()).unwrap();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Real)];
        state.push_result(&mut ctx, &mut aggr_result).unwrap();

        assert_eq!(aggr_result[0].to_real_vec(), &[Real::new(54.5).ok()]);
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Sum, FieldTypeTp::Double) // Expect NewDecimal but give Double
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong)) // FIXME: This type can be incorrect as well
            .build();
        AggrFnDefinitionParserSum.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserSum
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap_err();
    }

    #[test]
    fn test_sum_real_vector_contiguous() {
        // Test SIMD fast path with contiguous logical_rows
        let mut ctx = EvalContext::default();
        let function = AggrFnSum::<Real>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Real)];

        // Create a chunked vec with mix of Some and None values
        let chunked_vec: ChunkedVecSized<Real> = vec![
            Real::new(1.0).ok(),
            None,
            Real::new(2.0).ok(),
            Real::new(3.0).ok(),
            None,
            Real::new(4.0).ok(),
            None,
            None,
            Real::new(5.0).ok(),
            Real::new(6.0).ok(),
        ]
        .into();

        // Contiguous rows [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] - fast path
        update_vector!(state, &mut ctx, &chunked_vec, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        // Sum of 1 + 2 + 3 + 4 + 5 + 6 = 21
        assert_eq!(result[0].to_real_vec(), &[Real::new(21.0).ok()]);
    }

    #[test]
    fn test_sum_real_vector_non_contiguous() {
        // Test slow path with non-contiguous logical_rows
        let mut ctx = EvalContext::default();
        let function = AggrFnSum::<Real>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Real)];

        let chunked_vec: ChunkedVecSized<Real> = vec![
            Real::new(1.0).ok(), // 0
            None,                // 1
            Real::new(2.0).ok(), // 2
            Real::new(3.0).ok(), // 3
            None,                // 4
            Real::new(4.0).ok(), // 5
        ]
        .into();

        // Non-contiguous rows [0, 2, 4] - slow path
        update_vector!(state, &mut ctx, &chunked_vec, &[0, 2, 4]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        // Rows 0 and 2 have values (1.0 + 2.0 = 3.0), row 4 is None
        assert_eq!(result[0].to_real_vec(), &[Real::new(3.0).ok()]);
    }

    #[test]
    fn test_sum_real_vector_large_contiguous() {
        // Test with larger data to exercise multi-element SIMD processing
        let mut ctx = EvalContext::default();
        let function = AggrFnSum::<Real>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Real)];

        // Create a chunked vec with 200 elements
        let data: Vec<Option<Real>> = (0..200)
            .map(|i| {
                if i % 3 == 0 {
                    None
                } else {
                    Real::new(i as f64).ok()
                }
            })
            .collect();
        let chunked_vec: ChunkedVecSized<Real> = data.into();

        // Contiguous rows [0..200]
        let logical_rows: Vec<usize> = (0..200).collect();
        update_vector!(state, &mut ctx, &chunked_vec, &logical_rows).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();

        // Calculate expected sum: sum of i for i in 0..200 where i % 3 != 0
        let expected: f64 = (0..200).filter(|&i| i % 3 != 0).map(|i| i as f64).sum();
        assert_eq!(result[0].to_real_vec(), &[Real::new(expected).ok()]);
    }

    #[test]
    fn test_sum_real_vector_all_null() {
        // Test when all values are NULL
        let mut ctx = EvalContext::default();
        let function = AggrFnSum::<Real>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Real)];

        let chunked_vec: ChunkedVecSized<Real> = vec![None, None, None, None].into();

        update_vector!(state, &mut ctx, &chunked_vec, &[0, 1, 2, 3]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        // All NULL means no value, result should be NULL
        assert_eq!(result[0].to_real_vec(), &[None]);
    }

    #[test]
    fn test_sum_real_vector_empty() {
        // Test with empty logical_rows
        let mut ctx = EvalContext::default();
        let function = AggrFnSum::<Real>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Real)];

        let chunked_vec: ChunkedVecSized<Real> =
            vec![Real::new(1.0).ok(), Real::new(2.0).ok()].into();

        update_vector!(state, &mut ctx, &chunked_vec, &[]).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_real_vec(), &[None]);
    }

    #[test]
    fn test_sum_real_update_repeat() {
        // Test the optimized update_repeat_unsafe
        let mut ctx = EvalContext::default();
        let function = AggrFnSum::<Real>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Real)];

        update_repeat!(state, &mut ctx, Real::new(5.0).ok().as_ref(), 4).unwrap();

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_real_vec(), &[Real::new(20.0).ok()]);

        // Test update_repeat with None
        let function2 = AggrFnSum::<Real>::new();
        let mut state2 = function2.create_state();
        let mut result2 = [VectorValue::with_capacity(0, EvalType::Real)];

        update_repeat!(state2, &mut ctx, Option::<&Real>::None, 10).unwrap();

        state2.push_result(&mut ctx, &mut result2).unwrap();
        assert_eq!(result2[0].to_real_vec(), &[None]);
    }
}
