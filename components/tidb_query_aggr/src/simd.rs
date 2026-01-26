// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

//! SIMD utilities for aggregate functions.
//!
//! This module provides SIMD-optimized implementations for common aggregation
//! operations like sum. It uses AVX2/AVX-512 when available, with automatic
//! fallback to scalar implementations on unsupported platforms.

/// Sums all f64 values in the given slice using SIMD when available.
///
/// This function uses 4 independent accumulators to reduce dependency chains
/// and maximize instruction-level parallelism. NULL values in ChunkedVecSized
/// are stored as 0.0, so this correctly handles them by contributing 0 to the sum.
///
/// # Arguments
/// * `data` - Slice of f64 values to sum
///
/// # Returns
/// The sum of all values in the slice
#[inline]
pub fn sum_f64_slice(data: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We've checked that AVX2 is available
            return unsafe { sum_f64_avx2(data) };
        }
    }
    sum_f64_scalar(data)
}

/// Scalar fallback implementation for summing f64 values.
#[inline]
fn sum_f64_scalar(data: &[f64]) -> f64 {
    // Use 4 accumulators to reduce dependency chains
    let mut sum0 = 0.0f64;
    let mut sum1 = 0.0f64;
    let mut sum2 = 0.0f64;
    let mut sum3 = 0.0f64;

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        sum0 += chunk[0];
        sum1 += chunk[1];
        sum2 += chunk[2];
        sum3 += chunk[3];
    }

    for val in remainder {
        sum0 += val;
    }

    (sum0 + sum1) + (sum2 + sum3)
}

/// AVX2-optimized implementation for summing f64 values.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_f64_avx2(data: &[f64]) -> f64 {
    use std::arch::x86_64::*;

    let len = data.len();
    if len < 16 {
        // For small arrays, use scalar with 4 accumulators
        return sum_f64_scalar(data);
    }

    // Use 4 independent __m256d accumulators (each holds 4 f64 values)
    // This reduces dependency chains and allows better pipelining
    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();
    let mut acc2 = _mm256_setzero_pd();
    let mut acc3 = _mm256_setzero_pd();

    let ptr = data.as_ptr();
    let mut i = 0;

    // Process 16 elements (4 * 4) at a time
    while i + 16 <= len {
        let v0 = _mm256_loadu_pd(ptr.add(i));
        let v1 = _mm256_loadu_pd(ptr.add(i + 4));
        let v2 = _mm256_loadu_pd(ptr.add(i + 8));
        let v3 = _mm256_loadu_pd(ptr.add(i + 12));

        acc0 = _mm256_add_pd(acc0, v0);
        acc1 = _mm256_add_pd(acc1, v1);
        acc2 = _mm256_add_pd(acc2, v2);
        acc3 = _mm256_add_pd(acc3, v3);

        i += 16;
    }

    // Process remaining 4-element chunks
    while i + 4 <= len {
        let v = _mm256_loadu_pd(ptr.add(i));
        acc0 = _mm256_add_pd(acc0, v);
        i += 4;
    }

    // Combine all accumulators
    acc0 = _mm256_add_pd(acc0, acc1);
    acc2 = _mm256_add_pd(acc2, acc3);
    acc0 = _mm256_add_pd(acc0, acc2);

    // Horizontal sum of the final accumulator
    // acc0 = [a, b, c, d]
    // We need: a + b + c + d

    // Extract high 128 bits and add to low 128 bits
    let high = _mm256_extractf128_pd(acc0, 1); // [c, d]
    let low = _mm256_castpd256_pd128(acc0); // [a, b]
    let sum128 = _mm_add_pd(low, high); // [a+c, b+d]

    // Horizontal add within 128-bit register
    let sum_high = _mm_unpackhi_pd(sum128, sum128); // [b+d, b+d]
    let sum_final = _mm_add_sd(sum128, sum_high); // [(a+c)+(b+d), ...]

    // Handle remaining elements
    let mut sum = _mm_cvtsd_f64(sum_final);
    while i < len {
        sum += *ptr.add(i);
        i += 1;
    }

    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_f64_slice_empty() {
        let data: [f64; 0] = [];
        assert_eq!(sum_f64_slice(&data), 0.0);
    }

    #[test]
    fn test_sum_f64_slice_single() {
        let data = [42.5];
        assert_eq!(sum_f64_slice(&data), 42.5);
    }

    #[test]
    fn test_sum_f64_slice_small() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(sum_f64_slice(&data), 15.0);
    }

    #[test]
    fn test_sum_f64_slice_with_zeros() {
        // Simulates NULL values which are stored as 0.0
        let data = [1.0, 0.0, 3.0, 0.0, 5.0, 0.0, 7.0, 0.0];
        assert_eq!(sum_f64_slice(&data), 16.0);
    }

    #[test]
    fn test_sum_f64_slice_large() {
        // Test with a large array to exercise SIMD path
        let data: Vec<f64> = (1..=1000).map(|x| x as f64).collect();
        let expected: f64 = (1..=1000).sum::<i32>() as f64;
        assert_eq!(sum_f64_slice(&data), expected);
    }

    #[test]
    fn test_sum_f64_slice_negative() {
        let data = [-1.0, -2.0, -3.0, -4.0];
        assert_eq!(sum_f64_slice(&data), -10.0);
    }

    #[test]
    fn test_sum_f64_slice_mixed() {
        let data = [1.5, -2.5, 3.5, -4.5, 5.5];
        assert_eq!(sum_f64_slice(&data), 3.5);
    }

    #[test]
    fn test_sum_f64_slice_various_sizes() {
        // Test various sizes to exercise different code paths
        for size in [0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 256] {
            let data: Vec<f64> = (0..size).map(|x| x as f64).collect();
            let expected: f64 = if size == 0 {
                0.0
            } else {
                (0..size).sum::<i32>() as f64
            };
            assert_eq!(
                sum_f64_slice(&data),
                expected,
                "Failed for size {}",
                size
            );
        }
    }

    #[test]
    fn test_sum_f64_scalar_accuracy() {
        // Verify scalar fallback produces correct results
        let data: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let expected: f64 = (1..=100).sum::<i32>() as f64;
        assert_eq!(sum_f64_scalar(&data), expected);
    }
}
