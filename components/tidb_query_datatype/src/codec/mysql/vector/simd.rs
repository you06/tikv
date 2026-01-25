// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! SIMD-accelerated vector distance calculations.
//!
//! This module provides AVX2/AVX-512 optimized implementations of vector
//! distance functions with runtime CPU feature detection.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Cached CPU feature detection result.
/// Using a simple enum to avoid repeated runtime checks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SimdLevel {
    /// No SIMD support or non-x86 platform
    Scalar,
    /// AVX2 support (256-bit vectors, 8 x f32)
    Avx2,
    /// AVX-512F support (512-bit vectors, 16 x f32)
    Avx512,
}

impl SimdLevel {
    /// Detect the best available SIMD level at runtime.
    #[inline]
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            // Check AVX-512F first (highest priority)
            if is_x86_feature_detected!("avx512f") {
                return SimdLevel::Avx512;
            }
            // Check AVX2
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return SimdLevel::Avx2;
            }
        }
        SimdLevel::Scalar
    }
}

/// Get the detected SIMD level (cached via lazy initialization).
#[inline]
pub fn simd_level() -> SimdLevel {
    // Use a simple detection each time. The is_x86_feature_detected! macro
    // is already optimized and caches its result internally.
    SimdLevel::detect()
}

// ============================================================================
// AVX2 implementations
// ============================================================================

#[cfg(target_arch = "x86_64")]
mod avx2 {
    use super::*;

    /// Horizontal sum of an __m256 register (8 x f32 -> f32)
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn hsum_ps_256(v: __m256) -> f32 {
        // v = [a0, a1, a2, a3, a4, a5, a6, a7]
        // Extract high 128 bits
        let high = _mm256_extractf128_ps(v, 1); // [a4, a5, a6, a7]
        let low = _mm256_castps256_ps128(v); // [a0, a1, a2, a3]
        // Add high and low
        let sum128 = _mm_add_ps(low, high); // [a0+a4, a1+a5, a2+a6, a3+a7]
        // Horizontal add within 128-bit register
        let shuf = _mm_movehdup_ps(sum128); // [a1+a5, a1+a5, a3+a7, a3+a7]
        let sums = _mm_add_ps(sum128, shuf); // [a0+a4+a1+a5, ...]
        let shuf = _mm_movehl_ps(sums, sums); // Move high 64 bits to low
        let sums = _mm_add_ss(sums, shuf);
        _mm_cvtss_f32(sums)
    }

    /// Horizontal sum of an __m256d register (4 x f64 -> f64)
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn hsum_pd_256(v: __m256d) -> f64 {
        let high = _mm256_extractf128_pd(v, 1);
        let low = _mm256_castpd256_pd128(v);
        let sum128 = _mm_add_pd(low, high);
        let high64 = _mm_unpackhi_pd(sum128, sum128);
        let sum = _mm_add_sd(sum128, high64);
        _mm_cvtsd_f64(sum)
    }

    /// AVX2 implementation of L2 squared distance.
    /// Processes 8 f32 elements per iteration.
    #[target_feature(enable = "avx2", enable = "fma")]
    pub unsafe fn l2_squared_distance_avx2(a: &[u8], b: &[u8]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4; // Number of f32 elements
        let a_ptr = a.as_ptr() as *const f32;
        let b_ptr = b.as_ptr() as *const f32;

        let mut sum = _mm256_setzero_ps();
        let mut i = 0;

        // Process 8 elements at a time
        while i + 8 <= n {
            let va = _mm256_loadu_ps(a_ptr.add(i));
            let vb = _mm256_loadu_ps(b_ptr.add(i));
            let diff = _mm256_sub_ps(va, vb);
            sum = _mm256_fmadd_ps(diff, diff, sum);
            i += 8;
        }

        // Handle remaining elements (less than 8)
        let mut result = hsum_ps_256(sum);
        while i < n {
            let diff = *a_ptr.add(i) - *b_ptr.add(i);
            result += diff * diff;
            i += 1;
        }

        result
    }

    /// AVX2 implementation of inner product.
    #[target_feature(enable = "avx2", enable = "fma")]
    pub unsafe fn inner_product_avx2(a: &[u8], b: &[u8]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;
        let b_ptr = b.as_ptr() as *const f32;

        let mut sum = _mm256_setzero_ps();
        let mut i = 0;

        while i + 8 <= n {
            let va = _mm256_loadu_ps(a_ptr.add(i));
            let vb = _mm256_loadu_ps(b_ptr.add(i));
            sum = _mm256_fmadd_ps(va, vb, sum);
            i += 8;
        }

        let mut result = hsum_ps_256(sum);
        while i < n {
            result += *a_ptr.add(i) * *b_ptr.add(i);
            i += 1;
        }

        result
    }

    /// AVX2 implementation of cosine distance.
    /// Computes dot product and both norms in a single pass.
    #[target_feature(enable = "avx2", enable = "fma")]
    pub unsafe fn cosine_distance_avx2(a: &[u8], b: &[u8]) -> (f32, f32, f32) {
        debug_assert_eq!(a.len(), b.len());
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;
        let b_ptr = b.as_ptr() as *const f32;

        let mut dot_sum = _mm256_setzero_ps();
        let mut norm_a_sum = _mm256_setzero_ps();
        let mut norm_b_sum = _mm256_setzero_ps();
        let mut i = 0;

        while i + 8 <= n {
            let va = _mm256_loadu_ps(a_ptr.add(i));
            let vb = _mm256_loadu_ps(b_ptr.add(i));
            dot_sum = _mm256_fmadd_ps(va, vb, dot_sum);
            norm_a_sum = _mm256_fmadd_ps(va, va, norm_a_sum);
            norm_b_sum = _mm256_fmadd_ps(vb, vb, norm_b_sum);
            i += 8;
        }

        let mut dot = hsum_ps_256(dot_sum);
        let mut norm_a = hsum_ps_256(norm_a_sum);
        let mut norm_b = hsum_ps_256(norm_b_sum);

        while i < n {
            let va = *a_ptr.add(i);
            let vb = *b_ptr.add(i);
            dot += va * vb;
            norm_a += va * va;
            norm_b += vb * vb;
            i += 1;
        }

        (dot, norm_a, norm_b)
    }

    /// AVX2 implementation of L1 distance (Manhattan distance).
    #[target_feature(enable = "avx2")]
    pub unsafe fn l1_distance_avx2(a: &[u8], b: &[u8]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;
        let b_ptr = b.as_ptr() as *const f32;

        // Sign bit mask for absolute value: clear the sign bit
        let sign_mask = _mm256_set1_ps(-0.0f32);

        let mut sum = _mm256_setzero_ps();
        let mut i = 0;

        while i + 8 <= n {
            let va = _mm256_loadu_ps(a_ptr.add(i));
            let vb = _mm256_loadu_ps(b_ptr.add(i));
            let diff = _mm256_sub_ps(va, vb);
            // Absolute value: clear sign bit using andnot
            let abs_diff = _mm256_andnot_ps(sign_mask, diff);
            sum = _mm256_add_ps(sum, abs_diff);
            i += 8;
        }

        let mut result = hsum_ps_256(sum);
        while i < n {
            let diff = *a_ptr.add(i) - *b_ptr.add(i);
            result += diff.abs();
            i += 1;
        }

        result
    }

    /// AVX2 implementation of L2 norm using double precision for accuracy.
    #[target_feature(enable = "avx2", enable = "fma")]
    pub unsafe fn l2_norm_avx2(a: &[u8]) -> f64 {
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;

        let mut sum = _mm256_setzero_pd();
        let mut i = 0;

        // Process 4 elements at a time (convert f32 to f64)
        while i + 4 <= n {
            let va = _mm_loadu_ps(a_ptr.add(i));
            let va_pd = _mm256_cvtps_pd(va);
            sum = _mm256_fmadd_pd(va_pd, va_pd, sum);
            i += 4;
        }

        let mut result = hsum_pd_256(sum);

        while i < n {
            let v = *a_ptr.add(i) as f64;
            result += v * v;
            i += 1;
        }

        result
    }
}

// ============================================================================
// AVX-512 implementations
// ============================================================================

#[cfg(target_arch = "x86_64")]
mod avx512 {
    use super::*;

    /// Horizontal sum of an __m512 register (16 x f32 -> f32)
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn hsum_ps_512(v: __m512) -> f32 {
        // Reduce 512 -> 256 -> 128 -> scalar
        let low = _mm512_castps512_ps256(v);
        let high = _mm512_extractf32x8_ps(v, 1);
        let sum256 = _mm256_add_ps(low, high);

        // Now reduce 256-bit to scalar
        let high128 = _mm256_extractf128_ps(sum256, 1);
        let low128 = _mm256_castps256_ps128(sum256);
        let sum128 = _mm_add_ps(low128, high128);
        let shuf = _mm_movehdup_ps(sum128);
        let sums = _mm_add_ps(sum128, shuf);
        let shuf = _mm_movehl_ps(sums, sums);
        let sums = _mm_add_ss(sums, shuf);
        _mm_cvtss_f32(sums)
    }

    /// Horizontal sum of an __m512d register (8 x f64 -> f64)
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn hsum_pd_512(v: __m512d) -> f64 {
        let low = _mm512_castpd512_pd256(v);
        let high = _mm512_extractf64x4_pd(v, 1);
        let sum256 = _mm256_add_pd(low, high);

        let high128 = _mm256_extractf128_pd(sum256, 1);
        let low128 = _mm256_castpd256_pd128(sum256);
        let sum128 = _mm_add_pd(low128, high128);
        let high64 = _mm_unpackhi_pd(sum128, sum128);
        let sum = _mm_add_sd(sum128, high64);
        _mm_cvtsd_f64(sum)
    }

    /// AVX-512 implementation of L2 squared distance.
    /// Processes 16 f32 elements per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn l2_squared_distance_avx512(a: &[u8], b: &[u8]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;
        let b_ptr = b.as_ptr() as *const f32;

        let mut sum = _mm512_setzero_ps();
        let mut i = 0;

        while i + 16 <= n {
            let va = _mm512_loadu_ps(a_ptr.add(i));
            let vb = _mm512_loadu_ps(b_ptr.add(i));
            let diff = _mm512_sub_ps(va, vb);
            sum = _mm512_fmadd_ps(diff, diff, sum);
            i += 16;
        }

        let mut result = hsum_ps_512(sum);

        // Handle remaining elements (less than 16)
        while i < n {
            let diff = *a_ptr.add(i) - *b_ptr.add(i);
            result += diff * diff;
            i += 1;
        }

        result
    }

    /// AVX-512 implementation of inner product.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn inner_product_avx512(a: &[u8], b: &[u8]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;
        let b_ptr = b.as_ptr() as *const f32;

        let mut sum = _mm512_setzero_ps();
        let mut i = 0;

        while i + 16 <= n {
            let va = _mm512_loadu_ps(a_ptr.add(i));
            let vb = _mm512_loadu_ps(b_ptr.add(i));
            sum = _mm512_fmadd_ps(va, vb, sum);
            i += 16;
        }

        let mut result = hsum_ps_512(sum);

        while i < n {
            result += *a_ptr.add(i) * *b_ptr.add(i);
            i += 1;
        }

        result
    }

    /// AVX-512 implementation of cosine distance.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn cosine_distance_avx512(a: &[u8], b: &[u8]) -> (f32, f32, f32) {
        debug_assert_eq!(a.len(), b.len());
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;
        let b_ptr = b.as_ptr() as *const f32;

        let mut dot_sum = _mm512_setzero_ps();
        let mut norm_a_sum = _mm512_setzero_ps();
        let mut norm_b_sum = _mm512_setzero_ps();
        let mut i = 0;

        while i + 16 <= n {
            let va = _mm512_loadu_ps(a_ptr.add(i));
            let vb = _mm512_loadu_ps(b_ptr.add(i));
            dot_sum = _mm512_fmadd_ps(va, vb, dot_sum);
            norm_a_sum = _mm512_fmadd_ps(va, va, norm_a_sum);
            norm_b_sum = _mm512_fmadd_ps(vb, vb, norm_b_sum);
            i += 16;
        }

        let mut dot = hsum_ps_512(dot_sum);
        let mut norm_a = hsum_ps_512(norm_a_sum);
        let mut norm_b = hsum_ps_512(norm_b_sum);

        while i < n {
            let va = *a_ptr.add(i);
            let vb = *b_ptr.add(i);
            dot += va * vb;
            norm_a += va * va;
            norm_b += vb * vb;
            i += 1;
        }

        (dot, norm_a, norm_b)
    }

    /// AVX-512 implementation of L1 distance.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn l1_distance_avx512(a: &[u8], b: &[u8]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;
        let b_ptr = b.as_ptr() as *const f32;

        let mut sum = _mm512_setzero_ps();
        let mut i = 0;

        while i + 16 <= n {
            let va = _mm512_loadu_ps(a_ptr.add(i));
            let vb = _mm512_loadu_ps(b_ptr.add(i));
            let diff = _mm512_sub_ps(va, vb);
            let abs_diff = _mm512_abs_ps(diff);
            sum = _mm512_add_ps(sum, abs_diff);
            i += 16;
        }

        let mut result = hsum_ps_512(sum);

        while i < n {
            let diff = *a_ptr.add(i) - *b_ptr.add(i);
            result += diff.abs();
            i += 1;
        }

        result
    }

    /// AVX-512 implementation of L2 norm.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn l2_norm_avx512(a: &[u8]) -> f64 {
        debug_assert_eq!(a.len() % 4, 0);

        let n = a.len() / 4;
        let a_ptr = a.as_ptr() as *const f32;

        let mut sum = _mm512_setzero_pd();
        let mut i = 0;

        // Process 8 elements at a time (convert f32 to f64)
        while i + 8 <= n {
            let va = _mm256_loadu_ps(a_ptr.add(i));
            let va_pd = _mm512_cvtps_pd(va);
            sum = _mm512_fmadd_pd(va_pd, va_pd, sum);
            i += 8;
        }

        let mut result = hsum_pd_512(sum);

        while i < n {
            let v = *a_ptr.add(i) as f64;
            result += v * v;
            i += 1;
        }

        result
    }
}

// ============================================================================
// Public dispatch functions
// ============================================================================

/// Compute L2 squared distance using the best available SIMD implementation.
#[inline]
pub fn l2_squared_distance(a: &[u8], b: &[u8]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match simd_level() {
            SimdLevel::Avx512 => unsafe { avx512::l2_squared_distance_avx512(a, b) },
            SimdLevel::Avx2 => unsafe { avx2::l2_squared_distance_avx2(a, b) },
            SimdLevel::Scalar => l2_squared_distance_scalar(a, b),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        l2_squared_distance_scalar(a, b)
    }
}

/// Compute inner product using the best available SIMD implementation.
#[inline]
pub fn inner_product(a: &[u8], b: &[u8]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match simd_level() {
            SimdLevel::Avx512 => unsafe { avx512::inner_product_avx512(a, b) },
            SimdLevel::Avx2 => unsafe { avx2::inner_product_avx2(a, b) },
            SimdLevel::Scalar => inner_product_scalar(a, b),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        inner_product_scalar(a, b)
    }
}

/// Compute cosine distance components using the best available SIMD implementation.
/// Returns (dot_product, norm_a_squared, norm_b_squared).
#[inline]
pub fn cosine_distance_components(a: &[u8], b: &[u8]) -> (f32, f32, f32) {
    #[cfg(target_arch = "x86_64")]
    {
        match simd_level() {
            SimdLevel::Avx512 => unsafe { avx512::cosine_distance_avx512(a, b) },
            SimdLevel::Avx2 => unsafe { avx2::cosine_distance_avx2(a, b) },
            SimdLevel::Scalar => cosine_distance_components_scalar(a, b),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        cosine_distance_components_scalar(a, b)
    }
}

/// Compute L1 distance using the best available SIMD implementation.
#[inline]
pub fn l1_distance(a: &[u8], b: &[u8]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match simd_level() {
            SimdLevel::Avx512 => unsafe { avx512::l1_distance_avx512(a, b) },
            SimdLevel::Avx2 => unsafe { avx2::l1_distance_avx2(a, b) },
            SimdLevel::Scalar => l1_distance_scalar(a, b),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        l1_distance_scalar(a, b)
    }
}

/// Compute L2 norm using the best available SIMD implementation.
#[inline]
pub fn l2_norm_squared(a: &[u8]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        match simd_level() {
            SimdLevel::Avx512 => unsafe { avx512::l2_norm_avx512(a) },
            SimdLevel::Avx2 => unsafe { avx2::l2_norm_avx2(a) },
            SimdLevel::Scalar => l2_norm_squared_scalar(a),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        l2_norm_squared_scalar(a)
    }
}

// ============================================================================
// Scalar fallback implementations
// ============================================================================

#[inline]
fn l2_squared_distance_scalar(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len() % 4, 0);

    let n = a.len() / 4;
    let a_ptr = a.as_ptr() as *const f32;
    let b_ptr = b.as_ptr() as *const f32;

    let mut sum: f32 = 0.0;
    for i in 0..n {
        let diff = unsafe { *a_ptr.add(i) - *b_ptr.add(i) };
        sum += diff * diff;
    }
    sum
}

#[inline]
fn inner_product_scalar(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len() % 4, 0);

    let n = a.len() / 4;
    let a_ptr = a.as_ptr() as *const f32;
    let b_ptr = b.as_ptr() as *const f32;

    let mut sum: f32 = 0.0;
    for i in 0..n {
        sum += unsafe { *a_ptr.add(i) * *b_ptr.add(i) };
    }
    sum
}

#[inline]
fn cosine_distance_components_scalar(a: &[u8], b: &[u8]) -> (f32, f32, f32) {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len() % 4, 0);

    let n = a.len() / 4;
    let a_ptr = a.as_ptr() as *const f32;
    let b_ptr = b.as_ptr() as *const f32;

    let mut dot: f32 = 0.0;
    let mut norm_a: f32 = 0.0;
    let mut norm_b: f32 = 0.0;

    for i in 0..n {
        let va = unsafe { *a_ptr.add(i) };
        let vb = unsafe { *b_ptr.add(i) };
        dot += va * vb;
        norm_a += va * va;
        norm_b += vb * vb;
    }

    (dot, norm_a, norm_b)
}

#[inline]
fn l1_distance_scalar(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len() % 4, 0);

    let n = a.len() / 4;
    let a_ptr = a.as_ptr() as *const f32;
    let b_ptr = b.as_ptr() as *const f32;

    let mut sum: f32 = 0.0;
    for i in 0..n {
        let diff = unsafe { *a_ptr.add(i) - *b_ptr.add(i) };
        sum += diff.abs();
    }
    sum
}

#[inline]
fn l2_norm_squared_scalar(a: &[u8]) -> f64 {
    debug_assert_eq!(a.len() % 4, 0);

    let n = a.len() / 4;
    let a_ptr = a.as_ptr() as *const f32;

    let mut sum: f64 = 0.0;
    for i in 0..n {
        let v = unsafe { *a_ptr.add(i) as f64 };
        sum += v * v;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_data(n: usize) -> (Vec<u8>, Vec<u8>) {
        let a: Vec<f32> = (0..n).map(|i| (i as f32) * 0.1 + 1.0).collect();
        let b: Vec<f32> = (0..n).map(|i| (i as f32) * 0.2 + 0.5).collect();
        let a_bytes: Vec<u8> = bytemuck::cast_slice(&a).to_vec();
        let b_bytes: Vec<u8> = bytemuck::cast_slice(&b).to_vec();
        (a_bytes, b_bytes)
    }

    /// Check if two f32 values are approximately equal using relative error.
    fn approx_eq_f32(a: f32, b: f32, rel_eps: f32) -> bool {
        let diff = (a - b).abs();
        let max_val = a.abs().max(b.abs()).max(1.0);
        diff / max_val < rel_eps
    }

    /// Check if two f64 values are approximately equal using relative error.
    fn approx_eq_f64(a: f64, b: f64, rel_eps: f64) -> bool {
        let diff = (a - b).abs();
        let max_val = a.abs().max(b.abs()).max(1.0);
        diff / max_val < rel_eps
    }

    #[test]
    fn test_simd_level_detection() {
        let level = simd_level();
        println!("Detected SIMD level: {:?}", level);
        // Just ensure detection doesn't panic
    }

    #[test]
    fn test_l2_squared_distance_consistency() {
        // Use relative error tolerance of 1e-5 (0.001%)
        const REL_EPS: f32 = 1e-5;
        for n in [3, 7, 8, 15, 16, 31, 32, 64, 128, 256] {
            let (a, b) = make_test_data(n);
            let scalar_result = l2_squared_distance_scalar(&a, &b);
            let simd_result = l2_squared_distance(&a, &b);
            assert!(
                approx_eq_f32(scalar_result, simd_result, REL_EPS),
                "L2 squared distance mismatch for n={}: scalar={}, simd={}",
                n,
                scalar_result,
                simd_result
            );
        }
    }

    #[test]
    fn test_inner_product_consistency() {
        const REL_EPS: f32 = 1e-5;
        for n in [3, 7, 8, 15, 16, 31, 32, 64, 128, 256] {
            let (a, b) = make_test_data(n);
            let scalar_result = inner_product_scalar(&a, &b);
            let simd_result = inner_product(&a, &b);
            assert!(
                approx_eq_f32(scalar_result, simd_result, REL_EPS),
                "Inner product mismatch for n={}: scalar={}, simd={}",
                n,
                scalar_result,
                simd_result
            );
        }
    }

    #[test]
    fn test_cosine_distance_components_consistency() {
        const REL_EPS: f32 = 1e-5;
        for n in [3, 7, 8, 15, 16, 31, 32, 64, 128, 256] {
            let (a, b) = make_test_data(n);
            let (s_dot, s_na, s_nb) = cosine_distance_components_scalar(&a, &b);
            let (simd_dot, simd_na, simd_nb) = cosine_distance_components(&a, &b);
            assert!(
                approx_eq_f32(s_dot, simd_dot, REL_EPS),
                "Cosine dot mismatch for n={}: scalar={}, simd={}",
                n,
                s_dot,
                simd_dot
            );
            assert!(
                approx_eq_f32(s_na, simd_na, REL_EPS),
                "Cosine norm_a mismatch for n={}: scalar={}, simd={}",
                n,
                s_na,
                simd_na
            );
            assert!(
                approx_eq_f32(s_nb, simd_nb, REL_EPS),
                "Cosine norm_b mismatch for n={}: scalar={}, simd={}",
                n,
                s_nb,
                simd_nb
            );
        }
    }

    #[test]
    fn test_l1_distance_consistency() {
        const REL_EPS: f32 = 1e-5;
        for n in [3, 7, 8, 15, 16, 31, 32, 64, 128, 256] {
            let (a, b) = make_test_data(n);
            let scalar_result = l1_distance_scalar(&a, &b);
            let simd_result = l1_distance(&a, &b);
            assert!(
                approx_eq_f32(scalar_result, simd_result, REL_EPS),
                "L1 distance mismatch for n={}: scalar={}, simd={}",
                n,
                scalar_result,
                simd_result
            );
        }
    }

    #[test]
    fn test_l2_norm_squared_consistency() {
        const REL_EPS: f64 = 1e-10;
        for n in [3, 7, 8, 15, 16, 31, 32, 64, 128, 256] {
            let (a, _) = make_test_data(n);
            let scalar_result = l2_norm_squared_scalar(&a);
            let simd_result = l2_norm_squared(&a);
            assert!(
                approx_eq_f64(scalar_result, simd_result, REL_EPS),
                "L2 norm squared mismatch for n={}: scalar={}, simd={}",
                n,
                scalar_result,
                simd_result
            );
        }
    }

    #[test]
    fn test_empty_vectors() {
        let empty: Vec<u8> = vec![];
        assert_eq!(l2_squared_distance(&empty, &empty), 0.0);
        assert_eq!(inner_product(&empty, &empty), 0.0);
        assert_eq!(l1_distance(&empty, &empty), 0.0);
        assert_eq!(l2_norm_squared(&empty), 0.0);
        let (dot, na, nb) = cosine_distance_components(&empty, &empty);
        assert_eq!(dot, 0.0);
        assert_eq!(na, 0.0);
        assert_eq!(nb, 0.0);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_avx2_directly() {
        if !is_x86_feature_detected!("avx2") || !is_x86_feature_detected!("fma") {
            println!("AVX2/FMA not available, skipping direct AVX2 test");
            return;
        }

        const REL_EPS: f32 = 1e-5;
        for n in [8, 16, 32, 64, 128] {
            let (a, b) = make_test_data(n);
            let scalar = l2_squared_distance_scalar(&a, &b);
            let avx2 = unsafe { avx2::l2_squared_distance_avx2(&a, &b) };
            assert!(
                approx_eq_f32(scalar, avx2, REL_EPS),
                "AVX2 L2 squared mismatch for n={}: scalar={}, avx2={}",
                n,
                scalar,
                avx2
            );
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_avx512_directly() {
        if !is_x86_feature_detected!("avx512f") {
            println!("AVX-512F not available, skipping direct AVX-512 test");
            return;
        }

        const REL_EPS: f32 = 1e-5;
        for n in [16, 32, 64, 128] {
            let (a, b) = make_test_data(n);
            let scalar = l2_squared_distance_scalar(&a, &b);
            let avx512 = unsafe { avx512::l2_squared_distance_avx512(&a, &b) };
            assert!(
                approx_eq_f32(scalar, avx512, REL_EPS),
                "AVX-512 L2 squared mismatch for n={}: scalar={}, avx512={}",
                n,
                scalar,
                avx512
            );
        }
    }
}
