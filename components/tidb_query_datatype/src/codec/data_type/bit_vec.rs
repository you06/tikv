// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

/// A boolean vector, which consolidates 64 booleans into 1 u64 to save space.
///
/// `BitVec` is mainly used to implement bitmap in ChunkedVec.
#[derive(Debug, PartialEq, Clone)]
pub struct BitVec {
    data: Vec<u64>,
    length: usize,
}
const BITS: usize = 64;
impl BitVec {
    fn upper_bound(size: usize) -> usize {
        (size + BITS - 1) >> 6
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(Self::upper_bound(capacity)),
            length: 0,
        }
    }

    #[inline]
    pub fn push(&mut self, value: bool) {
        let idx = self.length >> 6;
        if idx >= self.data.len() {
            self.data.push(0);
        }

        let mask = (1_u64) << (self.length & (BITS - 1));
        self.length += 1;
        if value {
            self.data[idx] |= mask;
        } else {
            self.data[idx] &= !mask;
        }
    }

    pub fn replace(&mut self, idx: usize, value: bool) {
        assert!(idx < self.length);
        let mask = (1_u64) << (idx & (BITS - 1));
        let pos = idx >> 6;
        if value {
            self.data[pos] |= mask;
        } else {
            self.data[pos] &= !mask;
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.length {
            self.length = len;
            self.data.truncate(Self::upper_bound(len));
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.len() << 6
    }

    pub fn append(&mut self, other: &mut Self) {
        for i in 0..other.len() {
            self.push(other.get(i));
        }
        other.truncate(0);
    }

    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        assert!(idx < self.length);
        let mask = (1_u64) << (idx & (BITS - 1));
        let pos = idx >> 6;
        (self.data[pos] & mask) != 0
    }

    /// Counts the number of 1 bits in the range [start, end).
    ///
    /// This method is optimized using the hardware POPCNT instruction via
    /// Rust's `count_ones()`, which provides significant speedup for batch
    /// NULL counting in aggregation functions like COUNT.
    ///
    /// # Panics
    ///
    /// Panics if `start > end` or `end > self.length`.
    #[inline]
    pub fn count_ones_range(&self, start: usize, end: usize) -> usize {
        assert!(start <= end);
        assert!(end <= self.length);

        if start == end {
            return 0;
        }

        let start_word = start >> 6;
        let start_bit = start & 63;
        let end_word = end >> 6;
        let end_bit = end & 63;

        if start_word == end_word {
            // All bits are in the same u64 word.
            // Create a mask for bits [start_bit, end_bit).
            let mask = ((1u64 << end_bit) - 1) & !((1u64 << start_bit) - 1);
            return (self.data[start_word] & mask).count_ones() as usize;
        }

        let mut count = 0usize;

        // First partial word: bits [start_bit, 64)
        if start_bit == 0 {
            count += self.data[start_word].count_ones() as usize;
        } else {
            let mask = !((1u64 << start_bit) - 1);
            count += (self.data[start_word] & mask).count_ones() as usize;
        }

        // Full words in the middle: [start_word + 1, end_word)
        for word in &self.data[start_word + 1..end_word] {
            count += word.count_ones() as usize;
        }

        // Last partial word: bits [0, end_bit)
        if end_bit != 0 {
            let mask = (1u64 << end_bit) - 1;
            count += (self.data[end_word] & mask).count_ones() as usize;
        }

        count
    }
}

pub struct BitAndIterator<'a> {
    vecs: &'a [&'a BitVec],
    or: u64,
    cnt: usize,
    output_rows: usize,
}

impl<'a> BitAndIterator<'a> {
    pub fn new(vecs: &'a [&'a BitVec], output_rows: usize) -> Self {
        for i in vecs {
            if i.len() != output_rows {
                panic!("column length doesn't match");
            }
        }
        Self {
            vecs,
            or: 0,
            cnt: 0,
            output_rows,
        }
    }
}

impl Iterator for BitAndIterator<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cnt == self.output_rows {
            return None;
        }
        if self.cnt % BITS == 0 {
            let mut result: u64 = 0xffffffffffffffff;
            let idx = self.cnt / BITS;
            for i in self.vecs {
                result &= i.data[idx];
            }
            self.or = result;
        }
        self.cnt += 1;
        let val = self.or & 0x1 == 1;
        self.or >>= 1;
        Some(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_capacity() {
        BitVec::with_capacity(0);
        BitVec::with_capacity(8);
        BitVec::with_capacity(233);
    }

    #[test]
    fn test_len_is_empty() {
        assert_eq!(BitVec::with_capacity(0).len(), 0);
        assert_eq!(BitVec::with_capacity(8).len(), 0);
        assert_eq!(BitVec::with_capacity(233).len(), 0);
        assert!(BitVec::with_capacity(0).is_empty());
        assert!(BitVec::with_capacity(8).is_empty());
        assert!(BitVec::with_capacity(233).is_empty());
        let mut x = BitVec::with_capacity(233);
        x.push(false);
        assert_eq!(x.len(), 1);
        assert!(!x.is_empty());
        x.push(true);
        assert_eq!(x.len(), 2);
        assert!(!x.is_empty());
        for _ in 0..2333 {
            x.push(false);
        }
        assert_eq!(x.len(), 2 + 2333);
        assert!(!x.is_empty());
        for i in 0..2333 {
            x.replace(i, true);
        }
        assert_eq!(x.len(), 2 + 2333);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_push() {
        let mut x = BitVec::with_capacity(0);
        x.push(false);
        x.push(true);
        assert_eq!(x.get(0), false);
        assert_eq!(x.get(1), true);
    }

    #[test]
    fn test_push_all_combinations() {
        let mut x = BitVec::with_capacity(0);
        for i in 0..256 {
            for bit in 0..8 {
                x.push(i & (1 << bit) != 0);
            }
            for bit in 0..8 {
                assert_eq!(x.get(i * 8 + bit), i & (1 << bit) != 0);
            }
        }
    }

    #[test]
    fn test_push_on_edge() {
        let mut x = BitVec::with_capacity(0);
        let mut base = 0;
        for _ in 0..8 {
            for i in 0..256 {
                for bit in 0..8 {
                    x.push(i & (1 << bit) != 0);
                }
                for bit in 0..8 {
                    assert_eq!(x.get(base + i * 8 + bit), i & (1 << bit) != 0);
                }
            }
            x.push(false); // try to mis-align boolean values on 8 bound
            base += 256 * 8 + 1;
        }
    }

    #[test]
    fn test_replace() {
        let mut x = BitVec::with_capacity(0);
        x.push(false);
        x.push(true);
        assert!(!x.get(0));
        assert!(x.get(1));
        x.replace(0, true);
        assert!(x.get(0));
        assert!(x.get(1));
        x.replace(0, false);
        assert!(!x.get(0));
        assert!(x.get(1));
    }

    #[test]
    fn test_replace_all_combinations() {
        let mut x = BitVec::with_capacity(0);
        for i in 0..256 {
            for bit in 0..8 {
                x.push(i & (1 << bit) != 0);
            }
            x.replace(i * 8, false);
            for bit in 1..8 {
                assert_eq!(x.get(i * 8 + bit), i & (1 << bit) != 0);
            }
            assert!(!x.get(i * 8));
        }
    }

    #[test]
    fn test_append() {
        let mut x = BitVec::with_capacity(0);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        let mut y = BitVec::with_capacity(0);
        y.push(false);
        y.push(true);
        y.push(false);
        y.push(true);
        y.push(false);
        y.push(true);
        y.push(false);
        y.push(true);
        y.push(false);
        x.append(&mut y);
        for i in 0..16 {
            assert_eq!(x.get(i), i % 2 == 0);
        }
        assert_eq!(x.len(), 16);
        assert_eq!(y.len(), 0);
    }

    #[test]
    fn test_truncate() {
        let mut x = BitVec::with_capacity(0);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        x.push(false);
        x.push(true);
        x.truncate(4);
        assert_eq!(x.len(), 4);
        x.truncate(100);
        assert_eq!(x.len(), 4);
        x.truncate(0);
        assert_eq!(x.len(), 0);
    }

    #[test]
    fn test_bit_and_iterator_empty() {
        let mut cnt = 0;
        for i in BitAndIterator::new(&[], 2333) {
            assert!(i);
            cnt += 1;
        }
        assert_eq!(cnt, 2333);
    }

    #[test]
    fn test_bit_and_iterator() {
        let mut cnt = 0;
        let mut vec1 = BitVec::with_capacity(0);
        let mut vec2 = BitVec::with_capacity(0);
        let mut vec3 = BitVec::with_capacity(0);
        let mut vec5 = BitVec::with_capacity(0);
        let size = 2333;
        for i in 0..size {
            vec1.push(true);
            vec2.push(i % 2 == 0);
            vec3.push(i % 3 == 0);
            vec5.push(i % 5 == 0);
        }
        for (idx, i) in BitAndIterator::new(&[&vec1, &vec2, &vec3, &vec5], size).enumerate() {
            assert_eq!(i, idx % 30 == 0);
            cnt += 1;
        }
        assert_eq!(cnt, size);
    }

    #[test]
    fn test_count_ones_range_empty() {
        let x = BitVec::with_capacity(0);
        // Empty range on empty vec should return 0 (start == end == 0)
        assert_eq!(x.count_ones_range(0, 0), 0);
    }

    #[test]
    fn test_count_ones_range_single_word() {
        let mut x = BitVec::with_capacity(64);
        // Create pattern: true, false, true, false, ...
        for i in 0..32 {
            x.push(i % 2 == 0);
        }
        // Bits 0, 2, 4, 6, ... 30 are true (16 ones)
        assert_eq!(x.count_ones_range(0, 32), 16);
        // Empty range
        assert_eq!(x.count_ones_range(5, 5), 0);
        // Single element ranges
        assert_eq!(x.count_ones_range(0, 1), 1); // bit 0 is true
        assert_eq!(x.count_ones_range(1, 2), 0); // bit 1 is false
        assert_eq!(x.count_ones_range(2, 3), 1); // bit 2 is true
        // Partial ranges within single word
        assert_eq!(x.count_ones_range(0, 8), 4); // bits 0,2,4,6 are true
        assert_eq!(x.count_ones_range(1, 9), 4); // bits 2,4,6,8 are true
        assert_eq!(x.count_ones_range(10, 20), 5); // bits 10,12,14,16,18 are true
    }

    #[test]
    fn test_count_ones_range_multiple_words() {
        let mut x = BitVec::with_capacity(200);
        // Create 200 bits with pattern: true, false, true, false, ...
        for i in 0..200 {
            x.push(i % 2 == 0);
        }
        // Full range
        assert_eq!(x.count_ones_range(0, 200), 100);
        // First word only (bits 0-63)
        assert_eq!(x.count_ones_range(0, 64), 32);
        // Second word only (bits 64-127)
        assert_eq!(x.count_ones_range(64, 128), 32);
        // Across word boundary
        assert_eq!(x.count_ones_range(60, 70), 5); // bits 60,62,64,66,68 are true
        // Multiple complete words
        assert_eq!(x.count_ones_range(0, 128), 64);
        // Partial start, full middle, partial end
        assert_eq!(x.count_ones_range(10, 150), 70); // (64-10)/2 + 32 + (150-128)/2 = 27 + 32 + 11 = 70
    }

    #[test]
    fn test_count_ones_range_all_ones() {
        let mut x = BitVec::with_capacity(256);
        for _ in 0..256 {
            x.push(true);
        }
        assert_eq!(x.count_ones_range(0, 256), 256);
        assert_eq!(x.count_ones_range(0, 64), 64);
        assert_eq!(x.count_ones_range(64, 128), 64);
        assert_eq!(x.count_ones_range(10, 250), 240);
    }

    #[test]
    fn test_count_ones_range_all_zeros() {
        let mut x = BitVec::with_capacity(256);
        for _ in 0..256 {
            x.push(false);
        }
        assert_eq!(x.count_ones_range(0, 256), 0);
        assert_eq!(x.count_ones_range(0, 64), 0);
        assert_eq!(x.count_ones_range(64, 128), 0);
        assert_eq!(x.count_ones_range(10, 250), 0);
    }

    #[test]
    fn test_count_ones_range_word_boundary() {
        let mut x = BitVec::with_capacity(128);
        for i in 0..128 {
            // Set bits 63 and 64 to true, rest false
            x.push(i == 63 || i == 64);
        }
        assert_eq!(x.count_ones_range(0, 128), 2);
        assert_eq!(x.count_ones_range(0, 64), 1);  // bit 63
        assert_eq!(x.count_ones_range(64, 128), 1); // bit 64
        assert_eq!(x.count_ones_range(63, 65), 2);  // bits 63 and 64
        assert_eq!(x.count_ones_range(62, 66), 2);
        assert_eq!(x.count_ones_range(0, 63), 0);
        assert_eq!(x.count_ones_range(65, 128), 0);
    }

    #[test]
    fn test_count_ones_range_verify_against_naive() {
        // Verify count_ones_range matches naive counting for various patterns
        let mut x = BitVec::with_capacity(500);
        for i in 0..500 {
            x.push(i % 3 == 0 || i % 7 == 0);
        }

        // Test various ranges
        let ranges = [
            (0, 0),
            (0, 1),
            (0, 64),
            (0, 65),
            (0, 128),
            (0, 500),
            (1, 64),
            (63, 65),
            (63, 129),
            (100, 400),
            (127, 384),
            (64, 64),
            (64, 65),
        ];

        for (start, end) in ranges {
            let expected: usize = (start..end).filter(|&i| x.get(i)).count();
            assert_eq!(
                x.count_ones_range(start, end),
                expected,
                "Mismatch for range [{}, {})",
                start,
                end
            );
        }
    }
}
