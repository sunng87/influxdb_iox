use arrow::buffer::Buffer;

/// An arrow-compatible mutable bitset implementation
///
/// Note: This currently operates on individual bytes at a time
/// it could be optimised to instead operate on usize blocks
#[derive(Debug, Default)]
pub struct BitSet {
    /// The underlying data
    ///
    /// Data is stored in the least significant bit of a byte first
    buffer: Vec<u8>,

    /// The length of this mask in bits
    len: usize,
}

impl BitSet {
    /// Creates a new BitSet
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new BitSet with `len` unset bits
    pub fn new_with_unset(len: usize) -> Self {
        let mut ret = Self::new();
        ret.append_unset(len);
        ret
    }

    /// Appends `count` unset bits
    pub fn append_unset(&mut self, count: usize) {
        self.len += count;
        let new_buf_len = (self.len + 7) >> 3;
        self.buffer.resize(new_buf_len, 0);
    }

    /// Appends `count` boolean values from the slice of packed bits
    pub fn append_bits(&mut self, count: usize, to_set: &[u8]) {
        let new_len = self.len + count;
        let new_buf_len = (new_len + 7) >> 3;
        self.buffer.reserve(new_buf_len - self.buffer.len());

        let whole_bytes = count >> 3;
        let overrun = count & 7;

        let skew = self.len & 7;
        if skew == 0 {
            self.buffer.extend_from_slice(&to_set[..whole_bytes]);
            if overrun > 0 {
                let masked = to_set[whole_bytes] & ((1 << overrun) - 1);
                self.buffer.push(masked)
            }

            self.len = new_len;
            debug_assert_eq!(self.buffer.len(), new_buf_len);
            return;
        }

        for to_set_byte in &to_set[..whole_bytes] {
            let low = *to_set_byte << skew;
            let high = *to_set_byte >> (8 - skew);

            *self.buffer.last_mut().unwrap() |= low;
            self.buffer.push(high);
        }

        if overrun > 0 {
            let masked = to_set[whole_bytes] & ((1 << overrun) - 1);
            let low = masked << skew;
            *self.buffer.last_mut().unwrap() |= low;

            if overrun > 8 - skew {
                let high = masked >> (8 - skew);
                self.buffer.push(high)
            }
        }

        self.len = new_len;
        debug_assert_eq!(self.buffer.len(), new_buf_len);
    }

    /// Sets a given bit
    pub fn set(&mut self, idx: usize) {
        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        self.buffer[byte_idx] |= 1 << bit_idx;
    }

    /// Returns if the given index is set
    pub fn get(&self, idx: usize) -> bool {
        is_bit_set(idx, &self.buffer)
    }

    /// Append a boolean value
    pub fn push(&mut self, set: bool) {
        self.append_unset(1);
        if set {
            self.set(self.len - 1)
        }
    }

    /// Returns the number of set bits
    pub fn count_set(&self) -> usize {
        count_set_bits(&self.buffer)
    }

    /// Returns the number of unset bits
    pub fn count_unset(&self) -> usize {
        let total: usize = self.buffer.iter().map(|x| x.count_zeros() as usize).sum();
        total + (self.len & 7) - 8
    }

    /// Returns a reference to the compacted array
    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    /// Takes the underlying buffer out of this bitset
    pub fn take_bytes(self) -> Vec<u8> {
        self.buffer
    }

    /// Converts this BitSet to a buffer compatible with arrows boolean encoding
    pub fn to_arrow(&self) -> Buffer {
        Buffer::from(&self.buffer)
    }

    /// Returns the number of values stored in the bitset
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns if this bitset is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of bytes used by this bitset
    pub fn byte_len(&self) -> usize {
        self.buffer.len()
    }
}

#[inline]
pub fn negate_mask(buffer: &mut [u8], row_count: usize) {
    let whole_bytes = row_count >> 3;
    for i in 0..whole_bytes {
        buffer[i] = !buffer[i]
    }
    let remainder = row_count & 7;
    if remainder != 0 {
        buffer[whole_bytes] = negate_bits(buffer[whole_bytes], remainder)
    }
}

#[inline]
pub fn negate_bits(byte: u8, bits: usize) -> u8 {
    let bits = bits & 7;
    let mask = (1 << bits) - 1;
    (!byte & mask) | (byte & !mask)
}

#[inline]
pub fn count_set_bits(buffer: &[u8]) -> usize {
    buffer.iter().map(|x| x.count_ones() as usize).sum()
}

#[inline]
pub fn is_bit_set(idx: usize, buffer: &[u8]) -> bool {
    let byte_idx = idx >> 3;
    let bit_idx = idx & 7;
    (buffer[byte_idx] >> bit_idx) & 1 != 0
}

/// Returns an iterator over all the bits in a packed mask
pub fn iter_bits(bytes: &[u8], row_count: usize) -> impl Iterator<Item = bool> + '_ {
    debug_assert!(((row_count + 7) >> 3) >= bytes.len());

    let mut byte_idx = 0;
    let mut bit_idx = 0;
    let mut count = 0;
    std::iter::from_fn(move || {
        if count == row_count {
            return None;
        }

        let ret = bytes[byte_idx] >> bit_idx & 1 != 0;
        if bit_idx == 7 {
            byte_idx += 1;
            bit_idx = 0;
        } else {
            bit_idx += 1;
        }
        count += 1;

        return Some(ret);
    })
}

/// Returns an iterator over set bit positions in increasing order
pub fn iter_set_positions(bytes: &[u8]) -> impl Iterator<Item = usize> + '_ {
    let mut byte_idx = 0;
    let mut in_progress = bytes.get(0).cloned().unwrap_or(0);
    std::iter::from_fn(move || loop {
        if in_progress != 0 {
            let bit_pos = in_progress.trailing_zeros();
            in_progress ^= 1 << bit_pos;
            return Some((byte_idx << 3) + (bit_pos as usize));
        }
        byte_idx += 1;
        in_progress = *bytes.get(byte_idx)?;
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BooleanBufferBuilder;
    use rand::RngCore;

    /// Computes a compacted representation of a given bool array
    fn compact_bools(bools: &[bool]) -> Vec<u8> {
        bools
            .chunks(8)
            .map(|x| {
                let mut collect = 0_u8;
                for (idx, set) in x.iter().enumerate() {
                    if *set {
                        collect |= 1 << idx
                    }
                }
                collect
            })
            .collect()
    }

    fn iter_set_bools(bools: &[bool]) -> impl Iterator<Item = usize> + '_ {
        bools.iter().enumerate().filter_map(|(x, y)| y.then(|| x))
    }

    #[test]
    fn test_compact_bools() {
        let bools = &[
            false, false, true, true, false, false, true, false, true, false,
        ];
        let collected = compact_bools(bools);
        let indexes: Vec<_> = iter_set_bools(bools).collect();
        assert_eq!(collected.as_slice(), &[0b01001100, 0b00000001]);
        assert_eq!(indexes.as_slice(), &[2, 3, 6, 8])
    }

    #[test]
    fn test_iter_bits() {
        let bools = &[
            false, false, true, true, false, false, true, false, true, false, false, true,
        ];
        let compacted = compact_bools(bools);
        let c1: Vec<_> = iter_bits(&compacted, bools.len()).collect();
        let c2: Vec<_> = iter_bits(&compacted, 9).collect();

        assert_eq!(bools, c1.as_slice());
        assert_eq!(&bools[0..9], c2.as_slice());
    }

    #[test]
    fn test_bit_mask() {
        let mut mask = BitSet::new();

        mask.append_bits(8, &[0b11111111]);
        let d1 = mask.buffer.clone();

        mask.append_bits(3, &[0b01010010]);
        let d2 = mask.buffer.clone();

        mask.append_bits(5, &[0b00010100]);
        let d3 = mask.buffer.clone();

        mask.append_bits(2, &[0b11110010]);
        let d4 = mask.buffer.clone();

        mask.append_bits(15, &[0b11011010, 0b01010101]);
        let d5 = mask.buffer.clone();

        assert_eq!(d1.as_slice(), &[0b11111111]);
        assert_eq!(d2.as_slice(), &[0b11111111, 0b00000010]);
        assert_eq!(d3.as_slice(), &[0b11111111, 0b10100010]);
        assert_eq!(d4.as_slice(), &[0b11111111, 0b10100010, 0b00000010]);
        assert_eq!(
            d5.as_slice(),
            &[0b11111111, 0b10100010, 0b01101010, 0b01010111, 0b00000001]
        );

        assert!(mask.get(0));
        assert!(!mask.get(8));
        assert!(mask.get(9));
        assert!(mask.get(19));
    }

    #[test]
    fn test_push() {
        let bools = &[
            false, true, false, true, false, false, true, true, true, false, true, true,
        ];
        let mut mask = BitSet::new();
        for bool in bools {
            mask.push(*bool)
        }
        let compacted = compact_bools(bools);
        assert_eq!(compacted, mask.buffer);
        assert_eq!(mask.count_set(), 7);
        assert_eq!(mask.count_unset(), 5);
    }

    #[test]
    fn test_bit_mask_all_set() {
        let mut mask = BitSet::new();
        let mut all_bools = vec![];
        let mut rng = rand::thread_rng();

        for _ in 0..100 {
            let mask_length = (rng.next_u32() % 50) as usize;
            let bools: Vec<_> = std::iter::repeat(true).take(mask_length).collect();

            let collected = compact_bools(&bools);
            mask.append_bits(mask_length, &collected);
            all_bools.extend_from_slice(&bools);
        }

        let collected = compact_bools(&all_bools);
        assert_eq!(mask.buffer, collected);

        let expected_indexes: Vec<_> = iter_set_bools(&all_bools).collect();
        let actual_indexes: Vec<_> = iter_set_positions(&mask.buffer).collect();
        assert_eq!(expected_indexes, actual_indexes);
    }

    #[test]
    fn test_bit_mask_fuzz() {
        let mut mask = BitSet::new();
        let mut all_bools = vec![];
        let mut rng = rand::thread_rng();

        for _ in 0..100 {
            let mask_length = (rng.next_u32() % 50) as usize;
            let bools: Vec<_> = std::iter::from_fn(|| Some(rng.next_u32() & 1 == 0))
                .take(mask_length)
                .collect();

            let collected = compact_bools(&bools);
            mask.append_bits(mask_length, &collected);
            all_bools.extend_from_slice(&bools);
        }

        let collected = compact_bools(&all_bools);
        assert_eq!(mask.buffer, collected);

        let expected_indexes: Vec<_> = iter_set_bools(&all_bools).collect();
        let actual_indexes: Vec<_> = iter_set_positions(&mask.buffer).collect();
        assert_eq!(expected_indexes, actual_indexes);
        for index in actual_indexes {
            assert!(mask.get(index));
        }
    }

    #[test]
    fn test_arrow_compat() {
        let bools = &[
            false, false, true, true, false, false, true, false, true, false, false, true,
        ];

        let mut builder = BooleanBufferBuilder::new(bools.len());
        builder.append_slice(bools);
        let buffer = builder.finish();

        let collected = compact_bools(bools);
        let mut mask = BitSet::new();
        mask.append_bits(bools.len(), &collected);
        let mask_buffer = mask.to_arrow();

        assert_eq!(collected.as_slice(), buffer.as_slice());
        assert_eq!(buffer.as_slice(), mask_buffer.as_slice());
    }

    #[test]
    fn test_negate() {
        assert_eq!(negate_bits(0b00000000, 3), 0b00000111);
        assert_eq!(negate_bits(0b11000000, 3), 0b11000111);
        assert_eq!(negate_bits(0b11000010, 3), 0b11000101);

        let mut mask = vec![0b10111111];
        negate_mask(mask.as_mut_slice(), 8);
        assert_eq!(mask[0], 0b01000000);

        let bools = &[
            false, false, true, true, false, false, true, false, true, false, false, true,
        ];
        let negated_bools: Vec<_> = bools.iter().map(|x| !*x).collect();

        let mut c1 = compact_bools(bools);
        negate_mask(c1.as_mut_slice(), bools.len());
        let c2 = compact_bools(&negated_bools);

        assert_eq!(c1, c2)
    }
}
