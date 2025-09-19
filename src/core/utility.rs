/// Rotates a u64 value right by n bits.
pub fn rotr64(x: u64, n: u32) -> u64 {
    x.rotate_right(n)
}

/// Checks if a u64 value is a power of two.
pub fn is_power_of_two(x: u64) -> bool {
    x > 0 && (x & (x - 1)) == 0
}

/// Constexpr log2 from C++ utility.h
pub const fn log2_const(n: usize) -> usize {
    if n < 2 { 0 } else { 1 + log2_const(n >> 1) }
}

/// Constexpr is_power_of_2 from C++ utility.h
pub const fn is_power_of_2_const(v: usize) -> bool {
    v > 0 && (v & (v - 1)) == 0
}

/// FasterHash implementation.
pub struct FasterHash;

impl FasterHash {
    const K_MAGIC_NUM: u64 = 40343;

    /// Computes a hash for a byte slice.
    pub fn compute_bytes<T: Copy + Into<u64>>(data: &[T]) -> u64 {
        let mut hash_state = data.len() as u64;
        for &item in data {
            hash_state = Self::K_MAGIC_NUM * hash_state + item.into();
        }
        rotr64(Self::K_MAGIC_NUM * hash_state, 6)
    }

    /// Computes a hash for a u64 input.
    pub fn compute_u64(input: u64) -> u64 {
        let local_rand = input;
        let mut local_rand_hash = 8;
        local_rand_hash = Self::K_MAGIC_NUM * local_rand_hash + ((local_rand) & 0xFFFF);
        local_rand_hash = Self::K_MAGIC_NUM * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
        local_rand_hash = Self::K_MAGIC_NUM * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
        local_rand_hash = Self::K_MAGIC_NUM * local_rand_hash + (local_rand >> 48);
        local_rand_hash = Self::K_MAGIC_NUM * local_rand_hash;
        rotr64(local_rand_hash, 43)
    }
}

// pad_alignment function (temporary location)
pub fn pad_alignment(size: usize, alignment: usize) -> usize {
    debug_assert!(alignment > 0);
    debug_assert!(
        (alignment & (alignment - 1)) == 0,
        "Alignment must be a power of 2"
    );
    let max_padding = alignment - 1;
    (size + max_padding) & !max_padding
}
