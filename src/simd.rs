#[cfg(target_arch = "aarch64")]
#[inline(always)]
/// Will search `value` inside `buffer`. No guarantees as the position of find.
/// It can be the first, the last or any other.
pub fn position_of_any_bool(buffer: &[bool], value: bool) -> Option<usize> {
    use std::arch::aarch64::{vceqq_u8, vdupq_n_u8, vld1q_u8, vmaxvq_u8};

    let len = buffer.len();
    let ptr = buffer.as_ptr() as *const u8;
    let mut i = 0;

    let values = unsafe { vdupq_n_u8(if value { 1 } else { 0 }) };

    while i + 16 <= len {
        let chunk = unsafe { vld1q_u8(ptr.add(i)) };
        let cmp = unsafe { vceqq_u8(chunk, values) };
        let max_val = unsafe { vmaxvq_u8(cmp) } as u8;
        if max_val != 0 {
            for j in 0..16 {
                if buffer[i + j] == value {
                    return Some(i + j);
                }
            }
        }
        i += 16;
    }

    buffer[i..]
        .iter()
        .position(|&v| v == value)
        .map(|position| i + position)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_of_any_bool_tests() {
        let mut buffer = [false; 16];
        assert!(matches!(
            position_of_any_bool(buffer.as_ref(), false),
            Some(0)
        ));

        // Just the first is true
        buffer[0] = true;
        assert!(matches!(
            position_of_any_bool(buffer.as_ref(), true),
            Some(0)
        ));
        assert!(matches!(
            position_of_any_bool(buffer.as_ref(), false),
            Some(1)
        ));

        // Just the last is true
        buffer.fill(false);
        buffer[buffer.len() - 1] = true;
        assert!(matches!(
            position_of_any_bool(buffer.as_ref(), false),
            Some(0)
        ));
        assert!(matches!(
            position_of_any_bool(buffer.as_ref(), true),
            Some(idx) if idx == buffer.len() - 1
        ));
    }
}
