//! How a counted cost grows with one dimension of its input: the cost checks of the tests. A cost is measured at three
//! sizes n, 2n and 4n; the second difference d2/d1, with d1 = c(2n) - c(n) and d2 = c(4n) - c(2n), cancels any fixed
//! cost, so linear work gives about 2, n log n slightly more and quadratic work 4. A difference is only meaningful when
//! it stands clear of the counts' own noise, so a check refuses a d1 that is not positive and at least ten times the
//! largest spread its sizes measured, and computes no ratio from it.

/// The cost counted at one size, given by its two coordinates, and the spread of the counts of the same work repeated
/// at that size.
#[derive(Clone, Copy, Debug)]
pub struct Measured {
    pub size: (u64, u64),
    pub count: u64,
    pub spread: u64,
}

/// Asserts that `what` grows at most linearly over `sizes`, which are n, 2n and 4n: d2/d1 at most `limit`.
pub fn assert_linear(what: &str, sizes: [Measured; 3], limit: f64) {
    let [n, two, four] = sizes;
    let d1 = two.count as i128 - n.count as i128;
    let d2 = four.count as i128 - two.count as i128;
    let noise = sizes.iter().map(|measured| measured.spread).max().unwrap_or(0) as i128;
    assert!(
        d1 > 0 && d1 >= 10 * noise,
        "{what}: d1 = {d1} is not a meaningful denominator against a spread of {noise}, so no ratio is computed: {sizes:?}"
    );
    let ratio = d2 as f64 / d1 as f64;
    assert!(ratio <= limit, "{what} grows faster than linearly: d2/d1 = {ratio:.3} > {limit} over {sizes:?}");
}

/// Asserts that `what` stays flat over `sizes`, which are n, 2n and 4n: c(4n) at most `limit` times c(n).
pub fn assert_flat(what: &str, sizes: [Measured; 3], limit: f64) {
    let [n, _, four] = sizes;
    let ratio = four.count as f64 / n.count as f64;
    assert!(ratio <= limit, "{what} is not flat: c(4n)/c(n) = {ratio:.3} > {limit} over {sizes:?}");
}

/// Asserts that `what`'s count at its documented size is within `limit` times its base count.
pub fn assert_budget(what: &str, measured: Measured, base: u64, limit: f64) {
    assert!(
        measured.count as f64 <= limit * base as f64,
        "{what} exceeds its budget: {} instructions > {limit} x the base {base} ({measured:?})",
        measured.count
    );
}

/// The spread of repeated counts of the same work: the largest minus the smallest.
pub fn spread(counts: &[u64]) -> u64 {
    counts.iter().max().unwrap_or(&0) - counts.iter().min().unwrap_or(&0)
}
