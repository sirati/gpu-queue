//! Text as terminal cells: what keeps a line to one row, how many columns it takes, and the part of it a
//! window of columns shows. Each character takes its own display width (wide characters two columns,
//! combining marks none), the measure terminals lay text out by.
use unicode_width::UnicodeWidthChar;

/// Text as one row: a control character (a newline, a tab, an escape) shows as `?`.
pub fn printable(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

fn columns_of(c: char) -> usize {
    c.width().unwrap_or(0)
}

/// The columns printable `text` takes.
pub fn width(text: &str) -> usize {
    text.chars().map(columns_of).sum()
}

/// The columns of printable `text` from `left`, at most `columns` of them; a wide character cut by either
/// edge shows its part as spaces, and a combining mark goes with the character before it.
pub fn cut(text: &str, left: usize, columns: usize) -> String {
    let right = left + columns;
    let mut shown = String::new();
    let mut at = 0;
    let mut kept = false;
    for c in text.chars() {
        let end = at + columns_of(c);
        kept = match (end == at, end <= left, at < left, end <= right, at < right) {
            (true, ..) => {
                shown.extend(kept.then_some(c));
                kept
            }
            (false, true, ..) => false,
            (false, false, true, ..) => {
                shown.push_str(&" ".repeat(end.min(right) - left));
                false
            }
            (false, false, false, true, _) => {
                shown.push(c);
                true
            }
            (false, false, false, false, true) => {
                shown.push_str(&" ".repeat(right - at));
                false
            }
            (false, false, false, false, false) => break,
        };
        at = end;
    }
    shown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_control_character_shows_as_one_question_mark() {
        assert_eq!(printable("a\nb\tc\x1b[2J"), "a?b?c?[2J");
    }

    #[test]
    fn a_cut_counts_columns_and_a_split_wide_character_becomes_spaces() {
        assert_eq!(width("显卡 card"), 9);
        assert_eq!(cut("显卡 card", 0, 9), "显卡 card");
        assert_eq!(cut("显卡 card", 1, 4), " 卡 ");
        assert_eq!(cut("显卡 card", 0, 3), "显 ");
        assert_eq!(cut("显卡 card", 5, 10), "card");
        assert_eq!(cut("e\u{301}x", 0, 1), "e\u{301}");
        assert_eq!(cut("显e\u{301}", 1, 3), " e\u{301}");
        assert_eq!(cut("显\u{301}x", 1, 3), " x");
        assert_eq!(cut("e\u{301}x", 1, 3), "x");
        assert_eq!(cut("short", 9, 4), "");
    }
}
