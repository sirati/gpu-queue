//! Text as terminal cells: what keeps a line to one row and how many columns it takes. Each character takes its
//! own display width (wide characters two columns, combining marks none), the measure terminals lay text out by.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_control_character_shows_as_one_question_mark() {
        assert_eq!(printable("a\nb\tc\x1b[2J"), "a?b?c?[2J");
    }

    #[test]
    fn a_wide_character_takes_two_columns_and_a_combining_mark_none() {
        assert_eq!(width("显卡 card"), 9);
        assert_eq!(width("e\u{301}x"), 2);
    }
}
