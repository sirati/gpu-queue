//! A refreshing, scrollable full-screen view of text lines. The caller supplies the lines; this module owns the
//! terminal: raw input, the alternate screen, no autowrap, all restored on every way out. Each line stays one
//! row (cut at the window, never wrapped); arrow keys scroll by a row or a few columns, PgUp/PgDn by a window,
//! Home/End jump to the start or the end; q, Esc or Ctrl-C quits. The lines are refreshed every `refresh`;
//! a key only redraws them.
use std::io::Write;
use std::time::{Duration, Instant};

/// Columns one horizontal arrow press moves.
const COLUMN_STEP: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Key {
    Up,
    Down,
    Left,
    Right,
    PageUp,
    PageDown,
    Home,
    End,
    Quit,
}

/// The keys in one read of the terminal; anything else is ignored.
pub fn keys(bytes: &[u8]) -> Vec<Key> {
    let mut found = Vec::new();
    let mut rest = bytes;
    while let Some(&first) = rest.first() {
        let (key, used) = match rest {
            [0x1b, b'[', b'A', ..] => (Some(Key::Up), 3),
            [0x1b, b'[', b'B', ..] => (Some(Key::Down), 3),
            [0x1b, b'[', b'C', ..] => (Some(Key::Right), 3),
            [0x1b, b'[', b'D', ..] => (Some(Key::Left), 3),
            [0x1b, b'[', b'H', ..] => (Some(Key::Home), 3),
            [0x1b, b'[', b'F', ..] => (Some(Key::End), 3),
            [0x1b, b'[', b'5', b'~', ..] => (Some(Key::PageUp), 4),
            [0x1b, b'[', b'6', b'~', ..] => (Some(Key::PageDown), 4),
            [0x1b, b'[', b'1', b'~', ..] => (Some(Key::Home), 4),
            [0x1b, b'[', b'4', b'~', ..] => (Some(Key::End), 4),
            [0x1b, b'[', ..] => (None, 2),
            [0x1b] => (Some(Key::Quit), 1),
            _ => (
                matches!(first, b'q' | b'Q' | 0x03 | 0x1b).then_some(Key::Quit),
                1,
            ),
        };
        found.extend(key);
        rest = &rest[used..];
    }
    found
}

/// The part of the lines in view: the first row and column shown.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Window {
    pub top: usize,
    pub left: usize,
}

impl Window {
    /// After `key`, kept inside `lines` for a window of `rows` by `columns`.
    pub fn moved(self, key: Key, lines: &[String], rows: usize, columns: usize) -> Window {
        let moved = match key {
            Key::Up => Window {
                top: self.top.saturating_sub(1),
                ..self
            },
            Key::Down => Window {
                top: self.top + 1,
                ..self
            },
            Key::Left => Window {
                left: self.left.saturating_sub(COLUMN_STEP),
                ..self
            },
            Key::Right => Window {
                left: self.left + COLUMN_STEP,
                ..self
            },
            Key::PageUp => Window {
                top: self.top.saturating_sub(rows),
                ..self
            },
            Key::PageDown => Window {
                top: self.top + rows,
                ..self
            },
            Key::Home => Window::default(),
            Key::End => Window {
                top: usize::MAX,
                left: 0,
            },
            Key::Quit => self,
        };
        moved.within(lines, rows, columns)
    }

    pub fn within(self, lines: &[String], rows: usize, columns: usize) -> Window {
        let widest = lines
            .iter()
            .map(|line| line.chars().count())
            .max()
            .unwrap_or(0);
        Window {
            top: self.top.min(lines.len().saturating_sub(rows)),
            left: self.left.min(widest.saturating_sub(columns)),
        }
    }

    /// The rows as drawn: each line cut to the window, never wrapped.
    pub fn view(self, lines: &[String], rows: usize, columns: usize) -> Vec<String> {
        lines
            .iter()
            .skip(self.top)
            .take(rows)
            .map(|line| line.chars().skip(self.left).take(columns).collect())
            .collect()
    }
}

/// The terminal in raw mode on the alternate screen, restored when dropped.
struct Screen {
    saved: libc::termios,
}

impl Screen {
    fn enter() -> Result<Screen, String> {
        // SAFETY: isatty only inspects the descriptors.
        if unsafe { libc::isatty(0) == 0 || libc::isatty(1) == 0 } {
            return Err("status --live needs a terminal".into());
        }
        let mut saved = std::mem::MaybeUninit::<libc::termios>::uninit();
        // SAFETY: tcgetattr fills the termios on success.
        if unsafe { libc::tcgetattr(0, saved.as_mut_ptr()) } != 0 {
            return Err(format!("terminal: {}", std::io::Error::last_os_error()));
        }
        // SAFETY: tcgetattr succeeded above.
        let saved = unsafe { saved.assume_init() };
        let mut raw = saved;
        raw.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG | libc::IEXTEN);
        raw.c_iflag &= !(libc::IXON | libc::ICRNL);
        raw.c_cc[libc::VMIN] = 0;
        raw.c_cc[libc::VTIME] = 0;
        // SAFETY: raw is a valid termios derived from the saved one.
        if unsafe { libc::tcsetattr(0, libc::TCSAFLUSH, &raw) } != 0 {
            return Err(format!("terminal: {}", std::io::Error::last_os_error()));
        }
        // A resize interrupts the wait, so the view follows the terminal at once.
        // SAFETY: the handler does nothing; it only makes poll return.
        unsafe { libc::signal(libc::SIGWINCH, resized as *const () as libc::sighandler_t) };
        print!("\x1b[?1049h\x1b[?25l\x1b[?7l");
        let _ = std::io::stdout().flush();
        Ok(Screen { saved })
    }

    /// Rows and columns of the terminal now.
    fn size(&self) -> (usize, usize) {
        let mut size = std::mem::MaybeUninit::<libc::winsize>::zeroed();
        // SAFETY: TIOCGWINSZ fills a winsize; on failure it stays zeroed.
        unsafe { libc::ioctl(1, libc::TIOCGWINSZ, size.as_mut_ptr()) };
        // SAFETY: zeroed or filled above.
        let size = unsafe { size.assume_init() };
        (
            usize::from(size.ws_row.max(2)),
            usize::from(size.ws_col.max(1)),
        )
    }

    /// The keys pressed within `wait`, or none.
    fn read(&self, wait: Duration) -> Vec<Key> {
        let mut input = libc::pollfd {
            fd: 0,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd.
        let ready =
            unsafe { libc::poll(&mut input, 1, wait.as_millis().min(i32::MAX as u128) as i32) };
        let mut bytes = [0u8; 64];
        // SAFETY: reads into the buffer, at most its length.
        let read = match ready > 0 {
            true => unsafe { libc::read(0, bytes.as_mut_ptr().cast(), bytes.len()) },
            false => 0,
        };
        keys(&bytes[..read.max(0) as usize])
    }

    fn draw(&self, rows: &[String], status: &str, columns: usize) {
        let mut frame = String::from("\x1b[H");
        for row in rows {
            frame.push_str(row);
            frame.push_str("\x1b[K\r\n");
        }
        frame.push_str("\x1b[J\x1b[7m");
        frame.extend(status.chars().take(columns));
        frame.push_str("\x1b[0m\x1b[K");
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(frame.as_bytes());
        let _ = out.flush();
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        print!("\x1b[?7h\x1b[?25h\x1b[?1049l");
        let _ = std::io::stdout().flush();
        // SAFETY: restores the attributes saved at entry.
        unsafe { libc::tcsetattr(0, libc::TCSAFLUSH, &self.saved) };
    }
}

extern "C" fn resized(_: libc::c_int) {}

/// Shows `frame()` until the viewer quits, refreshing it every `refresh`.
pub fn run(refresh: Duration, mut frame: impl FnMut() -> Vec<String>) -> Result<(), String> {
    let screen = Screen::enter()?;
    let mut window = Window::default();
    let mut lines = frame();
    let mut refreshed = Instant::now();
    loop {
        let (rows, columns) = screen.size();
        let height = rows - 1;
        window = window.within(&lines, height, columns);
        let status = format!(
            " q quit, arrows / PgUp / PgDn / Home / End scroll   rows {}-{} of {}, column {}",
            window.top + 1,
            (window.top + height).min(lines.len()),
            lines.len(),
            window.left + 1
        );
        screen.draw(&window.view(&lines, height, columns), &status, columns);
        let pressed = screen.read(refresh.saturating_sub(refreshed.elapsed()));
        if pressed.contains(&Key::Quit) {
            return Ok(());
        }
        window = pressed.into_iter().fold(window, |window, key| {
            window.moved(key, &lines, height, columns)
        });
        if refreshed.elapsed() >= refresh {
            lines = frame();
            refreshed = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_read_from_their_escape_sequences() {
        assert_eq!(
            keys(b"\x1b[A\x1b[B\x1b[C\x1b[D"),
            [Key::Up, Key::Down, Key::Right, Key::Left]
        );
        assert_eq!(
            keys(b"\x1b[5~\x1b[6~\x1b[H\x1b[4~"),
            [Key::PageUp, Key::PageDown, Key::Home, Key::End]
        );
        assert_eq!(keys(b"x\x1b[Zq"), [Key::Quit]);
        assert_eq!(keys(b"\x1b"), [Key::Quit]);
        assert_eq!(keys(b"\x03"), [Key::Quit]);
    }

    #[test]
    fn the_window_scrolls_within_the_lines_and_never_wraps_them() {
        let lines: Vec<String> = (0..10)
            .map(|n| format!("line {n} {}", "x".repeat(n * 4)))
            .collect();
        let window =
            Window::default()
                .moved(Key::Down, &lines, 4, 12)
                .moved(Key::Right, &lines, 4, 12);
        assert_eq!(window, Window { top: 1, left: 8 });
        assert_eq!(window.view(&lines, 4, 12)[0], "xxx");
        assert_eq!(window.view(&lines, 4, 12)[3], "xxxxxxxxxxxx");
        assert_eq!(
            window.moved(Key::End, &lines, 4, 12),
            Window { top: 6, left: 0 }
        );
        assert_eq!(
            window
                .moved(Key::PageDown, &lines, 4, 12)
                .moved(Key::PageDown, &lines, 4, 12)
                .top,
            6
        );
        assert_eq!(
            Window { top: 3, left: 50 }.moved(Key::Home, &lines, 4, 12),
            Window::default()
        );
        assert_eq!(
            Window { top: 0, left: 500 }.within(&lines, 4, 12).left,
            43 - 12
        );
    }
}
