//! A refreshing, scrollable full-screen view of text lines. The caller supplies the lines; this module owns the
//! terminal: raw input, the alternate screen, no autowrap, all restored on every way out. Each line stays one
//! row (cut at the window, never wrapped); arrow keys scroll by a row or a few columns, PgUp/PgDn by a window,
//! Home/End jump to the start or the end; q, Esc or Ctrl-C quits, and so do SIGTERM, SIGHUP and SIGINT. The
//! lines are refreshed every `refresh`; a key only redraws them. A terminal that hangs up or fails a read
//! or a write ends the view with an error.
use std::io::{ErrorKind, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::cells;

/// Columns one horizontal arrow press moves.
const COLUMN_STEP: usize = 8;

/// Bytes an escape sequence may take before it counts as garbled.
const SEQUENCE_MAX: usize = 32;

/// How long an escape waits for the rest of its sequence before it counts as the Esc key.
const ESCAPE_WAIT: Duration = Duration::from_millis(50);

/// Signals that end the view as q does, so the terminal is restored.
const STOPPING: [libc::c_int; 3] = [libc::SIGTERM, libc::SIGHUP, libc::SIGINT];

/// Set by a stopping signal; the view checks it after every wait.
static STOP: AtomicBool = AtomicBool::new(false);

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

/// The front of the terminal's bytes: a key or ignored bytes and their length, or the rest, which starts no
/// complete key (nothing, or an escape sequence the next read may complete).
enum Token {
    Read(Option<Key>, usize),
    Rest,
}

fn token(bytes: &[u8]) -> Token {
    match bytes {
        [] | [0x1b] | [0x1b, b'O'] => Token::Rest,
        [0x1b, b'[', rest @ ..] => control(rest),
        [0x1b, b'O', last, ..] => Token::Read(cursor(*last), 3),
        [0x1b, ..] | [b'q' | b'Q' | 0x03, ..] => Token::Read(Some(Key::Quit), 1),
        [_, ..] => Token::Read(None, 1),
    }
}

/// A control sequence after its `ESC [`: parameter and intermediate bytes, then one final byte.
fn control(rest: &[u8]) -> Token {
    match rest.iter().position(|b| !(0x20..=0x3f).contains(b)) {
        Some(at) if (0x40..=0x7e).contains(&rest[at]) => {
            Token::Read(control_key(&rest[..at], rest[at]), at + 3)
        }
        Some(_) => Token::Read(None, 2),
        None if rest.len() < SEQUENCE_MAX => Token::Rest,
        None => Token::Read(None, 2),
    }
}

fn control_key(parameters: &[u8], last: u8) -> Option<Key> {
    match (parameters, last) {
        (b"", last) => cursor(last),
        (b"5", b'~') => Some(Key::PageUp),
        (b"6", b'~') => Some(Key::PageDown),
        (b"1" | b"7", b'~') => Some(Key::Home),
        (b"4" | b"8", b'~') => Some(Key::End),
        _ => None,
    }
}

/// The key a cursor sequence ends in, in its `ESC [` and its `ESC O` form alike.
fn cursor(last: u8) -> Option<Key> {
    match last {
        b'A' => Some(Key::Up),
        b'B' => Some(Key::Down),
        b'C' => Some(Key::Right),
        b'D' => Some(Key::Left),
        b'H' => Some(Key::Home),
        b'F' => Some(Key::End),
        _ => None,
    }
}

/// The keys in the terminal's bytes, and the start of an escape sequence the next read may complete;
/// anything else is ignored.
pub fn keys(bytes: &[u8]) -> (Vec<Key>, &[u8]) {
    let mut found = Vec::new();
    let mut rest = bytes;
    while let Token::Read(key, used) = token(rest) {
        found.extend(key);
        rest = &rest[used..];
    }
    (found, rest)
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
            .map(|line| cells::width(&cells::printable(line)))
            .max()
            .unwrap_or(0);
        Window {
            top: self.top.min(lines.len().saturating_sub(rows)),
            left: self.left.min(widest.saturating_sub(columns)),
        }
    }

    /// The rows as drawn: each line printable and cut to the window's columns, never wrapped.
    pub fn view(self, lines: &[String], rows: usize, columns: usize) -> Vec<String> {
        lines
            .iter()
            .skip(self.top)
            .take(rows)
            .map(|line| cells::cut(&cells::printable(line), self.left, columns))
            .collect()
    }
}

/// What the terminal sent within a wait.
#[derive(Debug, PartialEq)]
enum Input {
    Bytes(Vec<u8>),
    /// The wait ran out with nothing sent.
    Quiet,
    /// A signal (a resize, a stop) cut the wait short.
    Interrupted,
}

/// A failed wait or read: a signal or a spurious wakeup only ends the wait, anything else the view.
fn interrupted(error: std::io::Error) -> Result<Input, String> {
    match error.kind() {
        ErrorKind::Interrupted | ErrorKind::WouldBlock => Ok(Input::Interrupted),
        _ => Err(format!("terminal: {error}")),
    }
}

/// The terminal in raw mode on the alternate screen, with the view's signal handlers; all restored when
/// dropped.
struct Screen {
    saved: libc::termios,
    handlers: Vec<(libc::c_int, libc::sighandler_t)>,
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
        // The guard exists before anything changes, so every way out from here restores the terminal.
        let mut screen = Screen {
            saved,
            handlers: Vec::new(),
        };
        STOP.store(false, Ordering::Relaxed);
        // A resize interrupts the wait, so the view follows the terminal at once; a stopping signal also
        // marks the view to end.
        let handled = STOPPING
            .iter()
            .map(|signal| (*signal, stopped as *const () as libc::sighandler_t))
            .chain([(libc::SIGWINCH, resized as *const () as libc::sighandler_t)]);
        for (signal, handler) in handled {
            // SAFETY: both handlers only store to an atomic or do nothing; poll returns on either.
            let before = unsafe { libc::signal(signal, handler) };
            screen.handlers.push((signal, before));
        }
        let mut raw = saved;
        raw.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG | libc::IEXTEN);
        raw.c_iflag &= !(libc::IXON | libc::ICRNL);
        raw.c_cc[libc::VMIN] = 0;
        raw.c_cc[libc::VTIME] = 0;
        // SAFETY: raw is a valid termios derived from the saved one.
        if unsafe { libc::tcsetattr(0, libc::TCSAFLUSH, &raw) } != 0 {
            return Err(format!("terminal: {}", std::io::Error::last_os_error()));
        }
        let mut out = std::io::stdout().lock();
        out.write_all(b"\x1b[?1049h\x1b[?25l\x1b[?7l")
            .and_then(|()| out.flush())
            .map_err(|e| format!("terminal: {e}"))?;
        drop(out);
        Ok(screen)
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

    /// What the terminal sends within `wait`. A hangup, an end of input or a failed read is an error.
    fn read(&self, wait: Duration) -> Result<Input, String> {
        let mut input = libc::pollfd {
            fd: 0,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd.
        let ready =
            unsafe { libc::poll(&mut input, 1, wait.as_millis().min(i32::MAX as u128) as i32) };
        let mut bytes = [0u8; 64];
        let read = match (ready, input.revents & libc::POLLIN != 0) {
            (0, _) => return Ok(Input::Quiet),
            (ready, _) if ready < 0 => return interrupted(std::io::Error::last_os_error()),
            (_, false) => return Err("terminal: hung up".into()),
            // SAFETY: reads into the buffer, at most its length.
            (_, true) => unsafe { libc::read(0, bytes.as_mut_ptr().cast(), bytes.len()) },
        };
        match read {
            0 => Err("terminal: end of input".into()),
            read if read > 0 => Ok(Input::Bytes(bytes[..read as usize].to_vec())),
            _ => interrupted(std::io::Error::last_os_error()),
        }
    }

    /// Each row on a cleared line (a row as wide as the screen leaves the cursor on its last cell, where
    /// clearing after it would erase that cell), then the status line in reverse video.
    fn draw(&self, rows: &[String], status: &str, columns: usize) -> Result<(), String> {
        let mut frame = String::from("\x1b[H");
        for row in rows {
            frame.push_str("\x1b[2K");
            frame.push_str(row);
            frame.push_str("\r\n");
        }
        frame.push_str("\x1b[J\x1b[7m");
        frame.push_str(&cells::cut(&cells::printable(status), 0, columns));
        frame.push_str("\x1b[0m");
        let mut out = std::io::stdout().lock();
        out.write_all(frame.as_bytes())
            .and_then(|()| out.flush())
            .map_err(|e| format!("terminal: {e}"))
    }
}

impl Drop for Screen {
    /// The terminal's attributes first, so a blocked write cannot keep it raw; then the signal handlers, so
    /// a stop during that write ends the process; then the screen.
    fn drop(&mut self) {
        // SAFETY: restores the attributes saved at entry.
        unsafe { libc::tcsetattr(0, libc::TCSAFLUSH, &self.saved) };
        for (signal, handler) in &self.handlers {
            // SAFETY: puts back the handler the view replaced.
            unsafe { libc::signal(*signal, *handler) };
        }
        let mut out = std::io::stdout().lock();
        let _ = out
            .write_all(b"\x1b[?7h\x1b[?25h\x1b[?1049l")
            .and_then(|()| out.flush());
    }
}

extern "C" fn resized(_: libc::c_int) {}

extern "C" fn stopped(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

/// Shows `frame()` until the viewer quits, refreshing it every `refresh`.
pub fn run(refresh: Duration, mut frame: impl FnMut() -> Vec<String>) -> Result<(), String> {
    let screen = Screen::enter()?;
    let mut window = Window::default();
    let mut lines = frame();
    let mut refreshed = Instant::now();
    let mut pending: Vec<u8> = Vec::new();
    while !STOP.load(Ordering::Relaxed) {
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
        screen.draw(&window.view(&lines, height, columns), &status, columns)?;
        let wait = match pending.is_empty() {
            true => refresh.saturating_sub(refreshed.elapsed()),
            false => ESCAPE_WAIT,
        };
        let pressed = match (screen.read(wait)?, pending.is_empty()) {
            (Input::Bytes(bytes), _) => {
                pending.extend(bytes);
                let (pressed, rest) = keys(&pending);
                pending = rest.to_vec();
                pressed
            }
            // An escape nothing followed within the wait is the Esc key.
            (Input::Quiet, false) => vec![Key::Quit],
            (Input::Quiet, true) | (Input::Interrupted, _) => Vec::new(),
        };
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_read_from_their_escape_sequences() {
        let none: &[u8] = b"";
        assert_eq!(
            keys(b"\x1b[A\x1b[B\x1b[C\x1b[D"),
            (vec![Key::Up, Key::Down, Key::Right, Key::Left], none)
        );
        assert_eq!(
            keys(b"\x1b[5~\x1b[6~\x1b[H\x1b[4~\x1bOA\x1bOF"),
            (
                vec![
                    Key::PageUp,
                    Key::PageDown,
                    Key::Home,
                    Key::End,
                    Key::Up,
                    Key::End
                ],
                none
            )
        );
        assert_eq!(keys(b"x\x1b[Zq\x1b[1;5A"), (vec![Key::Quit], none));
        assert_eq!(keys(b"\x03\x1bx"), (vec![Key::Quit, Key::Quit], none));
    }

    #[test]
    fn a_sequence_split_across_reads_waits_for_its_rest() {
        assert_eq!(keys(b"\x1b"), (Vec::new(), &b"\x1b"[..]));
        assert_eq!(keys(b"\x1b[A\x1b[5"), (vec![Key::Up], &b"\x1b[5"[..]));
        assert_eq!(keys(b"\x1b[5~"), (vec![Key::PageUp], &b""[..]));
        assert_eq!(keys(b"\x1bO"), (Vec::new(), &b"\x1bO"[..]));
        let garbled = [b"\x1b[".as_slice(), &[b'1'; SEQUENCE_MAX]].concat();
        assert_eq!(keys(&garbled), (Vec::new(), &b""[..]));
    }

    #[test]
    fn only_a_signal_or_a_spurious_wakeup_lets_the_view_wait_again() {
        assert_eq!(
            interrupted(std::io::Error::from(ErrorKind::Interrupted)),
            Ok(Input::Interrupted)
        );
        assert_eq!(
            interrupted(std::io::Error::from(ErrorKind::WouldBlock)),
            Ok(Input::Interrupted)
        );
        assert!(interrupted(std::io::Error::from_raw_os_error(libc::EIO)).is_err());
    }

    #[test]
    fn the_window_counts_display_columns_and_shows_control_characters_as_one_cell() {
        let lines = vec!["显卡显卡".to_string(), "a\tb".to_string()];
        assert_eq!(
            Window { top: 0, left: 1 }.view(&lines, 2, 4),
            [" 卡 ", "?b"]
        );
        assert_eq!(Window { top: 0, left: 9 }.within(&lines, 2, 4).left, 4);
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
