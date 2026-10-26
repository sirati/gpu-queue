//! `gpu-run hook`: Claude Code's PreToolUse hook for the container.
//!
//! In there the agent's model is NInfer, and the GPU is either NInfer's or the
//! queue's. A foreground `gpu-run wait` (or `switch-gpu`, `logs -f`, ...) in a
//! Bash call can wait for something that only happens once the agent's own
//! next request is served, which it cannot be while the call runs: a deadlock.
//! The MCP tools (`mcp.rs`) return at once; this hook turns the CLI away in a
//! foreground Bash call and lets it through in a background one, where waiting
//! blocks nothing.
//!
//! It is a guardrail for an agent that means well, not a security boundary: it
//! reads the command line, and a script or program that runs `gpu-run` on its
//! own gets past it. So the scan is shell-aware (quotes, substitutions,
//! separators, wrappers like `env`, `sudo`, `sh -c`, `nix run`) but gives up
//! quietly on what it cannot see into.

use std::collections::HashMap;
use std::io::Read;
use std::process::ExitCode;

use serde_json::{json, Value};

pub const DENY_REASON: &str = "Please use the MCP tool, otherwise you can deadlock yourself.";

const PROGRAM: &str = "gpu-run";
/// How deep `sh -c '...$(...)...'` is followed.
const MAX_DEPTH: usize = 8;

pub fn run() -> ExitCode {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_ok() {
        if let Some(decision) = decide(&input) {
            println!("{decision}");
        }
    }
    // Nothing printed and exit 0: the call goes ahead.
    ExitCode::SUCCESS
}

/// The hook's answer for one tool call, or `None` to let it through.
pub fn decide(input: &str) -> Option<Value> {
    let call: Value = serde_json::from_str(input).ok()?;
    if call.get("tool_name")?.as_str()? != "Bash" {
        return None;
    }
    let tool_input = call.get("tool_input")?;
    if tool_input.get("run_in_background").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    let command = tool_input.get("command")?.as_str()?;
    if !invokes_gpu_run(command) {
        return None;
    }
    Some(json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": DENY_REASON,
        }
    }))
}

/// Does running this shell command line run `gpu-run`?
pub fn invokes_gpu_run(script: &str) -> bool {
    let mut scan = Scan::default();
    scan.script_str(script, 0)
}

/// `gpu-run` as a word of its own: not inside `test_gpu-run.py` or `gpu-runner`.
fn contains_token(text: &str) -> bool {
    let word_char = |c: char| c.is_alphanumeric() || matches!(c, '_' | '-' | '.');
    text.match_indices(PROGRAM).any(|(i, m)| {
        let before = text[..i].chars().next_back();
        let after = text[i + m.len()..].chars().next();
        !before.is_some_and(word_char) && !after.is_some_and(word_char)
    })
}

fn base(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn is_assignment(word: &str) -> bool {
    match word.split_once('=') {
        Some((name, _)) => {
            !name.is_empty()
                && !name.starts_with(|c: char| c.is_ascii_digit())
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        None => false,
    }
}

/// One shell word after quote removal.
#[derive(Default)]
struct Word {
    text: String,
    /// A substitution in it (`$(which gpu-run)`) names gpu-run.
    mentions: bool,
}

#[derive(Default)]
struct Cmd {
    words: Vec<Word>,
    /// Here-document and here-string bodies fed to it.
    stdin: Vec<String>,
    /// Here-document delimiters still to read at the end of the line.
    heredocs: Vec<(String, bool)>,
    /// Piped into the next command.
    pipe_out: bool,
}

enum Redir {
    /// The next word is a file or fd.
    Target,
    Heredoc {
        strip_tabs: bool,
    },
    HereString,
}

enum Tok {
    Eof,
    Word(Word),
    Redir(Redir),
    /// `;`, `&`, `&&`, `||`: the next word starts a command.
    Sep,
    /// `|`, `|&`: as `Sep`, and the command's output feeds the next.
    Pipe,
    Newline,
    Open,
    Close,
}

struct P<'a> {
    s: &'a [char],
    i: usize,
}

impl P<'_> {
    fn peek(&self) -> Option<char> {
        self.s.get(self.i).copied()
    }
    fn at(&self, n: usize) -> Option<char> {
        self.s.get(self.i + n).copied()
    }
    fn next(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.i += 1;
        }
        c
    }
}

#[derive(Default)]
struct Scan {
    found: bool,
    /// Variables assigned a gpu-run path earlier in the script.
    vars: HashMap<String, String>,
}

impl Scan {
    fn script_str(&mut self, src: &str, depth: usize) -> bool {
        let chars: Vec<char> = src.chars().collect();
        self.script(&chars, depth);
        self.found
    }

    fn script(&mut self, src: &[char], depth: usize) {
        if depth > MAX_DEPTH {
            return;
        }
        let mut p = P { s: src, i: 0 };
        self.commands(&mut p, depth, false);
    }

    /// Read commands until the end, or, `nested`, the `)` that closes `$(`
    /// and `<(`, and check each.
    fn commands(&mut self, p: &mut P, depth: usize, nested: bool) {
        let mut line: Vec<Cmd> = Vec::new();
        let mut cur = Cmd::default();
        let mut redir: Option<Redir> = None;
        let mut parens = 0usize;
        loop {
            if self.found {
                return;
            }
            match self.token(p, depth) {
                Tok::Word(w) => match redir.take() {
                    Some(Redir::Target) => {}
                    Some(Redir::Heredoc { strip_tabs }) => cur.heredocs.push((w.text, strip_tabs)),
                    Some(Redir::HereString) => cur.stdin.push(w.text),
                    None => cur.words.push(w),
                },
                Tok::Redir(r) => redir = Some(r),
                Tok::Sep => {
                    redir = None;
                    line.push(std::mem::take(&mut cur));
                }
                Tok::Pipe => {
                    redir = None;
                    cur.pipe_out = true;
                    line.push(std::mem::take(&mut cur));
                }
                Tok::Open => {
                    parens += 1;
                    line.push(std::mem::take(&mut cur));
                }
                Tok::Close if parens > 0 => {
                    parens -= 1;
                    line.push(std::mem::take(&mut cur));
                }
                Tok::Close if !nested => line.push(std::mem::take(&mut cur)),
                Tok::Close | Tok::Eof => {
                    line.push(std::mem::take(&mut cur));
                    self.check_line(line, depth);
                    return;
                }
                Tok::Newline => {
                    redir = None;
                    line.push(std::mem::take(&mut cur));
                    read_heredocs(p, &mut line);
                    self.check_line(std::mem::take(&mut line), depth);
                }
            }
        }
    }

    fn check_line(&mut self, mut line: Vec<Cmd>, depth: usize) {
        // `echo 'gpu-run status' | bash`: what the left side names is the right side's script.
        for k in 1..line.len() {
            if line[k - 1].pipe_out {
                let fed: Vec<&str> = line[k - 1]
                    .words
                    .iter()
                    .skip(1)
                    .map(|w| w.text.as_str())
                    .collect();
                let fed = fed.join(" ");
                line[k].stdin.push(fed);
            }
        }
        for cmd in line {
            if !self.found && self.check_cmd(&cmd, depth) {
                self.found = true;
            }
        }
    }

    fn token(&mut self, p: &mut P, depth: usize) -> Tok {
        loop {
            match p.peek() {
                Some(' ' | '\t') => p.i += 1,
                Some('\\') if p.at(1) == Some('\n') => p.i += 2,
                Some('#') => {
                    while p.peek().is_some_and(|c| c != '\n') {
                        p.i += 1;
                    }
                }
                _ => break,
            }
        }
        let Some(c) = p.peek() else {
            return Tok::Eof;
        };
        match c {
            '\n' => {
                p.i += 1;
                Tok::Newline
            }
            ';' => {
                p.i += 1;
                if p.peek() == Some(';') {
                    p.i += 1;
                }
                Tok::Sep
            }
            '&' => {
                p.i += 1;
                match p.peek() {
                    Some('>') => {
                        p.i += 1;
                        if p.peek() == Some('>') {
                            p.i += 1;
                        }
                        Tok::Redir(Redir::Target)
                    }
                    Some('&') => {
                        p.i += 1;
                        Tok::Sep
                    }
                    _ => Tok::Sep,
                }
            }
            '|' => {
                p.i += 1;
                match p.peek() {
                    Some('|') => {
                        p.i += 1;
                        Tok::Sep
                    }
                    Some('&') => {
                        p.i += 1;
                        Tok::Pipe
                    }
                    _ => Tok::Pipe,
                }
            }
            '(' => {
                p.i += 1;
                Tok::Open
            }
            ')' => {
                p.i += 1;
                Tok::Close
            }
            '<' | '>' if p.at(1) == Some('(') => {
                // Process substitution: a command of its own.
                p.i += 2;
                self.commands(p, depth + 1, true);
                Tok::Word(Word {
                    text: "/dev/fd/63".into(),
                    mentions: false,
                })
            }
            '<' => {
                p.i += 1;
                match p.peek() {
                    Some('<') => {
                        p.i += 1;
                        match p.peek() {
                            Some('<') => {
                                p.i += 1;
                                Tok::Redir(Redir::HereString)
                            }
                            Some('-') => {
                                p.i += 1;
                                Tok::Redir(Redir::Heredoc { strip_tabs: true })
                            }
                            _ => Tok::Redir(Redir::Heredoc { strip_tabs: false }),
                        }
                    }
                    Some('&' | '>') => {
                        p.i += 1;
                        Tok::Redir(Redir::Target)
                    }
                    _ => Tok::Redir(Redir::Target),
                }
            }
            '>' => {
                p.i += 1;
                if matches!(p.peek(), Some('>' | '|' | '&')) {
                    p.i += 1;
                }
                Tok::Redir(Redir::Target)
            }
            _ => {
                let (w, plain) = self.word(p, depth);
                // `2>&1`, `3<file`: a bare number before a redirection is its fd.
                if plain
                    && !w.text.is_empty()
                    && w.text.chars().all(|c| c.is_ascii_digit())
                    && matches!(p.peek(), Some('<' | '>'))
                    && p.at(1) != Some('(')
                {
                    return self.token(p, depth);
                }
                Tok::Word(w)
            }
        }
    }

    /// One word, quotes removed. The flag says no quote or expansion was in it.
    fn word(&mut self, p: &mut P, depth: usize) -> (Word, bool) {
        let mut w = Word::default();
        let mut plain = true;
        while let Some(c) = p.peek() {
            match c {
                ' ' | '\t' | '\n' | ';' | '&' | '|' | '(' | ')' | '<' | '>' => break,
                '\\' => {
                    plain = false;
                    p.i += 1;
                    match p.next() {
                        Some('\n') | None => {}
                        Some(ch) => w.text.push(ch),
                    }
                }
                '\'' => {
                    plain = false;
                    p.i += 1;
                    while let Some(ch) = p.next() {
                        if ch == '\'' {
                            break;
                        }
                        w.text.push(ch);
                    }
                }
                '"' => {
                    plain = false;
                    p.i += 1;
                    self.double_quoted(p, depth, &mut w);
                }
                '$' => {
                    plain = false;
                    self.dollar(p, depth, &mut w, false);
                }
                '`' => {
                    plain = false;
                    self.backtick(p, depth, &mut w);
                }
                _ => {
                    w.text.push(c);
                    p.i += 1;
                }
            }
        }
        (w, plain)
    }

    fn double_quoted(&mut self, p: &mut P, depth: usize, w: &mut Word) {
        while let Some(c) = p.peek() {
            match c {
                '"' => {
                    p.i += 1;
                    return;
                }
                '\\' => {
                    p.i += 1;
                    match p.next() {
                        Some('\n') | None => {}
                        Some(ch @ ('$' | '`' | '"' | '\\')) => w.text.push(ch),
                        Some(ch) => {
                            w.text.push('\\');
                            w.text.push(ch);
                        }
                    }
                }
                '$' => self.dollar(p, depth, w, true),
                '`' => self.backtick(p, depth, w),
                _ => {
                    w.text.push(c);
                    p.i += 1;
                }
            }
        }
    }

    /// At a `$`.
    fn dollar(&mut self, p: &mut P, depth: usize, w: &mut Word, in_quotes: bool) {
        match p.at(1) {
            Some('(') if p.at(2) == Some('(') => {
                // Arithmetic: `$((`...`))`, kept as text.
                let start = p.i;
                p.i += 3;
                let mut open = 2usize;
                while let Some(c) = p.next() {
                    match c {
                        '(' => open += 1,
                        ')' => {
                            open -= 1;
                            if open == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                w.text.extend(&p.s[start..p.i]);
            }
            Some('(') => {
                let start = p.i + 2;
                p.i += 2;
                self.commands(p, depth + 1, true);
                let end = p.i.saturating_sub(1).max(start);
                let inner: String = p.s[start..end.min(p.s.len())].iter().collect();
                w.mentions |= contains_token(&inner);
                w.text.push_str("$(");
                w.text.push_str(&inner);
                w.text.push(')');
            }
            Some('{') => {
                let start = p.i;
                p.i += 2;
                let mut open = 1usize;
                while let Some(c) = p.next() {
                    match c {
                        '{' => open += 1,
                        '}' => {
                            open -= 1;
                            if open == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                w.text.extend(&p.s[start..p.i]);
            }
            Some('\'') if !in_quotes => {
                p.i += 2;
                while let Some(c) = p.next() {
                    match c {
                        '\\' => match p.next() {
                            Some('n') => w.text.push('\n'),
                            Some('t') => w.text.push('\t'),
                            Some(ch) => w.text.push(ch),
                            None => {}
                        },
                        '\'' => break,
                        _ => w.text.push(c),
                    }
                }
            }
            Some('"') if !in_quotes => {
                p.i += 2;
                self.double_quoted(p, depth, w);
            }
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {
                w.text.push('$');
                p.i += 1;
                while let Some(c) = p.peek().filter(|c| c.is_ascii_alphanumeric() || *c == '_') {
                    w.text.push(c);
                    p.i += 1;
                }
            }
            Some(c) => {
                w.text.push('$');
                w.text.push(c);
                p.i += 2;
            }
            None => {
                w.text.push('$');
                p.i += 1;
            }
        }
    }

    /// At a backtick.
    fn backtick(&mut self, p: &mut P, depth: usize, w: &mut Word) {
        p.i += 1;
        let mut inner = String::new();
        while let Some(c) = p.next() {
            match c {
                '`' => break,
                '\\' => match p.next() {
                    Some(ch @ ('`' | '\\' | '$')) => inner.push(ch),
                    Some(ch) => {
                        inner.push('\\');
                        inner.push(ch);
                    }
                    None => {}
                },
                _ => inner.push(c),
            }
        }
        w.mentions |= contains_token(&inner);
        self.script_str(&inner, depth + 1);
        w.text.push_str("$(");
        w.text.push_str(&inner);
        w.text.push(')');
    }

    /// Is this simple command a run of gpu-run?
    fn check_cmd(&mut self, cmd: &Cmd, depth: usize) -> bool {
        let mut i = 0;
        // Leading `NAME=value`s; remember the ones that name gpu-run.
        while let Some(w) = cmd.words.get(i).filter(|w| is_assignment(&w.text)) {
            self.note_assignment(&w.text);
            i += 1;
        }
        while cmd.words.get(i).is_some_and(|w| {
            matches!(
                w.text.as_str(),
                "!" | "{"
                    | "}"
                    | "if"
                    | "then"
                    | "else"
                    | "elif"
                    | "do"
                    | "while"
                    | "until"
                    | "time"
                    | "coproc"
            )
        }) {
            i += 1;
        }
        let Some(first) = cmd.words.get(i) else {
            return false;
        };
        if first.mentions {
            return true;
        }
        let argv: Vec<String> = cmd.words[i..].iter().map(|w| w.text.clone()).collect();
        self.runs(&argv, &cmd.stdin, depth)
    }

    fn note_assignment(&mut self, word: &str) {
        if let Some((name, value)) = word.split_once('=') {
            self.vars.insert(name.to_string(), value.to_string());
        }
    }

    /// Does the command `argv` end up running gpu-run?
    fn runs(&mut self, argv: &[String], stdin: &[String], depth: usize) -> bool {
        if depth > MAX_DEPTH {
            return false;
        }
        let Some(first) = argv.first() else {
            return false;
        };
        // `$X` or `${X}` for an X assigned a path to gpu-run earlier.
        if let Some(var) = first.strip_prefix('$') {
            let name = var.trim_start_matches('{');
            let name = name
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .unwrap_or("");
            if self.vars.get(name).is_some_and(|v| base(v) == PROGRAM) {
                return true;
            }
        }
        let name = base(first);
        if name == PROGRAM {
            return true;
        }
        let args = &argv[1..];
        match name {
            "sh" | "bash" | "dash" | "zsh" | "ksh" | "mksh" | "ash" | "fish" | "rbash" => {
                match shell_script(args) {
                    Some(script) => self.script_str(script, depth + 1),
                    None => stdin.iter().any(|t| self.script_str(t, depth + 1)),
                }
            }
            "eval" => self.script_str(&args.join(" "), depth + 1),
            "export" | "declare" | "local" | "readonly" | "typeset" => {
                for a in args.iter().filter(|a| is_assignment(a)) {
                    self.note_assignment(a);
                }
                false
            }
            "command" if args.first().is_some_and(|a| a == "-v" || a == "-V") => false,
            "command" | "builtin" => self.wrapped(&WRAPPER_PLAIN, args, stdin, depth),
            "exec" => self.wrapped(
                &Wrapper {
                    val: &["-a"],
                    ..WRAPPER_PLAIN
                },
                args,
                stdin,
                depth,
            ),
            "env" => {
                // `-S STRING` splits STRING into the command.
                if let Some(i) = args.iter().position(|a| a == "-S" || a == "--split-string") {
                    if let Some(s) = args.get(i + 1) {
                        if self.script_str(s, depth + 1) {
                            return true;
                        }
                    }
                }
                self.wrapped(
                    &Wrapper {
                        val: &["-u", "--unset", "-C", "--chdir", "-S", "--split-string"],
                        ..WRAPPER_PLAIN
                    },
                    args,
                    stdin,
                    depth,
                )
            }
            "nix" => {
                if let Some(i) = args.iter().position(|a| a == "-c" || a == "--command") {
                    if self.runs(&args[i + 1..], &[], depth + 1) {
                        return true;
                    }
                }
                args.iter().any(|a| {
                    a.split_once('#')
                        .is_some_and(|(_, attr)| attr.split(['^', '!']).next() == Some(PROGRAM))
                })
            }
            "nix-shell" => args
                .iter()
                .position(|a| a == "--run" || a == "--command")
                .and_then(|i| args.get(i + 1))
                .is_some_and(|s| self.script_str(s, depth + 1)),
            "find" => {
                let mut i = 0;
                while i < args.len() {
                    if matches!(args[i].as_str(), "-exec" | "-execdir" | "-ok" | "-okdir") {
                        let end = args[i + 1..]
                            .iter()
                            .position(|a| a == ";" || a == "+")
                            .map_or(args.len(), |e| i + 1 + e);
                        if self.runs(&args[i + 1..end], &[], depth + 1) {
                            return true;
                        }
                        i = end;
                    }
                    i += 1;
                }
                false
            }
            // Inline code can run anything; a mention of gpu-run is enough.
            "python" | "python2" | "python3" | "perl" | "ruby" | "node" | "nodejs" | "lua"
            | "luajit" | "php" | "awk" | "gawk" | "mawk" | "tclsh" | "Rscript" => {
                args.iter().any(|a| contains_token(a))
            }
            _ => match WRAPPERS.iter().find(|w| w.name == name) {
                Some(w) => self.wrapped(w, args, stdin, depth),
                None => {
                    name.strip_prefix("python")
                        .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
                        && args.iter().any(|a| contains_token(a))
                }
            },
        }
    }

    /// A command that runs the rest of its arguments as a command.
    fn wrapped(&mut self, w: &Wrapper, args: &[String], stdin: &[String], depth: usize) -> bool {
        // `-c SCRIPT`, also as the last of grouped flags (`script -qc SCRIPT`).
        let script_flag = |a: &String| {
            w.script.contains(&a.as_str())
                || (w.script.contains(&"-c")
                    && a.starts_with('-')
                    && !a.starts_with("--")
                    && a.len() > 2
                    && a.ends_with('c'))
        };
        if let Some(i) = args.iter().position(script_flag) {
            if let Some(s) = args.get(i + 1) {
                if self.script_str(s, depth + 1) {
                    return true;
                }
            }
        }
        let mut i = 0;
        let mut positionals = w.positionals;
        while let Some(a) = args.get(i) {
            if a == "--" {
                i += 1;
                break;
            }
            if a.starts_with('-') && a.len() > 1 {
                i += if w.val.contains(&a.as_str()) { 2 } else { 1 };
            } else if is_assignment(a) {
                self.note_assignment(a);
                i += 1;
            } else if positionals > 0 {
                positionals -= 1;
                i += 1;
            } else {
                break;
            }
        }
        let rest = &args[i.min(args.len())..];
        if w.joined {
            return self.script_str(&rest.join(" "), depth + 1);
        }
        self.runs(rest, stdin, depth + 1)
    }
}

/// The program text of `sh -c TEXT`, with flags like `-lc` and `-o OPTION`.
fn shell_script(args: &[String]) -> Option<&str> {
    let mut i = 0;
    while let Some(a) = args.get(i) {
        if a == "--" || !a.starts_with('-') {
            return None;
        }
        if !a.starts_with("--") && a[1..].contains('c') {
            return args.get(i + 1).map(String::as_str);
        }
        i += if a == "-o" || a == "-O" { 2 } else { 1 };
    }
    None
}

struct Wrapper {
    name: &'static str,
    /// Options that take the next word as their value.
    val: &'static [&'static str],
    /// Words that come before the command (`timeout 10 CMD`).
    positionals: usize,
    /// Options whose value is a shell script (`su -c SCRIPT`).
    script: &'static [&'static str],
    /// The rest is joined into one script (`watch`).
    joined: bool,
}

const WRAPPER_PLAIN: Wrapper = Wrapper {
    name: "",
    val: &[],
    positionals: 0,
    script: &[],
    joined: false,
};

const WRAPPERS: &[Wrapper] = &[
    Wrapper {
        name: "nohup",
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "setsid",
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "time",
        val: &["-f", "-o", "--format", "--output"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "nice",
        val: &["-n", "--adjustment"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "ionice",
        val: &["-c", "-n", "-p", "-P", "-u", "--class", "--classdata"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "stdbuf",
        val: &["-i", "-o", "-e"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "timeout",
        val: &["-s", "-k", "--signal", "--kill-after"],
        positionals: 1,
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "sudo",
        val: &[
            "-u",
            "-g",
            "-h",
            "-p",
            "-C",
            "-D",
            "-R",
            "-T",
            "-U",
            "-r",
            "-t",
            "--user",
            "--group",
            "--host",
            "--prompt",
            "--chdir",
            "--chroot",
            "--role",
            "--type",
            "--close-from",
            "--other-user",
            "--command-timeout",
        ],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "doas",
        val: &["-u", "-C"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "su",
        script: &["-c", "--command"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "runuser",
        val: &["-u", "-g", "-G", "-s", "-w"],
        script: &["-c", "--command"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "script",
        val: &["-I", "-O", "-B", "-T", "-m", "-E", "-o"],
        script: &["-c", "--command"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "flock",
        val: &["-w", "-E", "--timeout", "--conflict-exit-code"],
        positionals: 1,
        script: &["-c", "--command"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "taskset",
        positionals: 1,
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "chrt",
        positionals: 1,
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "unbuffer",
        val: &["-p"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "xargs",
        val: &[
            "-I",
            "-n",
            "-P",
            "-s",
            "-d",
            "-E",
            "-L",
            "-a",
            "--max-args",
            "--max-procs",
            "--delimiter",
            "--arg-file",
        ],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "parallel",
        val: &["-j", "-n", "-N", "-a", "-S", "--jobs"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "watch",
        val: &["-n", "--interval"],
        joined: true,
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "strace",
        val: &["-o", "-e", "-p", "-s", "-u", "-E", "-a"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "ltrace",
        val: &["-o", "-e", "-p", "-s", "-u", "-a"],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "systemd-run",
        val: &[
            "-p",
            "-u",
            "-E",
            "-M",
            "-H",
            "--property",
            "--unit",
            "--slice",
            "--setenv",
            "--uid",
            "--gid",
            "--description",
            "--working-directory",
            "--machine",
            "--host",
        ],
        ..WRAPPER_PLAIN
    },
    Wrapper {
        name: "pkexec",
        val: &["--user"],
        ..WRAPPER_PLAIN
    },
];

/// Read the bodies of the here-documents opened on the line just ended.
fn read_heredocs(p: &mut P, line: &mut [Cmd]) {
    for cmd in line.iter_mut() {
        for (delim, strip_tabs) in std::mem::take(&mut cmd.heredocs) {
            let mut body = String::new();
            while p.peek().is_some() {
                let start = p.i;
                while p.peek().is_some_and(|c| c != '\n') {
                    p.i += 1;
                }
                let text: String = p.s[start..p.i].iter().collect();
                if p.peek().is_some() {
                    p.i += 1;
                }
                let cmp = if strip_tabs {
                    text.trim_start_matches('\t')
                } else {
                    &text
                };
                if cmp == delim {
                    break;
                }
                body.push_str(&text);
                body.push('\n');
            }
            cmd.stdin.push(body);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn denied(cmd: &str) -> bool {
        invokes_gpu_run(cmd)
    }

    #[test]
    fn plain_invocations() {
        for c in [
            "gpu-run status",
            "gpu-run wait 3",
            "gpu-run switch-gpu",
            "gpu-run logs 4 -f",
            "  gpu-run   status  ",
            "/run/current-system/sw/bin/gpu-run status",
            "./gpu-run status",
            "~/bin/gpu-run status",
            "GPU_SWITCH_SOCKET=/x gpu-run status",
            "A=1 B=2 gpu-run status",
            "gpu-run --socket /x status",
            "gpu-run",
        ] {
            assert!(denied(c), "{c}");
        }
    }

    #[test]
    fn separators_and_grouping() {
        for c in [
            "cd /x; gpu-run wait 1",
            "cd /x && gpu-run wait 1",
            "false || gpu-run wait 1",
            "echo hi | gpu-run submit -- true",
            "echo hi |& gpu-run submit -- true",
            "sleep 1 & gpu-run wait 1",
            "echo a\ngpu-run wait 1",
            "echo a\n\n\n  gpu-run wait 1\n",
            "(gpu-run wait 1)",
            "( cd /x ; gpu-run wait 1 )",
            "{ gpu-run wait 1; }",
            "! gpu-run wait 1",
            "if gpu-run status; then echo ok; fi",
            "while ! gpu-run status; do sleep 1; done",
            "for i in 1 2 3; do gpu-run wait $i; done",
            "until gpu-run status; do :; done",
            "time gpu-run wait 1",
            "x=1\ngpu-run wait 1",
            "echo a # comment\ngpu-run wait 1",
            "f() { gpu-run wait \"$1\"; }; f 3",
            "case x in x) gpu-run status;; esac",
        ] {
            assert!(denied(c), "{c}");
        }
    }

    #[test]
    fn quoting_and_expansion() {
        for c in [
            "'gpu-run' status",
            "\"gpu-run\" status",
            "g''pu-run status",
            "gpu\"\"-run status",
            "gpu\\-run status",
            "\"$(gpu-run status)\"",
            "echo $(gpu-run status)",
            "echo `gpu-run status`",
            "echo \"x `gpu-run status` y\"",
            "echo \"nested $(echo $(gpu-run status))\"",
            "echo $(echo `gpu-run status`)",
            "$(which gpu-run) status",
            "`which gpu-run` status",
            "diff <(gpu-run status) /dev/null",
            "cat > >(gpu-run submit -- true)",
            "X=gpu-run; $X status",
            "X=/nix/store/abc/bin/gpu-run; ${X} status",
            "export G=gpu-run; $G status",
            "env G=gpu-run $G status",
            "exec gpu-run wait 1",
        ] {
            assert!(denied(c), "{c}");
        }
    }

    #[test]
    fn wrappers() {
        for c in [
            "env gpu-run status",
            "env -i gpu-run status",
            "env -u FOO FOO=1 gpu-run status",
            "env -C /x gpu-run status",
            "env -S 'gpu-run status'",
            "command gpu-run status",
            "builtin exec gpu-run status",
            "nohup gpu-run wait 1",
            "setsid gpu-run wait 1",
            "setsid -f gpu-run wait 1",
            "timeout 60 gpu-run wait 1",
            "timeout -s KILL 60 gpu-run wait 1",
            "timeout --kill-after=5 60 gpu-run wait 1",
            "nice -n 5 gpu-run wait 1",
            "ionice -c 3 gpu-run wait 1",
            "stdbuf -oL gpu-run logs 1 -f",
            "sudo gpu-run status",
            "sudo -u bob -g grp gpu-run status",
            "sudo -E env X=1 gpu-run status",
            "doas -u bob gpu-run status",
            "su -c 'gpu-run status' bob",
            "su bob -c \"gpu-run status\"",
            "runuser -u bob -- gpu-run status",
            "runuser -l bob -c 'gpu-run status'",
            "flock /tmp/lock gpu-run wait 1",
            "flock -w 5 /tmp/lock gpu-run wait 1",
            "flock /tmp/lock -c 'gpu-run wait 1'",
            "taskset -c 0-3 gpu-run wait 1",
            "chrt -f 10 gpu-run wait 1",
            "xargs gpu-run wait",
            "xargs -n1 -I{} gpu-run wait {}",
            "parallel -j4 gpu-run wait ::: 1 2",
            "watch -n 5 gpu-run status",
            "watch 'gpu-run status'",
            "strace -o /tmp/t -f gpu-run status",
            "systemd-run --user --scope --slice vm-tests.slice gpu-run status",
            "systemd-run --user --wait -p MemoryMax=1G gpu-run status",
            "script -qc 'gpu-run status' /dev/null",
            "find / -name x -exec gpu-run status {} \\;",
            "find . -exec echo {} + -exec gpu-run status \\;",
            "timeout 5 env X=1 nice gpu-run status",
        ] {
            assert!(denied(c), "{c}");
        }
    }

    #[test]
    fn shells_and_nix() {
        for c in [
            "sh -c 'gpu-run status'",
            "bash -c \"gpu-run status\"",
            "bash -lc 'gpu-run status'",
            "bash -ec 'cd /x && gpu-run wait 1'",
            "bash -o pipefail -c 'gpu-run status'",
            "zsh -c 'gpu-run status'",
            "sh -c 'echo $(gpu-run status)'",
            "sh -c \"sh -c 'gpu-run status'\"",
            "eval gpu-run status",
            "eval 'gpu-run status'",
            "echo gpu-run status | bash",
            "bash <<EOF\ngpu-run status\nEOF",
            "bash <<'EOF'\ncd /x\ngpu-run status\nEOF\n",
            "sh <<-EOF\n\tgpu-run status\n\tEOF\n",
            "bash <<< 'gpu-run status'",
            "nix run .#gpu-run -- status",
            "nix run path:/x#gpu-run^out -- status",
            "nix shell nixpkgs#hello -c gpu-run status",
            "nix shell nixpkgs#hello --command gpu-run status",
            "nix develop -c gpu-run status",
            "nix develop --command bash -c 'gpu-run status'",
            "nix-shell -p hello --run 'gpu-run status'",
            "nix-shell --command 'gpu-run status'",
            "python3 -c 'import os; os.system(\"gpu-run status\")'",
            "python -c \"import subprocess; subprocess.run(['gpu-run','status'])\"",
            "perl -e 'system(\"gpu-run status\")'",
            "node -e \"require('child_process').execSync('gpu-run status')\"",
            "awk 'BEGIN { system(\"gpu-run status\") }'",
        ] {
            assert!(denied(c), "{c}");
        }
    }

    #[test]
    fn things_that_only_mention_it() {
        for c in [
            "",
            "ls",
            "echo gpu-run",
            "echo 'gpu-run status'",
            "echo \"gpu-run status\"",
            "echo $(echo gpu-run)",
            "grep -rn gpu-run /etc",
            "cat gpu-run.md",
            "cat docs/gpu-run",
            "which gpu-run",
            "type gpu-run",
            "command -v gpu-run",
            "command -V gpu-run",
            "man gpu-run",
            "ls ~/.local/state/gpu-run/",
            "tail -n 20 ~/.local/state/gpu-run/3.log",
            "gpu-runner status",
            "my-gpu-run status",
            "gpu-run-helper status",
            "gpu_run status",
            "git commit -m 'gpu-run: fix timeout'",
            "git log --grep gpu-run",
            "cat <<EOF\ngpu-run status\nEOF",
            "cat <<'EOF' > notes.md\nuse gpu-run submit\nEOF\n",
            "cat > notes.md <<EOF\ngpu-run wait\nEOF\necho done",
            "echo hi > gpu-run.log",
            "echo hi 2>&1 > /tmp/gpu-run",
            "echo gpu-run | wc -c",
            "python3 -m pytest tests/test_gpu-run.py",
            "python3 script.py",
            "python3 -c 'print(1)'",
            "nix run .#gpu-arbiter -- --help",
            "nix build .#packages.x86_64-linux.gpu-arbiter",
            "find . -name gpu-run",
            "find . -exec echo {} \\;",
            "xargs grep -l gpu-run",
            "sudo cat /etc/passwd",
            "env FOO=1 true",
            "FOO=gpu-run true",
            "bash script.sh",
            "bash -c 'echo hi'",
            "sh <<EOF\necho hi\nEOF",
            "echo ok # gpu-run status",
        ] {
            assert!(!denied(c), "{c}");
        }
    }

    #[test]
    fn odd_input_does_not_crash() {
        for c in [
            "$(",
            "`",
            "'",
            "\"",
            "\\",
            "$",
            "${",
            "$((",
            "$((1+1",
            "<<",
            "<<EOF",
            ">",
            "&&",
            "))",
            "(((",
            "echo $(echo $(echo $(echo $(echo $(echo $(echo $(echo $(echo $(gpu-run status))))))))))",
            "a <<EOF1 <<EOF2\none\nEOF1\ntwo\nEOF2\ngpu-run status",
            "é gpu-run",
        ] {
            let _ = denied(c);
        }
        assert!(denied(
            "a <<EOF1 <<EOF2\none\nEOF1\ntwo\nEOF2\ngpu-run status"
        ));
    }

    #[test]
    fn the_hook_decision() {
        let call = |tool: &str, command: &str, background: Option<bool>| {
            let mut input = json!({"command": command});
            if let Some(b) = background {
                input["run_in_background"] = json!(b);
            }
            decide(&json!({"tool_name": tool, "tool_input": input}).to_string())
        };
        let d = call("Bash", "gpu-run wait 3", None).expect("denied");
        assert_eq!(d["hookSpecificOutput"]["hookEventName"], "PreToolUse");
        assert_eq!(d["hookSpecificOutput"]["permissionDecision"], "deny");
        assert_eq!(
            d["hookSpecificOutput"]["permissionDecisionReason"],
            "Please use the MCP tool, otherwise you can deadlock yourself."
        );
        assert!(call("Bash", "gpu-run wait 3", Some(false)).is_some());
        assert!(call("Bash", "gpu-run wait 3", Some(true)).is_none());
        assert!(call("Bash", "ls", None).is_none());
        assert!(call("Read", "gpu-run wait 3", None).is_none());
        assert!(call("Write", "gpu-run wait 3", None).is_none());
        for junk in [
            "",
            "not json",
            "{}",
            "{\"tool_name\":\"Bash\"}",
            "{\"tool_name\":\"Bash\",\"tool_input\":{}}",
            "{\"tool_name\":\"Bash\",\"tool_input\":{\"command\":3}}",
            "[]",
        ] {
            assert!(decide(junk).is_none(), "{junk}");
        }
    }
}
