//! A Unix time as this host's local date and time.

/// A local date and time, to the second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Local {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
}

impl std::fmt::Display for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }
}

/// `unix` in this host's time zone; none for a time that is not finite or that the C library cannot convert.
pub fn at(unix: f64) -> Option<Local> {
    if !unix.is_finite() {
        return None;
    }
    let seconds = unix as libc::time_t;
    let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
    // SAFETY: localtime_r initializes the output on success; neither pointer escapes.
    let result = unsafe { libc::localtime_r(&seconds, local.as_mut_ptr()) };
    if result.is_null() {
        return None;
    }
    // SAFETY: localtime_r succeeded above.
    let local = unsafe { local.assume_init() };
    Some(Local {
        year: local.tm_year + 1900,
        month: (local.tm_mon + 1) as u32,
        day: local.tm_mday as u32,
        hour: local.tm_hour as u32,
        minute: local.tm_min as u32,
        second: local.tm_sec as u32,
    })
}

/// `unix` as a local date and time, or as the number itself when it has none.
pub fn text(unix: f64) -> String {
    at(unix).map_or_else(|| format!("{unix}"), |local| local.to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_time_is_its_local_date_and_time_and_a_non_finite_one_has_none() {
        let local = super::at(1_700_000_000.0).unwrap();
        assert_eq!(local.to_string().len(), "2023-11-14 22:13:20".len());
        assert_eq!(super::at(f64::NAN), None);
        assert_eq!(super::text(f64::INFINITY), "inf");
    }
}

