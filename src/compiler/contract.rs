//! A compile job declares how long it runs and how much memory it needs. The
//! queue measures what it actually uses, ends a run past twice its declared
//! time, and reports every way the run broke its declaration.
use serde_json::{json, Value};

/// Requests above this get a queuing warning: memory admission serves them last.
pub const LARGE_REQUEST: u64 = 10_000_000_000;

/// Why a compile without a runtime declaration is refused.
pub const UNDECLARED_RUNTIME: &str = "declares no expected_runtime_secs; every compile declares how long it runs";

pub fn queuing_warnings(bytes: u64) -> Vec<String> {
    (bytes > LARGE_REQUEST)
        .then(|| {
            format!(
                "declares {bytes} bytes, more than 10 GB: memory admission gives requests this large very low \
                 priority and may delay them indefinitely; verify the work needs that much and declare less if it can"
            )
        })
        .into_iter()
        .collect()
}

/// What admission observed of a host memory claim, against its declaration. Neither figure is a bound: a
/// process that peaks and exits between samples can be missed.
pub struct Claim {
    pub declared: u64,
    /// The largest growth over the claim's grant-time baseline that admission charged.
    pub peak: Option<u64>,
    /// The sum of each observed process's largest resident set.
    pub high_water: Option<u64>,
}

/// The observed figures and every warning a claim's declaration earns: its size, a use beyond it, and an
/// observed high-water well below it.
pub fn claim_report(claim: Claim) -> Value {
    let mut warnings = queuing_warnings(claim.declared);
    if let Some(peak) = claim.peak.filter(|&peak| peak > claim.declared) {
        warnings.push(format!(
            "used more than its declared {} bytes: observed peak {peak} bytes",
            claim.declared
        ));
    }
    if let Some(high_water) = claim
        .high_water
        .filter(|&high_water| 1.25 * (high_water as f64) < 0.75 * claim.declared as f64)
    {
        warnings.push(format!(
            "declared {} bytes; observed high-water {high_water} bytes; the ruling's figure is about 1.25 x {high_water} = {} bytes",
            claim.declared,
            high_water + high_water / 4
        ));
    }
    json!({
        "observed_peak_bytes": claim.peak,
        "observed_high_water_bytes": claim.high_water,
        "warnings": warnings,
    })
}

/// What a running compile used, sampled once per scheduling tick.
#[derive(Default)]
pub struct Samples {
    memory: Vec<u64>,
    cores: Vec<f64>,
    last_cpu: Option<(u64, f64)>,
}
impl Samples {
    /// `cpu_usec` is the cell's cumulative CPU time, `at` a monotonic time in seconds.
    pub fn observe(&mut self, memory: u64, cpu_usec: u64, at: f64) {
        self.memory.push(memory);
        if let Some((cpu, then)) = self.last_cpu.filter(|&(_, then)| at > then) {
            self.cores
                .push(cpu_usec.saturating_sub(cpu) as f64 / 1e6 / (at - then));
        }
        self.last_cpu = Some((cpu_usec, at));
    }
}

fn percentile<T: Copy + PartialOrd>(values: &[T], fraction: f64) -> Option<T> {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let last = sorted.len().checked_sub(1)?;
    Some(sorted[((last as f64) * fraction).round() as usize])
}

/// Past twice its declared time a run ends.
pub fn overran(declared: Option<u64>, elapsed: f64) -> bool {
    declared.is_some_and(|seconds| elapsed > 2.0 * seconds as f64)
}

/// When a run started at `start` on the contract clock is declared to finish.
pub fn finish(start: f64, declared: u64) -> f64 {
    start + declared as f64
}

/// Whether a run started at `start` is declared to finish by `deadline`; one that declares no runtime never is.
pub fn finishes_by(start: f64, declared: Option<u64>, deadline: f64) -> bool {
    declared.is_some_and(|seconds| finish(start, seconds) <= deadline)
}

pub struct Run<'a> {
    pub samples: &'a Samples,
    pub peak: Option<u64>,
    pub declared_memory: u64,
    pub declared_seconds: Option<u64>,
    pub seconds: f64,
    pub succeeded: bool,
    pub overran: bool,
}

/// The measured usage and contract warnings a terminal result carries.
pub fn report(run: Run) -> Value {
    let mut warnings = Vec::new();
    let declared = run.declared_seconds.map(|seconds| seconds as f64);
    if run.overran {
        warnings.push(format!(
            "contract violation: ended after running past twice its declared {} s",
            run.declared_seconds.unwrap_or(0)
        ));
    } else if let Some(declared) = declared.filter(|&declared| run.seconds > 1.5 * declared) {
        warnings.push(format!(
            "contract violation: ran {:.0} s, more than 50% over its declared {declared:.0} s",
            run.seconds
        ));
    }
    if let Some(declared) = declared.filter(|&declared| run.succeeded && run.seconds < 0.5 * declared) {
        warnings.push(format!(
            "contract violation: ran {:.0} s, less than half its declared {declared:.0} s",
            run.seconds
        ));
    }
    if let Some(peak) = run.peak.filter(|&peak| {
        run.succeeded && run.declared_memory > 0 && (peak as f64) < 0.75 * run.declared_memory as f64
    }) {
        warnings.push(format!(
            "contract violation: peaked at {peak} bytes, more than 25% below its declared {} bytes",
            run.declared_memory
        ));
    }
    json!({
        "expected_runtime_secs": run.declared_seconds,
        "median_memory_bytes": percentile(&run.samples.memory, 0.5),
        "p90_memory_bytes": percentile(&run.samples.memory, 0.9),
        "median_cores": percentile(&run.samples.cores, 0.5).map(|cores| cores.max(1.0)),
        "p90_cores": percentile(&run.samples.cores, 0.9),
        "warnings": warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    const GIB: u64 = 1 << 30;

    fn run(samples: &Samples, seconds: f64, peak: u64) -> Run<'_> {
        Run {
            samples,
            peak: Some(peak),
            declared_memory: 4 * GIB,
            declared_seconds: Some(100),
            seconds,
            succeeded: true,
            overran: false,
        }
    }

    #[test]
    fn usage_reports_peak_percentiles_and_cores_with_a_one_core_median_floor() {
        let mut samples = Samples::default();
        for (step, memory) in [1, 2, 3, 4, 10].into_iter().enumerate() {
            samples.observe(memory * GIB, step as u64 * 500_000, step as f64);
        }
        let report = report(run(&samples, 80.0, 3 * GIB + GIB / 2));
        assert_eq!(report["median_memory_bytes"], 3 * GIB);
        assert_eq!(report["p90_memory_bytes"], 10 * GIB);
        assert_eq!(report["median_cores"], 1.0, "half a core reports as one");
        assert_eq!(report["p90_cores"], 0.5, "the 90th percentile is not rounded");
        assert_eq!(report["warnings"], json!([]));
        let mut busy = Samples::default();
        for step in 0..4 {
            busy.observe(GIB, step * 2_500_000, step as f64);
        }
        assert_eq!(super::report(run(&busy, 80.0, 4 * GIB))["median_cores"], 2.5);
    }

    #[test]
    fn every_broken_declaration_is_a_warning_and_twice_the_time_ends_the_run() {
        let samples = Samples::default();
        let warnings = |run: Run| report(run)["warnings"].as_array().unwrap().iter().map(|w| w.as_str().unwrap().to_owned()).collect::<Vec<_>>();
        assert_eq!(warnings(run(&samples, 100.0, 4 * GIB)), Vec::<String>::new());
        assert!(warnings(run(&samples, 151.0, 4 * GIB))[0].contains("more than 50% over"));
        assert!(warnings(run(&samples, 49.0, 4 * GIB))[0].contains("less than half"));
        assert!(warnings(run(&samples, 100.0, 3 * GIB - 1))[0].contains("more than 25% below"));
        assert!(!overran(Some(100), 200.0) && overran(Some(100), 200.5) && !overran(None, 1e9));
        let ended = Run { overran: true, succeeded: false, ..run(&samples, 201.0, 4 * GIB) };
        assert_eq!(warnings(ended).len(), 1, "a run ended for its time reports that alone");
        let failed = Run { succeeded: false, ..run(&samples, 10.0, GIB) };
        assert!(warnings(failed).is_empty(), "a failed run's short time and low memory break no declaration");
        assert_eq!(finish(30.0, 100), 130.0);
        assert!(finishes_by(30.0, Some(100), 130.0) && !finishes_by(30.5, Some(100), 130.0));
        assert!(!finishes_by(0.0, None, f64::INFINITY), "a run that declares no runtime never finishes by a deadline");
        assert_eq!(queuing_warnings(LARGE_REQUEST).len(), 0);
        assert!(queuing_warnings(LARGE_REQUEST + 1)[0].contains("very low priority"));
    }

    #[test]
    fn a_claim_report_warns_of_use_beyond_its_declaration_and_informs_of_a_far_lower_high_water() {
        let report = |peak, high_water| {
            claim_report(Claim {
                declared: 1000,
                peak,
                high_water,
            })
        };
        let unobserved = report(None, None);
        assert_eq!(unobserved["warnings"], json!([]));
        assert!(unobserved["observed_peak_bytes"].is_null() && unobserved["observed_high_water_bytes"].is_null());
        assert_eq!(report(Some(1000), Some(600))["warnings"], json!([]));
        let over = report(Some(1001), Some(1001));
        assert_eq!(over["observed_peak_bytes"], 1001);
        assert!(over["warnings"][0].as_str().unwrap().starts_with("used more than its declared 1000 bytes"));
        assert_eq!(over["warnings"].as_array().unwrap().len(), 1);
        let low = report(Some(100), Some(599));
        assert_eq!(
            low["warnings"][0],
            "declared 1000 bytes; observed high-water 599 bytes; the ruling's figure is about 1.25 x 599 = 748 bytes"
        );
        let large = claim_report(Claim {
            declared: LARGE_REQUEST + 1,
            peak: None,
            high_water: None,
        });
        assert!(large["warnings"][0].as_str().unwrap().contains("very low priority"));
    }
}
