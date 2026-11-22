//! What a holder was told, once each: the GPU usage contract rules its grants broke and the card losses linked to
//! its runs, and whether every gpu-switch it reached could tell it of a loss. A lease and a runner each keep one.

use crate::protocol::{DeviceLossReport, GrantLoss, Violation};
use serde::{Deserialize, Serialize};

/// One notice a holder hears.
pub enum Notice {
    Violation(Violation),
    DeviceLost(GrantLoss),
}

/// A session's capability history: whether every gpu-switch that it reached could report a card loss. It is
/// session-wide, so a process that inherits the session inherits it with the session. A session is as unreported as
/// its worst connection, hence the order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Every connection's lease reply declared card-loss protocol 1.
    Reportable,
    /// A connection reached a gpu-switch that does not report card losses, or the history is not known.
    #[default]
    Unreported,
}

impl Capability {
    /// What a connection whose lease reply declared card-loss protocol `protocol` adds to the history.
    fn of(protocol: u64) -> Capability {
        match protocol {
            1 => Capability::Reportable,
            _ => Capability::Unreported,
        }
    }
}

/// What the holder was told: each GPU usage contract rule its grant broke and each loss of one of its runs, once.
#[derive(Default)]
pub struct Notices {
    violations: Vec<Violation>,
    losses: Vec<GrantLoss>,
    capability: Capability,
    /// The warning of each broken rule, as it was printed.
    pub warnings: Vec<String>,
}

impl Notices {
    /// The notice's text the first time it is heard, nothing for a repeat: a violation by its rule and run, a loss
    /// by its run and its interval's identity. Replays and a loss heard again in the final report print once.
    pub fn hear(&mut self, notice: Notice, holder: &str) -> Option<String> {
        match notice {
            Notice::Violation(violation) => match self.violations.iter().any(|told| told.same(&violation)) {
                true => None,
                false => {
                    let warning = violation.warning(holder);
                    self.warnings.push(warning.clone());
                    self.violations.push(violation);
                    Some(warning)
                }
            },
            Notice::DeviceLost(grant) => match self.losses.iter().any(|told| told.same(&grant)) {
                true => None,
                false => {
                    let notice = grant.loss.notice(holder, grant.run);
                    self.losses.push(grant);
                    Some(notice)
                }
            },
        }
    }

    /// The record of a session whose first connection declared card-loss protocol `protocol`.
    pub fn of(protocol: u64) -> Notices {
        Notices::inherited(Capability::of(protocol))
    }

    /// The record of a session that this process inherited, with the capability history that the session had.
    pub fn inherited(capability: Capability) -> Notices {
        Notices { capability, ..Notices::default() }
    }

    /// A connection to a gpu-switch whose lease reply declared card-loss protocol `protocol`.
    pub fn connected(&mut self, protocol: u64) {
        self.capability = self.capability.max(Capability::of(protocol));
    }

    /// The session's capability history, for the process that inherits the session.
    pub fn capability(&self) -> Capability {
        self.capability
    }

    /// The session's loss: the first association heard, else unreported if any connection could not report one.
    pub fn report(&self) -> DeviceLossReport {
        match (self.losses.first(), self.capability) {
            (Some(grant), _) => DeviceLossReport::Lost(grant.clone()),
            (None, Capability::Unreported) => DeviceLossReport::Unreported,
            (None, Capability::Reportable) => DeviceLossReport::None,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::DeviceLoss;

    fn loss(start_ticks: u64, number: u64) -> DeviceLoss {
        DeviceLoss {
            incarnation: crate::process_identity::Instance { boot_id: "b".into(), pid: 7, start_ticks },
            number,
            last_good_began: Some(1.0),
            read_began: 2.0,
            observed: 3.0,
            published: 4.0,
            cause: "nvmlDeviceGetHandleByIndex_v2: NVML: device fell off the bus".into(),
        }
    }

    #[test]
    fn a_notice_heard_again_after_a_reattachment_or_in_the_final_report_prints_once() {
        let mut notices = Notices::of(1);
        let grant = GrantLoss { run: 1, loss: loss(1, 1) };
        let first = notices.hear(Notice::DeviceLost(grant.clone()), "holder");
        assert_eq!(first.as_deref(), Some(grant.loss.notice("holder", 1).as_str()));
        assert_eq!(notices.hear(Notice::DeviceLost(grant.clone()), "holder"), None, "a replay prints nothing");
        assert_eq!(notices.report(), DeviceLossReport::Lost(grant));
    }

    #[test]
    fn a_new_incarnations_loss_of_the_same_run_is_new_and_its_replay_prints_nothing() {
        let mut notices = Notices::of(1);
        let before = GrantLoss { run: 4, loss: loss(1, 1) };
        let after = GrantLoss { run: 4, loss: loss(2, 1) };
        assert!(notices.hear(Notice::DeviceLost(before.clone()), "holder").is_some());
        assert!(notices.hear(Notice::DeviceLost(after.clone()), "holder").is_some(), "(I2, 1) is new");
        assert!(notices.hear(Notice::DeviceLost(after), "holder").is_none());
        assert_eq!(notices.report(), DeviceLossReport::Lost(before), "the first association heard");
    }

    #[test]
    fn a_connection_without_the_capability_is_unreported_from_then_on_unless_a_loss_was_heard() {
        let mut notices = Notices::of(1);
        assert_eq!(notices.report(), DeviceLossReport::None);
        notices.connected(0);
        notices.connected(1);
        assert_eq!(notices.report(), DeviceLossReport::Unreported, "sticky");
        let mut heard = Notices::of(1);
        let grant = GrantLoss { run: 1, loss: loss(1, 1) };
        heard.hear(Notice::DeviceLost(grant.clone()), "holder");
        heard.connected(0);
        assert_eq!(heard.report(), DeviceLossReport::Lost(grant));
    }
}
