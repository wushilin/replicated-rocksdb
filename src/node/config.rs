//! `NodeConfig`.

use crate::config::MasterConfig;
use crate::error::{Error, Result};
use crate::slave::{SlaveConfig, UpstreamConfig};

use super::types::AcceptSplitBrainRisk;

/// Configuration of a [`Node`](crate::Node). `master` is used whenever the node runs as master,
/// `slave` whenever it runs as slave (`slave.upstream` is the default upstream). Both must
/// name the same directory and identifier.
#[derive(Clone)]
pub struct NodeConfig {
    pub master: MasterConfig,
    pub slave: SlaveConfig,
    /// The other nodes of the topology, asked by `Node::promote_checked` and by the fence
    /// check of a stored master.
    pub peers: Vec<UpstreamConfig>,
    /// Skip the fence check: a stored master opens as master at once, even if another master
    /// is running. Explicit consent; `None` by default.
    pub unchecked_master: Option<AcceptSplitBrainRisk>,
}

impl NodeConfig {
    /// No peers and no consent to skip the fence check.
    pub fn new(master: MasterConfig, slave: SlaveConfig) -> NodeConfig {
        NodeConfig {
            master,
            slave,
            peers: Vec::new(),
            unchecked_master: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.master.path != self.slave.path {
            return Err(Error::Config(
                "NodeConfig: master.path and slave.path must be the same directory".into(),
            ));
        }
        if self.master.identifier != self.slave.identifier {
            return Err(Error::Config(
                "NodeConfig: master.identifier and slave.identifier must be the same".into(),
            ));
        }
        Ok(())
    }
}
