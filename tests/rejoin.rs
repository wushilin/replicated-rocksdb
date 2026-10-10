//! `Slave::rejoin`: a former master becomes a slave without wiping its directory
//! (DESIGN.md §13.2).

mod support;

use std::sync::Arc;

use replicated_rocksdb::{
    AcceptDataLoss, Error, HaltReason, Master, ReadApi, RejoinOptions, RepointDivergence,
    RepointOutcome, Role, Slave, SlaveState,
};
use support::{Cluster, RecordingListener, assert_converged, failover};

async fn rejoin(
    c: &Cluster,
    b: &Master,
    opts: RejoinOptions,
) -> (Slave, Result<RepointOutcome, Error>, Arc<RecordingListener>) {
    let (cfg, listener) = c.slave_config("a", b.serve_addr().unwrap());
    let (slave, outcome) = tokio::time::timeout(support::WAIT, Slave::rejoin(cfg, opts))
        .await
        .expect("rejoin settles")
        .expect("the slave is opened");
    (slave, outcome, listener)
}

/// The old master is behind the new one (the new master wrote after the promotion): it
/// follows the new master's epoch without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_former_master_behind_the_new_one_advances_without_a_bootstrap() {
    let c = Cluster::new();
    let (_, b, _) = failover(&c, false).await;
    b.put("c", b"after failover", b"x").unwrap();
    let (a, outcome, la) = rejoin(&c, &b, RejoinOptions::default()).await;
    assert_eq!(outcome.unwrap(), RepointOutcome::EpochAdvanced);
    assert_converged(&b, &[("a", &a)]).await;
    assert_eq!(la.bootstraps(), 0, "no bootstrap");
    let history = a.promotion_info().epoch_history;
    assert_eq!(
        history[..],
        b.epoch_history()[..history.len()],
        "a prefix of B's"
    );
    assert_eq!(a.promotion_info().role, Role::Slave);
    b.put("c", b"later", b"y").unwrap();
    assert_converged(&b, &[("a", &a)]).await;
}

/// The old master restarted after the failover without writes: its per-open epoch (same
/// number as B's promotion epoch, another nonce) is dropped and burned, and it still follows
/// B without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trailing_empty_epoch_is_burned_and_the_old_master_advances() {
    let c = Cluster::new();
    let (a_cfg, b, _) = failover(&c, false).await;
    let restarted = Master::open(a_cfg).await.unwrap();
    let restart_epoch = restarted.identity().epoch;
    assert_eq!(
        restart_epoch,
        b.identity().epoch,
        "the same number, another nonce"
    );
    restarted.shutdown().await.unwrap();
    let (a, outcome, la) = rejoin(&c, &b, RejoinOptions::default()).await;
    assert_eq!(outcome.unwrap(), RepointOutcome::EpochAdvanced);
    assert_eq!(a.promotion_info().max_burned_epoch, restart_epoch);
    assert_converged(&b, &[("a", &a)]).await;
    assert_eq!(la.bootstraps(), 0);
}

/// The old master holds entries B never saw: refused by default, halted with its data intact,
/// the suffix salvageable, and the halt survives a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn divergence_is_refused_by_default_and_nothing_is_deleted() {
    let c = Cluster::new();
    let (_, b, fork) = failover(&c, true).await;
    let (a, outcome, _) = rejoin(&c, &b, RejoinOptions::default()).await;
    let a_last = a.last_seq();
    match outcome {
        Err(Error::Diverged {
            fork_seq,
            local_last_seq,
            divergent_entries,
        }) => {
            assert_eq!(
                (fork_seq, local_last_seq, divergent_entries),
                (fork, a_last, 2)
            )
        }
        other => panic!("expected Diverged, got {other:?}"),
    }
    assert_eq!(a.status().state, SlaveState::Halted(HaltReason::Diverged));
    assert_eq!(a.get("c", b"only on a").unwrap(), Some(b"2".to_vec()));
    assert_eq!(
        a.diverged_entries()
            .unwrap()
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        (fork..=a_last).collect::<Vec<_>>()
    );
    let (cfg, _) = c.slave_config("a", b.serve_addr().unwrap());
    let a = c.restart_slave(a, cfg).await;
    assert_eq!(
        a.status().state,
        SlaveState::Halted(HaltReason::Diverged),
        "persisted"
    );
    assert_eq!(a.last_seq(), a_last, "nothing deleted");
}

/// With `Repair { AcceptDataLoss }` only the divergent suffix is discarded and the old master
/// ends up equal to the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_with_consent_discards_only_the_divergent_suffix() {
    let c = Cluster::new();
    let (_, b, fork) = failover(&c, true).await;
    let opts = RejoinOptions {
        on_divergence: RepointDivergence::Repair {
            accept_loss: AcceptDataLoss,
        },
    };
    let (a, outcome, la) = rejoin(&c, &b, opts).await;
    assert_eq!(
        outcome.unwrap(),
        RepointOutcome::Repaired {
            discarded: fork..=fork + 1
        }
    );
    assert_converged(&b, &[("a", &a)]).await;
    assert_eq!(a.get("c", b"only on a").unwrap(), None);
    assert_eq!(la.bootstraps(), 0, "repaired, not bootstrapped");
}

/// Refusals before the rewrite leave the directory unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_refuses_a_slave_directory_an_empty_one_and_an_invalid_config() {
    let c = Cluster::new();
    let (_, b, _) = failover(&c, false).await;
    let b_addr = b.serve_addr().unwrap();
    // A slave directory.
    let (s, _) = c.slave("s", b_addr).await;
    s.shutdown().await.unwrap();
    let (cfg, _) = c.slave_config("s", b_addr);
    assert!(matches!(
        Slave::rejoin(cfg, RejoinOptions::default()).await,
        Err(Error::WrongRole {
            expected: Role::Master,
            found: Role::Slave
        })
    ));
    // No database.
    let (cfg, _) = c.slave_config("empty", b_addr);
    assert!(matches!(
        Slave::rejoin(cfg, RejoinOptions::default()).await,
        Err(Error::Config(_))
    ));
    // Invalid config: the stored master stays a master.
    let (mut cfg, _) = c.slave_config("a", b_addr);
    cfg.identifier = "bad id".into();
    assert!(matches!(
        Slave::rejoin(cfg, RejoinOptions::default()).await,
        Err(Error::Config(_))
    ));
    let m = Master::open(c.master_config("a"))
        .await
        .expect("still a master");
    m.shutdown().await.unwrap();
}

/// The directory is still open by a running master in this process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_refuses_a_directory_that_is_still_open() {
    let c = Cluster::new();
    let (a_cfg, b, _) = failover(&c, false).await;
    let a = Master::open(a_cfg).await.unwrap();
    let id = a.identity();
    let (cfg, _) = c.slave_config("a", b.serve_addr().unwrap());
    assert!(Slave::rejoin(cfg, RejoinOptions::default()).await.is_err());
    assert_eq!(a.identity(), id);
    a.put("c", b"still writing", b"1")
        .expect("the master is untouched");
    a.shutdown().await.unwrap();
    let a = Master::open(c.master_config("a"))
        .await
        .expect("still stored as master");
    a.shutdown().await.unwrap();
}
