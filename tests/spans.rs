//! Tracing spans, end to end (DESIGN.md §19). The recorder is installed per test, on every thread of the
//! test's own runtime (`set_default`), never as the global default.

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

use replicated_rocksdb::{CfSpec, VerifyMode};
use support::{Cluster, assert_converged};

/// Every span opened in this test binary: its name and recorded fields (values as text).
#[derive(Default)]
struct Capture {
    spans: Mutex<Vec<(String, BTreeMap<String, String>)>>,
}

impl Capture {
    /// Field maps of the spans named `name` opened by node `node_id`.
    fn find(&self, name: &str, node_id: &str) -> Vec<BTreeMap<String, String>> {
        let spans = self.spans.lock().unwrap();
        spans
            .iter()
            .filter(|(n, f)| n == name && f.get("node_id").map(String::as_str) == Some(node_id))
            .map(|(_, f)| f.clone())
            .collect()
    }
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

/// A minimal subscriber that records spans and ignores events.
struct Recorder(Arc<Capture>);

impl Subscriber for Recorder {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        let mut spans = self.0.spans.lock().unwrap();
        spans.push((attrs.metadata().name().to_string(), fields));
        Id::from_u64(spans.len() as u64)
    }

    fn record(&self, id: &Id, values: &Record<'_>) {
        let mut spans = self.0.spans.lock().unwrap();
        if let Some((_, fields)) = spans.get_mut(id.into_u64() as usize - 1) {
            values.record(&mut Fields(fields));
        }
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// Both ends of a handshake, a bootstrap and a verify session, and a reaper round, open a span
/// with `node_id`, `upstream` (absent on the master), `seq` and `epoch`.
#[test]
fn spans_carry_node_upstream_seq_and_epoch() {
    let capture = Arc::new(Capture::default());
    let dispatch = tracing::Dispatch::new(Recorder(capture.clone()));
    // The runtime's workers and blocking threads each install the dispatcher for their own
    // lifetime, so the test never touches the process-wide default.
    let start = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .on_thread_start(move || {
            let guard = tracing::dispatcher::set_default(&start);
            GUARD.with(|g| *g.borrow_mut() = Some(guard));
        })
        .on_thread_stop(|| GUARD.with(|g| drop(g.borrow_mut().take())))
        .build()
        .unwrap();
    let _main = tracing::dispatcher::set_default(&dispatch);
    rt.block_on(scenario(&capture));
}

thread_local! {
    static GUARD: std::cell::RefCell<Option<tracing::dispatcher::DefaultGuard>> =
        const { std::cell::RefCell::new(None) };
}

async fn scenario(capture: &Capture) {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    a.put_with_ttl("c", b"t", b"v", Duration::from_secs(1))
        .unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b)]).await;
    assert!(b.verify_now(VerifyMode::Quick).await.unwrap().is_clean());
    c.clock.advance(Duration::from_secs(2));
    assert_eq!(a.reap_expired().unwrap().deleted, 1);

    let upstream = format!("http://{}", a.serve_addr().unwrap());
    let epoch = a.identity().epoch.to_string();
    for (name, node) in [
        ("handshake", "a"),
        ("bootstrap", "a"),
        ("verify", "a"),
        ("reaper", "a"),
        ("handshake", "b"),
        ("bootstrap", "b"),
        ("verify", "b"),
    ] {
        let spans = capture.find(name, node);
        assert!(!spans.is_empty(), "no {name} span on {node}");
        for f in &spans {
            assert!(f.contains_key("seq"), "{name} on {node}: {f:?}");
            if node == "b" {
                assert_eq!(f.get("upstream"), Some(&upstream), "{name} on {node}");
            } else {
                assert!(
                    !f.contains_key("upstream"),
                    "the master has no upstream: {f:?}"
                );
            }
        }
    }
    for (name, node) in [
        ("bootstrap", "a"),
        ("verify", "a"),
        ("reaper", "a"),
        ("verify", "b"),
        ("handshake", "a"),
    ] {
        assert!(
            capture
                .find(name, node)
                .iter()
                .any(|f| f.get("epoch") == Some(&epoch)),
            "{name} on {node} has no epoch"
        );
    }
    let clients = |name: &str| {
        capture
            .find(name, "a")
            .into_iter()
            .filter_map(|f| f.get("client").cloned())
            .collect::<Vec<_>>()
    };
    assert!(clients("handshake").contains(&"b".to_string()));
    assert!(clients("bootstrap").contains(&"b".to_string()));
    assert!(
        capture
            .find("verify", "b")
            .iter()
            .any(|f| f.get("mode").map(String::as_str) == Some("Quick"))
    );
}
