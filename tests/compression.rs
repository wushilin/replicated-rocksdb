//! Optional zstd compression of replication traffic (PROTOCOL.md §3.4): every combination of
//! settings replicates, and the bytes on the wire show whether compression is in effect.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use replicated_rocksdb::{
    CfSpec, LogApi, Master, Slave, UpstreamConfig, VerifyMode, WireCompression,
};
use support::{Cluster, assert_converged, assert_same, dump};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const KEYS: u32 = 1000;
const VALUE: usize = 1024;

/// A TCP forwarder in front of `target` that counts the bytes flowing from the server back to
/// the client.
async fn counting_proxy(target: SocketAddr) -> (SocketAddr, Arc<AtomicU64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let down = Arc::new(AtomicU64::new(0));
    let counter = down.clone();
    tokio::spawn(async move {
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            let counter = counter.clone();
            tokio::spawn(async move {
                let Ok(server) = TcpStream::connect(target).await else {
                    return;
                };
                let (mut sr, mut sw) = server.into_split();
                let (mut cr, mut cw) = client.split();
                let up = async {
                    let mut buf = vec![0u8; 64 << 10];
                    while let Ok(n) = cr.read(&mut buf).await {
                        if n == 0 || sw.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                };
                let back = async {
                    let mut buf = vec![0u8; 64 << 10];
                    while let Ok(n) = sr.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        counter.fetch_add(n as u64, Ordering::Relaxed);
                        if cw.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                };
                tokio::join!(up, back);
            });
        }
    });
    (addr, down)
}

fn write_data(m: &Master) {
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..KEYS {
        m.put("c", &i.to_be_bytes(), &vec![b'a' + (i % 7) as u8; VALUE])
            .unwrap();
    }
}

async fn run(server: WireCompression, client: WireCompression, bootstrap: bool) {
    let c = Cluster::new();
    let mut mc = c.master_config("m");
    mc.serve.as_mut().unwrap().compression = server;
    let m = Master::open(mc).await.unwrap();
    if bootstrap {
        write_data(&m);
    }
    let (proxy, down) = counting_proxy(m.serve_addr().unwrap()).await;
    let (mut sc, listener) = c.slave_config("s", proxy);
    let mut up = UpstreamConfig::from_addr(proxy);
    up.compression = client;
    sc.upstream = up;
    let s = Slave::open(sc).await.unwrap();
    if !bootstrap {
        write_data(&m);
    }
    assert_converged(&m, &[("s", &s)]).await;
    assert_same("data", &dump(&m), &dump(&s));
    let r = s.verify_now(VerifyMode::Full).await.unwrap();
    assert!(r.is_clean(), "{r:?}");
    listener.assert_no_violations();
    if bootstrap {
        assert_eq!(listener.bootstraps(), 1);
    }
    let raw = (KEYS as u64) * (VALUE as u64);
    let seen = down.load(Ordering::Relaxed);
    if server == WireCompression::Zstd {
        assert!(seen < raw / 4, "{seen} bytes from a zstd server, raw {raw}");
    } else {
        assert!(seen > raw, "{seen} bytes from a plain server, raw {raw}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_works_in_every_combination() {
    use WireCompression::{None, Zstd};
    for (s, c) in [(Zstd, None), (None, Zstd), (Zstd, Zstd), (None, None)] {
        run(s, c, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_works_in_every_combination() {
    use WireCompression::{None, Zstd};
    for (s, c) in [(Zstd, None), (None, Zstd), (Zstd, Zstd)] {
        run(s, c, true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mixed_chain_forwards_entries_unchanged() {
    let c = Cluster::new();
    let mut mc = c.master_config("a");
    mc.serve.as_mut().unwrap().compression = WireCompression::Zstd;
    let a = Master::open(mc).await.unwrap();
    write_data(&a);

    let (mut bc, _) = c.slave_config("b", a.serve_addr().unwrap());
    let mut up = UpstreamConfig::from_addr(a.serve_addr().unwrap());
    up.compression = WireCompression::Zstd;
    bc.upstream = up;
    bc.serve.as_mut().unwrap().compression = WireCompression::None;
    let b = Slave::open(bc).await.unwrap();
    let (cn, _) = c.slave("c", b.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    for i in 0..20u8 {
        a.put("c", &[b'l', i], &[i; VALUE]).unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let from = cn.first_retained_seq() - 1;
    let ours: Vec<_> = cn
        .entries_since(from)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let origin: Vec<_> = a.entries_since(from).unwrap().map(Result::unwrap).collect();
    assert!(!ours.is_empty());
    assert_eq!(ours, origin);
    assert_same("data", &dump(&a), &dump(&cn));
}
