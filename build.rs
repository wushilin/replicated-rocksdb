fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .bytes(".rrdb.replication.v1.LogEntry.ops")
        .compile_protos(&["proto/replication.proto", "proto/local.proto"], &["proto"])?;
    Ok(())
}
