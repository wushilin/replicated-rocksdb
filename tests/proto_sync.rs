//! `proto/replication.proto` must be a verbatim copy of PROTOCOL.md §14.

#[test]
fn proto_file_matches_protocol_spec() {
    let root = env!("CARGO_MANIFEST_DIR");
    let spec = std::fs::read_to_string(format!("{root}/PROTOCOL.md")).unwrap();
    let section = spec.split("\n## 14.").nth(1).expect("PROTOCOL.md has a §14");
    let open = "```proto\n";
    let start = section.find(open).expect("§14 has a proto block") + open.len();
    let len = section[start..].find("\n```").expect("proto block is closed");
    let from_spec = &section[start..start + len + 1];
    let file = std::fs::read_to_string(format!("{root}/proto/replication.proto"))
        .expect("proto/replication.proto exists");
    assert_eq!(
        file, from_spec,
        "proto/replication.proto must be a verbatim copy of PROTOCOL.md §14"
    );
}
