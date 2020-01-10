fn main() {
    build_clients();
    build_servers();
}

fn build_servers() {
    tonic_build::configure()
        .build_client(false)
        .out_dir("db/src/api")
        .compile(&["proto/storage-api.proto"], &["proto"])
        .expect("Failed to compile protos");
}

fn build_clients() {
    tonic_build::configure()
        .build_server(false)
        .out_dir("front-end/src/api")
        .compile(&["proto/storage-api.proto"], &["proto"])
        .expect("Failed to compile protos");
}
