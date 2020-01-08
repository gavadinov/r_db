fn main() {
    tonic_build::configure()
        .build_client(false)
        .out_dir("db/src/api")
        .compile(&["proto/storage-api.proto"], &["proto"])
        .expect("Failed to compile protos");
}
