fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/meta/v1/meta.proto", "proto/chunk/v1/chunk.proto"],
            &["proto"],
        )?;
    Ok(())
}
