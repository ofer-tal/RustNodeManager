fn main() -> Result<(), Box<dyn std::error::Error>> {
    // On Windows, try to find protoc if PROTOC is not set
    #[cfg(windows)]
    {
        if std::env::var("PROTOC").is_err() {
            let candidates = [
                r"C:\protoc\bin\protoc.exe",
                r"C:\Program Files\protoc\bin\protoc.exe",
            ];
            for path in &candidates {
                if std::path::Path::new(path).exists() {
                    std::env::set_var("PROTOC", path);
                    break;
                }
            }
        }
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(
            ".",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(&["../../proto/node_manager.proto"], &["../../proto"])?;
    Ok(())
}
