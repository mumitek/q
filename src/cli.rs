use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about = "A distributed queue")]
pub(crate) struct QConfig {
    /// Host to listen on
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) host: String,

    /// Port to listen on
    #[arg(short, long, default_value_t = 50051)]
    pub(crate) port: u16,

    /// Directory where all the data will be stored.
    #[arg(short, long)]
    pub(crate) data_dir: String,

    /// Number of partitions to provision
    #[arg(short, long)]
    pub(crate) num_partition: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_port() {
        // Test that the parser handles default values correctly
        let cfg =
            QConfig::try_parse_from(["test_app", "--data-dir", "/tmp/", "--num-partition", "8"])
                .unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 50051);
        assert_eq!(cfg.data_dir, "/tmp/");
        assert_eq!(cfg.num_partition, 8);
    }

    #[test]
    fn test_custom_input() {
        // Test overriding the port via long flag
        let cfg = QConfig::try_parse_from([
            "test_app",
            "--host",
            "localhost",
            "--port",
            "8080",
            "--data-dir",
            "/tmp/",
            "--num-partition",
            "8",
        ])
        .unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 8080);
        assert_eq!(cfg.data_dir, "/tmp/");
        assert_eq!(cfg.num_partition, 8);
    }

    #[test]
    fn test_short_flag() {
        // Test overriding the port via short flag
        let cfg = QConfig::try_parse_from([
            "test_app",
            "-p",
            "9000",
            "-d",
            "/tmp/",
            "--num-partition",
            "8",
        ])
        .unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 9000);
        assert_eq!(cfg.data_dir, "/tmp/");
        assert_eq!(cfg.num_partition, 8);
    }

    #[test]
    fn test_invalid_port() {
        // Ensure non-numeric ports cause a failure
        let cfg = QConfig::try_parse_from([
            "test_app",
            "--data-dir",
            "/tmp/",
            "-p",
            "not_a_number",
            "--num-partition",
            "8",
        ]);
        assert!(cfg.is_err());
    }

    #[test]
    fn test_no_directory() {
        // Ensure non-numeric ports cause a failure
        let cfg = QConfig::try_parse_from(["test_app"]);
        assert!(cfg.is_err());
    }
}
