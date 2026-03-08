use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about = "A distributed queue")]
pub(crate) struct QConfig {
    /// Port to listen on
    #[arg(short, long, default_value_t = 50051)]
    pub(crate) port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_port() {
        // Test that the parser handles default values correctly
        let cfg = QConfig::try_parse_from(["test_app"]).unwrap();
        assert_eq!(cfg.port, 50051);
    }

    #[test]
    fn test_custom_port() {
        // Test overriding the port via long flag
        let cfg = QConfig::try_parse_from(["test_app", "--port", "8080"]).unwrap();
        assert_eq!(cfg.port, 8080);
    }

    #[test]
    fn test_short_port_flag() {
        // Test overriding the port via short flag
        let cfg = QConfig::try_parse_from(["test_app", "-p", "9000"]).unwrap();
        assert_eq!(cfg.port, 9000);
    }

    #[test]
    fn test_invalid_port() {
        // Ensure non-numeric ports cause a failure
        let result = QConfig::try_parse_from(["test_app", "-p", "not_a_number"]);
        assert!(result.is_err());
    }
}
