from tick_vault import cli, config


def test_cli_flags_preserve_existing_vault_and_network_settings(tmp_path):
    original = config.CONFIG.model_dump()
    config.reload_config(
        base_directory=tmp_path,
        fetch_max_retry_attempts=0,
        datafeed_base_url="https://mock.invalid/datafeed/",
        env_file=None,
    )
    try:
        args = cli.build_parser().parse_args(
            [
                "download",
                "EURUSD",
                "2024-01-02",
                "--quiet",
                "--workers",
                "2",
            ]
        )
        cli._configure(args)
        assert config.CONFIG.base_directory == tmp_path
        assert config.CONFIG.fetch_max_retry_attempts == 0
        assert config.CONFIG.datafeed_base_url == "https://mock.invalid/datafeed/"
        assert config.CONFIG.worker_per_proxy == 2
        assert config.CONFIG.console_log_level == "ERROR"
    finally:
        config.reload_config(**original, env_file=None)
