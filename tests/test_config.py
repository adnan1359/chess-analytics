from chess_analytics import config as config_mod


def test_env_override_coerces_types(monkeypatch):
    monkeypatch.setenv("CHESS_API__MIN_INTERVAL_SEC", "0.5")
    monkeypatch.setenv("CHESS_INGESTION__TOP_PLAYERS", "10")
    monkeypatch.setenv("CHESS_API__USE_OS_TRUST_STORE", "false")
    monkeypatch.setenv("CHESS_INGESTION__TITLED_CATEGORIES", "GM,IM")
    config_mod.load_config.cache_clear()

    cfg = config_mod.load_config()
    assert cfg["api"]["min_interval_sec"] == 0.5          # float
    assert cfg["ingestion"]["top_players"] == 10          # int
    assert cfg["api"]["use_os_trust_store"] is False      # bool
    assert cfg["ingestion"]["titled_categories"] == ["GM", "IM"]  # list

    config_mod.load_config.cache_clear()


def test_unknown_env_key_is_ignored(monkeypatch):
    monkeypatch.setenv("CHESS_DOES_NOT_EXIST", "x")
    config_mod.load_config.cache_clear()
    cfg = config_mod.load_config()   # must not raise
    assert "api" in cfg
    config_mod.load_config.cache_clear()
