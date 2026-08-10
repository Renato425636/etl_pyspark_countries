import json
from unittest.mock import MagicMock, patch

import pytest


def test_fetch_countries_saves_json(tmp_path):
    from src.extract.countries_api import fetch_countries

    mock_response = MagicMock()
    mock_response.json.return_value = [{"name": {"common": "Brazil"}}]
    mock_response.raise_for_status.return_value = None

    output = str(tmp_path / "countries.json")
    with patch("src.extract.countries_api.requests.get", return_value=mock_response):
        fetch_countries("http://example.com/api", output)

    with open(output) as f:
        data = json.load(f)
    assert data[0]["name"]["common"] == "Brazil"


def test_fetch_countries_raises_on_http_error(tmp_path):
    from requests.exceptions import HTTPError

    from src.extract.countries_api import fetch_countries

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = HTTPError(response=MagicMock(status_code=500, text="error"))

    with patch("src.extract.countries_api.requests.get", return_value=mock_response):
        with pytest.raises(HTTPError):
            fetch_countries("http://example.com/api", str(tmp_path / "out.json"))


def test_load_config(tmp_path):
    from src.config.settings import load_config

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("pipeline_name: test\nlog_level: DEBUG\n")
    result = load_config(str(cfg_file))
    assert result["pipeline_name"] == "test"
    assert result["log_level"] == "DEBUG"
