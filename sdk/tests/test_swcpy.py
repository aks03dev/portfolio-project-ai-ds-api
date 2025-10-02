import pytest
from swcpy import SWCClient 
from swcpy import SWCConfig
from swcpy.schemas import League, Team, Player, Performance
from io import BytesIO 
import pyarrow.parquet as pq 
import pandas as pd 
 
def test_health_check(httpx_mock):
    """Tests health check from SDK"""
    httpx_mock.add_response(status_code=200, json={"status": "ok"})
    config = SWCConfig(swc_base_url="http://testurl", backoff=False)
    client = SWCClient(config)
    response = client.get_health_check()
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_list_leagues(httpx_mock):
    """Tests get leagues from SDK"""
    mock_leagues = [
        {
            "league_id": 1,
            "league_name": "Test League",
            "scoring_type": "PPR",
            "last_changed_date": "2025-10-02T00:00:00",
        }
    ]
    httpx_mock.add_response(status_code=200, json=mock_leagues)
    config = SWCConfig(swc_base_url="http://testurl", backoff=False)
    client = SWCClient(config)
    leagues = client.list_leagues()
    assert isinstance(leagues, list)
    assert len(leagues) > 0
    assert leagues[0].league_name == "Test League"


def test_bulk_player_file_parquet(httpx_mock, tmp_path):
    """Tests bulk player download through SDK - Parquet"""
    mock_data = pd.DataFrame({"player_id": [1], "name": ["Player One"]})
    
    # The bulk file download uses a hardcoded URL, so we mock that
    player_file_url = "https://raw.githubusercontent.com/aks03dev/portfolio-project/main/bulk/player_data.parquet"
    httpx_mock.add_response(
        url=player_file_url, status_code=200, content=mock_data.to_parquet(index=False)
    )
    
    config = SWCConfig(swc_base_url="http://testurl", bulk_file_format="parquet")
    client = SWCClient(config)
    
    # Define the output file path
    output_file = tmp_path / "player_data.parquet"
    
    # Call the method to download the file
    result = client.get_bulk_player_file(file_path=str(output_file))
    
    # Assert that the method returns None and the file is created
    assert result is None
    assert output_file.exists()
    
    # Verify the content of the created file
    df = pd.read_parquet(output_file)
    assert not df.empty
    pd.testing.assert_frame_equal(df, mock_data)
