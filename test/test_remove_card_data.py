from unittest import mock
from src.data_sensitivity.remove_card_data import remove_card_data
from unittest.mock import Mock

def test_column_removed():
    mock_df = Mock()
    mock_df.return_value = {}
    remove_card_data(mock_df)
    mock_df.drop.assert_called_with(columns = ['card_no'], inplace=True)
