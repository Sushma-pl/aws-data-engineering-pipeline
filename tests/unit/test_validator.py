"""Unit tests for src/validation/validator.py"""
import pytest
import pandas as pd
from pathlib import Path

from src.validation.validator import validate_file, validate_schema


class TestValidateFile:

    def test_raises_if_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            validate_file(str(tmp_path / "nonexistent.parquet"))

    def test_raises_if_not_parquet(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("a,b,c")
        with pytest.raises(ValueError, match="Unsupported file format"):
            validate_file(str(f))

    def test_raises_if_empty_file(self, tmp_path):
        f = tmp_path / "empty.parquet"
        f.write_bytes(b"")
        with pytest.raises(ValueError, match="empty"):
            validate_file(str(f))

    def test_returns_path_for_valid_file(self, sample_source_file):
        result = validate_file(sample_source_file)
        assert isinstance(result, Path)


class TestValidateSchema:

    def test_raises_for_missing_columns(self, tmp_path):
        df = pd.DataFrame({"VendorID": [1], "trip_distance": [2.0]})
        p = tmp_path / "partial.parquet"
        df.to_parquet(p, index=False)
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_schema(str(p))

    def test_raises_for_empty_dataframe(self, tmp_path):
        from src.validation.validator import REQUIRED_COLUMNS
        df = pd.DataFrame(columns=list(REQUIRED_COLUMNS))
        p = tmp_path / "empty_schema.parquet"
        df.to_parquet(p, index=False)
        with pytest.raises(ValueError, match="empty"):
            validate_schema(str(p))

    def test_returns_dataframe_for_valid_file(self, sample_source_file):
        result = validate_schema(sample_source_file)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
