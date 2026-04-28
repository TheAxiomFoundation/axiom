"""Tests for the Supabase storage backend.

All database calls are mocked since we don't have a real Supabase instance.
"""

import os
from unittest.mock import patch

import pytest

# Import with graceful handling since psycopg may not be installed
try:
    from axiom.storage.supabase import HAS_PSYCOPG, SupabaseStorage, get_db_url
    _IMPORT_OK = True
except ImportError:
    _IMPORT_OK = False
    HAS_PSYCOPG = False

pytestmark = pytest.mark.skipif(not _IMPORT_OK, reason="Supabase storage not importable")


class TestGetDbUrl:
    def test_missing_env_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="AXIOM_SUPABASE_DB_URL"):
                get_db_url()

    def test_env_set(self):
        with patch.dict(os.environ, {"AXIOM_SUPABASE_DB_URL": "postgresql://test"}):
            url = get_db_url()
            assert url == "postgresql://test"


class TestSupabaseStorageInit:
    @pytest.mark.skipif(not HAS_PSYCOPG, reason="psycopg not installed")
    def test_init_with_url(self):
        with patch("axiom.storage.supabase.psycopg"):
            storage = SupabaseStorage(db_url="postgresql://test")
            assert storage.db_url == "postgresql://test"
            assert storage._conn is None

    @pytest.mark.skipif(not HAS_PSYCOPG, reason="psycopg not installed")
    def test_init_from_env(self):
        with patch.dict(os.environ, {"AXIOM_SUPABASE_DB_URL": "postgresql://env-test"}):
            with patch("axiom.storage.supabase.psycopg"):
                storage = SupabaseStorage()
                assert storage.db_url == "postgresql://env-test"

    @pytest.mark.skipif(HAS_PSYCOPG, reason="Test only when psycopg is not installed")
    def test_init_without_psycopg(self):
        with pytest.raises(ImportError, match="psycopg"):
            SupabaseStorage(db_url="postgresql://test")
