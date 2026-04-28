"""Tests for the storage base module."""

import pytest

from axiom.storage.base import StorageBackend


class TestStorageBackend:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            StorageBackend()

    def test_concrete_subclass(self):
        class ConcreteStorage(StorageBackend):
            def store_section(self, section):
                pass

            def get_section(self, title, section, subsection=None, as_of=None):
                return None

            def search(self, query, title=None, limit=20):
                return []

            def list_titles(self):
                return []

            def get_references_to(self, title, section):
                return []

            def get_referenced_by(self, title, section):
                return []

        storage = ConcreteStorage()
        assert storage.get_section(26, "32") is None
        assert storage.search("test") == []
        assert storage.list_titles() == []
        assert storage.get_references_to(26, "32") == []
        assert storage.get_referenced_by(26, "32") == []
