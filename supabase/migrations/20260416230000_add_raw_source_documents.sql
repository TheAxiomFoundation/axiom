-- Forward-looking scaffold for raw-fetch provenance. The table starts
-- empty; future ingest pipelines will write one row per upstream fetch
-- (eCFR XML, USLM XML, HTML scrape, PDF) so every parsed rule can be
-- traced back to the exact bytes it came from.
CREATE SCHEMA IF NOT EXISTS corpus;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ingest;

CREATE TABLE IF NOT EXISTS raw.fetched_documents (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_url          TEXT NOT NULL,
  jurisdiction        TEXT,                -- 'us', 'uk', 'us-ca', ...
  upstream_system     TEXT,                -- 'ecfr' | 'uscode.house.gov' | 'dc_council' | ...
  upstream_version    TEXT,                -- eCFR date, USLM release point, etc.
  r2_key              TEXT,                -- 'raw/us/ecfr/title-7/part-273.xml'
  content_sha256      TEXT,                -- of the raw bytes; detects upstream changes
  byte_size           BIGINT,
  fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_fetched_documents_jurisdiction
  ON raw.fetched_documents (jurisdiction);
CREATE INDEX IF NOT EXISTS idx_raw_fetched_documents_upstream
  ON raw.fetched_documents (upstream_system, upstream_version);
CREATE INDEX IF NOT EXISTS idx_raw_fetched_documents_sha
  ON raw.fetched_documents (content_sha256);

-- Private provenance surface: service_role writes, no browser reads.
GRANT USAGE ON SCHEMA raw TO postgres, service_role;
GRANT USAGE ON SCHEMA ingest TO postgres, service_role;
GRANT ALL ON TABLE raw.fetched_documents TO postgres, service_role;

ALTER TABLE raw.fetched_documents ENABLE ROW LEVEL SECURITY;

-- Eventually corpus.provisions.source_document_id FK here. Adding the column
-- today (nullable) so the backfill path is a pure UPDATE, not a
-- schema migration + backfill.
ALTER TABLE corpus.provisions
  ADD COLUMN IF NOT EXISTS source_document_id UUID
    REFERENCES raw.fetched_documents(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_provisions_source_document
  ON corpus.provisions (source_document_id)
  WHERE source_document_id IS NOT NULL;
