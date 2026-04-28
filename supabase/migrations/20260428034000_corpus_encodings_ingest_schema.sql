-- Greenfield Supabase layout:
--   corpus    = browsable source corpus and public corpus RPCs
--   encodings = RuleSpec generation/evaluation metadata
--   telemetry       = encoder transcripts, SDK sessions, and event logs
--   app       = Axiom product metadata
--   ingest = private ingest/staging workspace
--   raw    = private raw-source provenance metadata

CREATE SCHEMA IF NOT EXISTS corpus;

CREATE SCHEMA IF NOT EXISTS encodings;
CREATE SCHEMA IF NOT EXISTS telemetry;
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS raw;

DO $$
DECLARE
  table_name_var text;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'encoding_runs'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'encodings'
      AND table_name = 'encoding_runs'
  ) THEN
    ALTER TABLE public.encoding_runs SET SCHEMA encodings;
  END IF;

  FOREACH table_name_var IN ARRAY ARRAY[
    'agent_transcripts',
    'sdk_sessions',
    'sdk_session_events'
  ]
  LOOP
    IF EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = table_name_var
    ) AND NOT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'telemetry'
        AND table_name = table_name_var
    ) THEN
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA telemetry', table_name_var);
    END IF;
  END LOOP;
END $$;

ALTER SEQUENCE IF EXISTS public.agent_transcripts_id_seq SET SCHEMA telemetry;

ALTER TABLE IF EXISTS encodings.encoding_runs
  ADD COLUMN IF NOT EXISTS complexity jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS scores jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS final_scores jsonb,
  ADD COLUMN IF NOT EXISTS has_issues boolean,
  ADD COLUMN IF NOT EXISTS note text,
  ADD COLUMN IF NOT EXISTS session_id text,
  ADD COLUMN IF NOT EXISTS file_path text,
  ADD COLUMN IF NOT EXISTS rulespec_content text,
  ADD COLUMN IF NOT EXISTS synced_at timestamptz,
  ADD COLUMN IF NOT EXISTS encoder_version text,
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

GRANT USAGE ON SCHEMA corpus TO postgres, service_role, anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA corpus TO postgres, service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA corpus TO anon, authenticated;

GRANT USAGE ON SCHEMA encodings TO postgres, service_role, anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA encodings TO anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA encodings TO postgres, service_role;

GRANT USAGE ON SCHEMA telemetry TO postgres, service_role, anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA telemetry TO anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA telemetry TO postgres, service_role;

DO $$
BEGIN
  IF to_regclass('telemetry.agent_transcripts_id_seq') IS NOT NULL THEN
    EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE telemetry.agent_transcripts_id_seq TO postgres, service_role';
  END IF;
END $$;

GRANT USAGE ON SCHEMA ingest TO postgres, service_role;
GRANT USAGE ON SCHEMA raw TO postgres, service_role;
REVOKE USAGE ON SCHEMA ingest FROM anon, authenticated;
REVOKE USAGE ON SCHEMA raw FROM anon, authenticated;
REVOKE SELECT ON ALL TABLES IN SCHEMA raw FROM anon, authenticated;

DO $$
BEGIN
  IF to_regclass('raw.fetched_documents') IS NOT NULL THEN
    DROP POLICY IF EXISTS anon_read ON raw.fetched_documents;
    DROP POLICY IF EXISTS authenticated_read ON raw.fetched_documents;
  END IF;
END $$;

DROP FUNCTION IF EXISTS public.get_encoding_runs(int, int);
DROP FUNCTION IF EXISTS encodings.get_encoding_runs(int, int);

CREATE OR REPLACE FUNCTION encodings.get_encoding_runs(
  limit_count int DEFAULT 100,
  offset_count int DEFAULT 0
)
RETURNS TABLE (
  id text,
  "timestamp" timestamptz,
  citation text,
  iterations jsonb,
  scores jsonb,
  has_issues boolean,
  note text,
  total_duration_ms integer,
  agent_type text,
  agent_model text,
  data_source text,
  session_id text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = encodings
AS $$
  SELECT
    encoding_runs.id,
    encoding_runs."timestamp",
    encoding_runs.citation,
    encoding_runs.iterations,
    COALESCE(encoding_runs.scores, encoding_runs.final_scores, '{}'::jsonb) AS scores,
    encoding_runs.has_issues,
    encoding_runs.note,
    encoding_runs.total_duration_ms,
    encoding_runs.agent_type,
    encoding_runs.agent_model,
    encoding_runs.data_source,
    encoding_runs.session_id
  FROM encodings.encoding_runs
  ORDER BY encoding_runs."timestamp" DESC
  LIMIT GREATEST(1, LEAST(limit_count, 500))
  OFFSET GREATEST(0, offset_count);
$$;

GRANT EXECUTE ON FUNCTION encodings.get_encoding_runs(int, int) TO anon, authenticated;

DROP FUNCTION IF EXISTS corpus.get_axiom_stats();
DROP FUNCTION IF EXISTS corpus.get_corpus_stats();

CREATE OR REPLACE FUNCTION corpus.get_corpus_stats()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
AS $$
  SELECT jsonb_build_object(
    'provisions_count',
      COALESCE((SELECT SUM(provision_count)::bigint FROM corpus.jurisdiction_counts), 0),
    'references_count',
      (SELECT COUNT(*)::bigint FROM corpus.provision_references),
    'jurisdictions_count',
      (SELECT COUNT(*)::int FROM corpus.jurisdiction_counts),
    'jurisdictions',
      COALESCE(
        (
          SELECT jsonb_agg(
                   jsonb_build_object(
                     'jurisdiction', jurisdiction,
                     'count', provision_count
                   )
                   ORDER BY provision_count DESC
                 )
          FROM corpus.jurisdiction_counts
        ),
        '[]'::jsonb
      )
  )
$$;

CREATE OR REPLACE FUNCTION corpus.search_provisions(
  q text,
  jurisdiction_in text DEFAULT NULL,
  doc_type_in text DEFAULT NULL,
  limit_in int DEFAULT 30
)
RETURNS TABLE (
  id uuid,
  jurisdiction text,
  doc_type text,
  citation_path text,
  heading text,
  snippet text,
  has_rulespec boolean,
  rank real
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
SET statement_timeout = 0
SET lock_timeout = 0
AS $$
  WITH parsed AS (
    SELECT websearch_to_tsquery('english', q) AS tsq
  ), ranked AS (
    SELECT
      r.id,
      r.jurisdiction,
      r.doc_type,
      r.citation_path,
      r.heading,
      r.body,
      r.has_rulespec,
      ts_rank_cd(r.fts, p.tsq) AS rank,
      p.tsq
    FROM corpus.provisions r
    CROSS JOIN parsed p
    WHERE r.fts @@ p.tsq
      AND (jurisdiction_in IS NULL OR r.jurisdiction = jurisdiction_in)
      AND (doc_type_in IS NULL OR r.doc_type = doc_type_in)
    ORDER BY rank DESC, r.citation_path ASC
    LIMIT GREATEST(1, LEAST(limit_in, 100))
  )
  SELECT
    ranked.id,
    ranked.jurisdiction,
    ranked.doc_type,
    ranked.citation_path,
    ranked.heading,
    ts_headline(
      'english',
      COALESCE(ranked.body, ranked.heading, ''),
      ranked.tsq,
      'StartSel=<mark>,StopSel=</mark>,MaxWords=30,MinWords=15,ShortWord=3,MaxFragments=1'
    ) AS snippet,
    ranked.has_rulespec,
    ranked.rank
  FROM ranked;
$$;

CREATE OR REPLACE FUNCTION corpus.get_provision_references(citation_path_in text)
RETURNS TABLE (
  direction text,
  citation_text text,
  pattern_kind text,
  confidence real,
  start_offset integer,
  end_offset integer,
  other_citation_path text,
  other_provision_id uuid,
  other_heading text,
  target_resolved boolean
)
LANGUAGE sql
STABLE
AS $$
  WITH self AS (
    SELECT id FROM corpus.provisions
    WHERE citation_path = citation_path_in
    LIMIT 1
  )
  SELECT
    'outgoing'::text AS direction,
    r.citation_text,
    r.pattern_kind,
    r.confidence,
    r.start_offset,
    r.end_offset,
    r.target_citation_path AS other_citation_path,
    r.target_provision_id AS other_provision_id,
    tgt.heading AS other_heading,
    (r.target_provision_id IS NOT NULL) AS target_resolved
  FROM corpus.provision_references r
  JOIN self ON r.source_provision_id = self.id
  LEFT JOIN corpus.provisions tgt ON tgt.id = r.target_provision_id

  UNION ALL

  SELECT
    'incoming'::text AS direction,
    r.citation_text,
    r.pattern_kind,
    r.confidence,
    r.start_offset,
    r.end_offset,
    src.citation_path AS other_citation_path,
    r.source_provision_id AS other_provision_id,
    src.heading AS other_heading,
    TRUE AS target_resolved
  FROM corpus.provision_references r
  JOIN self ON r.target_provision_id = self.id
  JOIN corpus.provisions src ON src.id = r.source_provision_id

  ORDER BY direction, start_offset NULLS LAST;
$$;

CREATE OR REPLACE FUNCTION corpus.refresh_jurisdiction_counts()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = corpus, public
SET statement_timeout = 0
SET lock_timeout = 0
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY corpus.jurisdiction_counts;
END
$$;

GRANT EXECUTE ON FUNCTION corpus.search_provisions(text, text, text, int) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.get_provision_references(text) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.get_corpus_stats() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.refresh_jurisdiction_counts() TO anon, authenticated;

ALTER ROLE authenticator SET pgrst.db_schemas = 'public,graphql_public,corpus,encodings,telemetry,app';

NOTIFY pgrst, 'reload config';
NOTIFY pgrst, 'reload schema';
