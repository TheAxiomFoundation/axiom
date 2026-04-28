-- Clean domain names:
--   corpus     = browsable source corpus
--   encodings  = RuleSpec generation/evaluation/session records
--   app        = Axiom product metadata
--   ingest/raw = private pipeline workspace

CREATE SCHEMA IF NOT EXISTS corpus;

CREATE SCHEMA IF NOT EXISTS encodings;
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS raw;

DO $$
BEGIN
  IF to_regclass('corpus.rules') IS NOT NULL
    AND to_regclass('corpus.provisions') IS NULL THEN
    ALTER TABLE corpus.rules RENAME TO provisions;
  END IF;

  IF to_regclass('corpus.rule_references') IS NOT NULL
    AND to_regclass('corpus.provision_references') IS NULL THEN
    ALTER TABLE corpus.rule_references RENAME TO provision_references;
  END IF;

  IF to_regclass('corpus.provision_references') IS NOT NULL THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'corpus'
        AND table_name = 'provision_references'
        AND column_name = 'source_rule_id'
    ) THEN
      ALTER TABLE corpus.provision_references
        RENAME COLUMN source_rule_id TO source_provision_id;
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'corpus'
        AND table_name = 'provision_references'
        AND column_name = 'target_rule_id'
    ) THEN
      ALTER TABLE corpus.provision_references
        RENAME COLUMN target_rule_id TO target_provision_id;
    END IF;
  END IF;

  IF to_regclass('corpus.jurisdiction_counts') IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM pg_attribute
      WHERE attrelid = 'corpus.jurisdiction_counts'::regclass
        AND attname = 'rule_count'
        AND NOT attisdropped
    ) THEN
    ALTER MATERIALIZED VIEW corpus.jurisdiction_counts
      RENAME COLUMN rule_count TO provision_count;
  END IF;
END $$;

ALTER INDEX IF EXISTS corpus.idx_rules_orphan_citation_prefix_ordinal
  RENAME TO idx_provisions_orphan_citation_prefix_ordinal;
ALTER INDEX IF EXISTS corpus.idx_rules_orphan_citation_prefix
  RENAME TO idx_provisions_orphan_citation_prefix;
ALTER INDEX IF EXISTS corpus.idx_rules_doctype_citation_body
  RENAME TO idx_provisions_doctype_citation_body;
ALTER INDEX IF EXISTS corpus.idx_rule_references_source_span
  RENAME TO idx_provision_references_source_span;
ALTER INDEX IF EXISTS corpus.idx_rule_references_source
  RENAME TO idx_provision_references_source;
ALTER INDEX IF EXISTS corpus.idx_rule_references_target_id
  RENAME TO idx_provision_references_target_id;
ALTER INDEX IF EXISTS corpus.idx_rule_references_target_path_unresolved
  RENAME TO idx_provision_references_target_path_unresolved;
ALTER INDEX IF EXISTS corpus.idx_rule_references_kind
  RENAME TO idx_provision_references_kind;

DO $$
DECLARE
  table_name_var text;
BEGIN
  FOREACH table_name_var IN ARRAY ARRAY[
    'encoding_runs',
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
      WHERE table_schema = 'encodings'
        AND table_name = table_name_var
    ) THEN
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA encodings', table_name_var);
    END IF;

    IF EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'app'
        AND table_name = table_name_var
    ) AND NOT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'encodings'
        AND table_name = table_name_var
    ) THEN
      EXECUTE format('ALTER TABLE app.%I SET SCHEMA encodings', table_name_var);
    END IF;
  END LOOP;
END $$;

ALTER SEQUENCE IF EXISTS public.agent_transcripts_id_seq SET SCHEMA encodings;
ALTER SEQUENCE IF EXISTS app.agent_transcripts_id_seq SET SCHEMA encodings;

GRANT USAGE ON SCHEMA corpus TO postgres, service_role, anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA corpus TO postgres, service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA corpus TO anon, authenticated;

GRANT USAGE ON SCHEMA encodings TO postgres, service_role, anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA encodings TO anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA encodings TO postgres, service_role;

DO $$
BEGIN
  IF to_regclass('encodings.agent_transcripts_id_seq') IS NOT NULL THEN
    EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE encodings.agent_transcripts_id_seq TO postgres, service_role';
  END IF;
END $$;

GRANT USAGE ON SCHEMA app TO postgres, service_role, anon, authenticated;
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
DROP FUNCTION IF EXISTS app.get_encoding_runs(int, int);
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
DROP FUNCTION IF EXISTS corpus.get_law_stats();
DROP FUNCTION IF EXISTS corpus.get_corpus_stats();
DROP FUNCTION IF EXISTS corpus.search_rules(text, text, text, int);
DROP FUNCTION IF EXISTS corpus.search_provisions(text, text, text, int);
DROP FUNCTION IF EXISTS corpus.get_references(text);
DROP FUNCTION IF EXISTS corpus.get_provision_references(text);

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
      p.id,
      p.jurisdiction,
      p.doc_type,
      p.citation_path,
      p.heading,
      p.body,
      p.has_rulespec,
      ts_rank_cd(p.fts, parsed.tsq) AS rank,
      parsed.tsq
    FROM corpus.provisions p
    CROSS JOIN parsed
    WHERE p.fts @@ parsed.tsq
      AND (jurisdiction_in IS NULL OR p.jurisdiction = jurisdiction_in)
      AND (doc_type_in IS NULL OR p.doc_type = doc_type_in)
    ORDER BY rank DESC, p.citation_path ASC
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

ALTER ROLE authenticator SET pgrst.db_schemas = 'public,graphql_public,corpus,encodings,app';

NOTIFY pgrst, 'reload config';
NOTIFY pgrst, 'reload schema';
