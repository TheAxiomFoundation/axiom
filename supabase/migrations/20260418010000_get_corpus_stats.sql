-- RPC returning high-level Axiom corpus + citation-graph stats for the
-- landing page. One round-trip instead of four.
--
-- Speed matters: this runs on every page-load for the Axiom landing,
-- so exact counts on 600k+ rules are unaffordable. ``pg_class.reltuples``
-- gives an estimate that's accurate after each ``ANALYZE`` and costs
-- a single index lookup.
--
-- Returned shape:
--   provisions_count          — approx rows in corpus.provisions
--   references_count     — approx rows in corpus.provision_references
--   jurisdictions_count  — distinct non-null jurisdictions in corpus.provisions
--
-- Interjurisdictional-ref counts were considered but require a heap
-- join across the refs table, which blows PostgREST's statement-
-- timeout. Revisit once we have a materialized side-table for graph
-- stats.

CREATE OR REPLACE FUNCTION corpus.get_corpus_stats()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
AS $$
  SELECT jsonb_build_object(
    'provisions_count',
      GREATEST(
        (
          SELECT reltuples::bigint
          FROM pg_class
          WHERE oid = 'corpus.provisions'::regclass
        ),
        0
      ),
    'references_count',
      GREATEST(
        (
          SELECT reltuples::bigint
          FROM pg_class
          WHERE oid = 'corpus.provision_references'::regclass
        ),
        0
      ),
    'jurisdictions_count',
      (
        SELECT COUNT(DISTINCT jurisdiction)::int
        FROM corpus.provisions
        WHERE jurisdiction IS NOT NULL
      )
  )
$$;

GRANT EXECUTE ON FUNCTION corpus.get_corpus_stats() TO anon, authenticated;

NOTIFY pgrst, 'reload schema';
