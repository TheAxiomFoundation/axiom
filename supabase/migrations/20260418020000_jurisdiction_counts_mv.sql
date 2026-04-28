-- Materialized view backing the per-jurisdiction breakdown on the
-- Axiom landing page.
--
-- An exact GROUP BY jurisdiction over 600k rows runs ~3s on the live
-- cluster and busts PostgREST's statement timeout. A materialized
-- view gives us instant reads at the cost of a periodic REFRESH.
-- Staleness isn't a concern for a landing-page stat — the counts
-- refresh with each ingest run (call REFRESH MATERIALIZED VIEW at
-- the tail of the driver or manually).
--
-- Schema
-- ------
-- jurisdiction  — e.g. 'us', 'us-ny', 'us-dc', 'uk', 'ca'
-- provision_count    — approximate count (exact at last refresh)

CREATE MATERIALIZED VIEW IF NOT EXISTS corpus.jurisdiction_counts AS
SELECT
  jurisdiction,
  COUNT(*)::bigint AS provision_count
FROM corpus.provisions
WHERE jurisdiction IS NOT NULL
GROUP BY jurisdiction;

-- Unique index lets us REFRESH CONCURRENTLY without blocking readers
-- once the view is populated.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jurisdiction_counts_jurisdiction
  ON corpus.jurisdiction_counts (jurisdiction);

-- Initial populate. Subsequent refreshes use CONCURRENTLY.
REFRESH MATERIALIZED VIEW corpus.jurisdiction_counts;

GRANT SELECT ON corpus.jurisdiction_counts TO anon, authenticated;

-- Expand get_corpus_stats to include the per-jurisdiction breakdown
-- as a sorted array of {jurisdiction, count} pairs.
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
        SELECT COUNT(*)::int
        FROM corpus.jurisdiction_counts
      ),
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

NOTIFY pgrst, 'reload schema';
