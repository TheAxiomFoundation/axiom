-- RPC returning high-level Atlas corpus + citation-graph stats for the
-- landing page. One round-trip instead of four.
--
-- Speed matters: this runs on every page-load for the Atlas landing,
-- so exact counts on 600k+ rules are unaffordable. ``pg_class.reltuples``
-- gives an estimate that's accurate after each ``ANALYZE`` and costs
-- a single index lookup.
--
-- Returned shape:
--   rules_count          — approx rows in arch.rules
--   references_count     — approx rows in arch.rule_references
--   jurisdictions_count  — distinct non-null jurisdictions in arch.rules
--
-- Interjurisdictional-ref counts were considered but require a heap
-- join across the refs table, which blows PostgREST's statement-
-- timeout. Revisit once we have a materialized side-table for graph
-- stats.

CREATE OR REPLACE FUNCTION arch.get_atlas_stats()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = arch, public
AS $$
  SELECT jsonb_build_object(
    'rules_count',
      GREATEST(
        (
          SELECT reltuples::bigint
          FROM pg_class
          WHERE oid = 'arch.rules'::regclass
        ),
        0
      ),
    'references_count',
      GREATEST(
        (
          SELECT reltuples::bigint
          FROM pg_class
          WHERE oid = 'arch.rule_references'::regclass
        ),
        0
      ),
    'jurisdictions_count',
      (
        SELECT COUNT(DISTINCT jurisdiction)::int
        FROM arch.rules
        WHERE jurisdiction IS NOT NULL
      )
  )
$$;

GRANT EXECUTE ON FUNCTION arch.get_atlas_stats() TO anon, authenticated;

NOTIFY pgrst, 'reload schema';
