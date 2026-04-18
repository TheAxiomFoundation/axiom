-- RPC that ingest drivers call at end-of-run to refresh the landing-
-- page stats materialized view.
--
-- Why wrap it in an RPC: the ingest scripts talk to PostgREST via
-- HTTP (RuleUploader) and don't hold a direct Postgres connection.
-- Exposing the REFRESH as a callable function keeps the privilege
-- boundary clean — only this specific operation is exposed, not
-- arbitrary DDL — and makes the call a one-liner from Python.
--
-- The underlying MV has a UNIQUE index on ``jurisdiction`` so
-- CONCURRENTLY is safe: readers on the stats page aren't blocked.

CREATE OR REPLACE FUNCTION akn.refresh_jurisdiction_counts()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = akn, public
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY akn.jurisdiction_counts;
END
$$;

GRANT EXECUTE ON FUNCTION akn.refresh_jurisdiction_counts() TO anon, authenticated;

NOTIFY pgrst, 'reload schema';
