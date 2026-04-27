-- Revert the citation-flow Sankey viz.
--
-- The landing-page diagram it backed read as decorative rather than
-- informative — state codes cite federal code, everyone knew that.
-- Drop the supporting objects rather than leave dead schema.
--
-- refresh_jurisdiction_counts reverts to refreshing just the counts
-- MV (its form before 20260418040000).

DROP FUNCTION IF EXISTS arch.get_jurisdiction_flows();

DROP MATERIALIZED VIEW IF EXISTS arch.jurisdiction_flows;

CREATE OR REPLACE FUNCTION arch.refresh_jurisdiction_counts()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = arch, public
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY arch.jurisdiction_counts;
END
$$;

NOTIFY pgrst, 'reload schema';
