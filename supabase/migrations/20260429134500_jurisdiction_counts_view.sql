-- Drop jurisdiction-only aliases; corpus.provision_counts is the count surface.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_matviews
    WHERE schemaname = 'corpus'
      AND matviewname = 'jurisdiction_counts'
  ) THEN
    DROP MATERIALIZED VIEW corpus.jurisdiction_counts;
  ELSE
    DROP VIEW IF EXISTS corpus.jurisdiction_counts;
  END IF;
END $$;

DROP FUNCTION IF EXISTS corpus.refresh_jurisdiction_counts();

NOTIFY pgrst, 'reload schema';
