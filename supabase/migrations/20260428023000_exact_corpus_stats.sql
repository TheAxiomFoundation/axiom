-- Report exact Axiom corpus counts from the materialized view.
--
-- `pg_class.reltuples` is useful before stats views exist, but the Axiom
-- Axiom database maintains `corpus.jurisdiction_counts`, so the public stats
-- RPC can return an exact rules count without scanning `corpus.provisions`.

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

GRANT EXECUTE ON FUNCTION corpus.get_corpus_stats() TO anon, authenticated;

NOTIFY pgrst, 'reload schema';
