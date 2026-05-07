-- Keep public corpus reads pinned to the active release boundary.
--
-- The physical corpus tables still retain all imported rows for service/admin
-- workflows. Public clients should read only the current-release views and
-- current-scoped RPCs.

REVOKE SELECT ON corpus.provisions FROM PUBLIC;
REVOKE SELECT ON corpus.provisions FROM anon, authenticated;

REVOKE SELECT ON corpus.provision_counts FROM PUBLIC;
REVOKE SELECT ON corpus.provision_counts FROM anon, authenticated;
GRANT SELECT ON corpus.provision_counts TO postgres, service_role;

REVOKE SELECT ON corpus.provision_references FROM PUBLIC;
REVOKE SELECT ON corpus.provision_references FROM anon, authenticated;
GRANT ALL ON corpus.provision_references TO postgres, service_role;

GRANT SELECT ON corpus.release_scopes TO anon, authenticated;
GRANT SELECT ON corpus.current_release_scopes TO anon, authenticated;
GRANT SELECT ON corpus.current_provisions TO anon, authenticated;
GRANT SELECT ON corpus.current_provision_counts TO anon, authenticated;

CREATE OR REPLACE FUNCTION corpus.get_provision_references(citation_path_in text)
RETURNS TABLE (
  direction              text,
  citation_text          text,
  pattern_kind           text,
  confidence             real,
  start_offset           integer,
  end_offset             integer,
  other_citation_path    text,
  other_provision_id     uuid,
  other_heading          text,
  target_resolved        boolean
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = corpus, public
AS $$
  WITH self AS (
    SELECT id
    FROM corpus.current_provisions
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
    tgt.id AS other_provision_id,
    tgt.heading AS other_heading,
    (tgt.id IS NOT NULL) AS target_resolved
  FROM corpus.provision_references r
  JOIN self ON r.source_provision_id = self.id
  LEFT JOIN corpus.current_provisions tgt ON tgt.id = r.target_provision_id

  UNION ALL

  SELECT
    'incoming'::text AS direction,
    r.citation_text,
    r.pattern_kind,
    r.confidence,
    r.start_offset,
    r.end_offset,
    src.citation_path AS other_citation_path,
    src.id AS other_provision_id,
    src.heading AS other_heading,
    TRUE AS target_resolved
  FROM corpus.provision_references r
  JOIN self ON r.target_provision_id = self.id
  JOIN corpus.current_provisions src ON src.id = r.source_provision_id

  ORDER BY direction, start_offset NULLS LAST;
$$;

GRANT EXECUTE ON FUNCTION corpus.get_provision_references(text) TO anon, authenticated;

REVOKE EXECUTE ON FUNCTION corpus.get_all_corpus_stats() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION corpus.get_all_corpus_stats() FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.get_all_corpus_stats() TO postgres, service_role;

REVOKE EXECUTE ON FUNCTION corpus.search_all_provisions(text, text, text, int) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION corpus.search_all_provisions(text, text, text, int)
  FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.search_all_provisions(text, text, text, int)
  TO postgres, service_role;

REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() FROM anon, authenticated;
GRANT EXECUTE ON FUNCTION corpus.refresh_corpus_analytics() TO postgres, service_role;

NOTIFY pgrst, 'reload schema';
