-- Full-text search over arch.rules with ranking and headlines.
--
-- Uses the generated tsvector column `fts` (indexed via GIN at
-- idx_rules_fts) and `websearch_to_tsquery` so the query string can
-- carry the same operators people use in Google (quoted phrases, `OR`,
-- leading `-` for exclusion) without bespoke parsing on the client.
--
-- Why an RPC instead of PostgREST's ?fts= operator:
--   * PostgREST cannot emit ts_headline output — we want highlighted
--     snippets of the matching body text, not just the row.
--   * We want ranking (ts_rank_cd) which is also not expressible via
--     the REST filter syntax.
--
-- Parameters
--   q                Raw query string (websearch syntax).
--   jurisdiction_in  Optional filter (e.g. 'us').
--   doc_type_in      Optional filter ('statute' | 'regulation').
--   limit_in         Max rows (caller should clamp client-side too).
--
-- Returns one row per hit with the canonical metadata the Atlas viewer
-- already knows how to render plus `rank` for secondary sorting and
-- `snippet` (already marked up with <mark> tags) for display.
CREATE OR REPLACE FUNCTION arch.search_rules(
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
AS $$
  WITH parsed AS (
    SELECT websearch_to_tsquery('english', q) AS tsq
  )
  SELECT
    r.id,
    r.jurisdiction,
    r.doc_type,
    r.citation_path,
    r.heading,
    ts_headline(
      'english',
      COALESCE(r.body, r.heading, ''),
      p.tsq,
      'StartSel=<mark>,StopSel=</mark>,MaxWords=30,MinWords=15,ShortWord=3,MaxFragments=1'
    ) AS snippet,
    r.has_rulespec,
    ts_rank_cd(r.fts, p.tsq) AS rank
  FROM arch.rules r
  CROSS JOIN parsed p
  WHERE r.fts @@ p.tsq
    AND (jurisdiction_in IS NULL OR r.jurisdiction = jurisdiction_in)
    AND (doc_type_in IS NULL OR r.doc_type = doc_type_in)
  ORDER BY rank DESC, r.citation_path ASC
  LIMIT GREATEST(1, LEAST(limit_in, 100));
$$;

-- Allow the anon role (used by the website) to call the RPC.
-- The function body is SECURITY INVOKER by default, so row-level
-- security on arch.rules still applies.
GRANT EXECUTE ON FUNCTION arch.search_rules(text, text, text, int) TO anon;
GRANT EXECUTE ON FUNCTION arch.search_rules(text, text, text, int) TO authenticated;
