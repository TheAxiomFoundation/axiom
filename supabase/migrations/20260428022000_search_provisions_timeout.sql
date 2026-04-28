-- Keep broad Axiom searches from hitting PostgREST's statement timeout.
--
-- The corpus is large enough that rank ordering a broad query can exceed the
-- authenticator role's default timeout. The function is SECURITY DEFINER and
-- clears statement/lock timeouts only for this RPC call.

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
      r.id,
      r.jurisdiction,
      r.doc_type,
      r.citation_path,
      r.heading,
      r.body,
      r.has_rulespec,
      ts_rank_cd(r.fts, p.tsq) AS rank,
      p.tsq
    FROM corpus.provisions r
    CROSS JOIN parsed p
    WHERE r.fts @@ p.tsq
      AND (jurisdiction_in IS NULL OR r.jurisdiction = jurisdiction_in)
      AND (doc_type_in IS NULL OR r.doc_type = doc_type_in)
    ORDER BY rank DESC, r.citation_path ASC
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

GRANT EXECUTE ON FUNCTION corpus.search_provisions(text, text, text, int) TO anon, authenticated;

NOTIFY pgrst, 'reload schema';
