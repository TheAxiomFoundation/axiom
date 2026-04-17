-- Compound partial index for the refs-extractor's paged fetch.
--
-- scripts/extract_references.py walks rules by doc_type + citation_path
-- using keyset pagination:
--
--   SELECT id, citation_path, body FROM akn.rules
--   WHERE body IS NOT NULL
--     AND doc_type = 'regulation'
--     AND citation_path > '<cursor>'
--   ORDER BY citation_path ASC
--   LIMIT 500;
--
-- Before this index, the planner used rules_citation_path_unique (plain
-- btree on citation_path) and then Filter-removed everything that wasn't
-- the right doc_type or was body-null. At the tail of regulation (after
-- us/regulation/9/...) the scan still had to walk ~225k non-matching
-- rows per page — 9.5s per call, persistently statement-timing-out and
-- blocking the refs backfill.
--
-- The compound index indexes exactly the predicate pattern the fetch
-- uses. Measured: 9484 ms -> 6 ms on the same query (1500x). The
-- partial predicate (WHERE body IS NOT NULL) keeps the index small by
-- excluding the ~40% of rows with no body (parts, subparts, etc.).

CREATE INDEX IF NOT EXISTS idx_rules_doctype_citation_body
  ON akn.rules (doc_type, citation_path)
  WHERE body IS NOT NULL;
