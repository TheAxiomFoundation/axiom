-- Indexes that unblock Atlas title- and part-level browsing.
--
-- The viewer renders a rule's children by looking up all `arch.rules` rows
-- whose `citation_path` starts with the parent's path, restricted to orphan
-- rows (parent_id IS NULL) so only top-level statute/regulation sections
-- appear at that tier.
--
-- Without these two indexes, the query
--     SELECT * FROM arch.rules
--      WHERE citation_path LIKE 'us/statute/7/%'
--        AND parent_id IS NULL
--      ORDER BY ordinal
--      LIMIT 100;
-- degenerates into a scan of ~65k rows on the existing partial index
-- `idx_rules_toplevel_sort (jurisdiction, source_path) WHERE parent_id IS NULL`
-- because `citation_path LIKE` is not backed by a btree with
-- `text_pattern_ops`. Measured latency: >10s on prod before, ~400ms after.
--
-- `text_pattern_ops` is required because the default `collate` opclass on
-- `text` cannot support `LIKE 'prefix%'` lookups; only the C-locale pattern
-- ops can. The `WHERE parent_id IS NULL` partial predicate keeps the index
-- small (only top-level rows qualify for tree nodes) and aligned with how
-- the viewer queries.
--
-- Original ad-hoc creation date: 2026-04-16 (post-facto formalization).

CREATE INDEX IF NOT EXISTS idx_rules_orphan_citation_prefix_ordinal
  ON arch.rules (citation_path text_pattern_ops, ordinal)
  WHERE parent_id IS NULL;

-- Slim companion for count-only queries that don't need `ordinal`.
-- Kept separate so EXPLAIN can pick the cheaper index when ordering
-- doesn't matter.
CREATE INDEX IF NOT EXISTS idx_rules_orphan_citation_prefix
  ON arch.rules (citation_path text_pattern_ops)
  WHERE parent_id IS NULL;
