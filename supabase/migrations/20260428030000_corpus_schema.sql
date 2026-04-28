-- Expose the browsable source corpus under a product-neutral `corpus` schema.

DO $$
DECLARE
  source_schema text;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.schemata
    WHERE schema_name = 'corpus'
  ) THEN
    SELECT table_schema
    INTO source_schema
    FROM information_schema.tables
    WHERE table_name IN ('provisions', 'rules')
      AND table_schema NOT IN (
        'public',
        'raw',
        'graphql_public',
        'information_schema',
        'pg_catalog'
      )
    ORDER BY table_schema
    LIMIT 1;

    IF source_schema IS NULL THEN
      CREATE SCHEMA corpus;
    ELSE
      EXECUTE format('ALTER SCHEMA %I RENAME TO corpus', source_schema);
    END IF;
  END IF;
END $$;

GRANT USAGE ON SCHEMA corpus TO postgres, service_role, anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA corpus TO postgres, service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA corpus TO anon, authenticated;

DO $$
BEGIN
  IF to_regprocedure('corpus.search_provisions(text,text,text,integer)') IS NOT NULL THEN
    ALTER FUNCTION corpus.search_provisions(text, text, text, int)
      SET search_path = corpus, public;
  END IF;

  IF to_regprocedure('corpus.get_provision_references(text)') IS NOT NULL THEN
    ALTER FUNCTION corpus.get_provision_references(text)
      SET search_path = corpus, public;
  END IF;

  IF to_regprocedure('corpus.get_corpus_stats()') IS NOT NULL THEN
    ALTER FUNCTION corpus.get_corpus_stats()
      SET search_path = corpus, public;
  END IF;

  IF to_regprocedure('corpus.refresh_jurisdiction_counts()') IS NOT NULL THEN
    ALTER FUNCTION corpus.refresh_jurisdiction_counts()
      SET search_path = corpus, public;
  END IF;
END $$;

ALTER ROLE authenticator SET pgrst.db_schemas = 'public,graphql_public,corpus,encodings,telemetry,app';

NOTIFY pgrst, 'reload config';
NOTIFY pgrst, 'reload schema';
