-- Clean switch from AutoRuleSpec-era column names to Encoder-era names.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'encodings'
      AND table_name = 'encoding_runs'
      AND column_name = 'autorulespec_version'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'encodings'
      AND table_name = 'encoding_runs'
      AND column_name = 'encoder_version'
  ) THEN
    ALTER TABLE encodings.encoding_runs
      RENAME COLUMN autorulespec_version TO encoder_version;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'telemetry'
      AND table_name = 'sdk_sessions'
      AND column_name = 'autorulespec_version'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'telemetry'
      AND table_name = 'sdk_sessions'
      AND column_name = 'encoder_version'
  ) THEN
    ALTER TABLE telemetry.sdk_sessions
      RENAME COLUMN autorulespec_version TO encoder_version;
  END IF;
END $$;

NOTIFY pgrst, 'reload schema';
