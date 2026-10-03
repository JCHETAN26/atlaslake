-- PII classification (tags) + tag-based masking (Phase 33).
-- Gold already carries only a masked email; the policy is defence in depth: any column tagged
-- restricted/confidential is masked for every role except ATLAS_PII_READER (and its parents).

USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS ATLASLAKE.GOVERNANCE WITH MANAGED ACCESS;
USE SCHEMA ATLASLAKE.GOVERNANCE;

CREATE TAG IF NOT EXISTS PII_CLASS
  ALLOWED_VALUES 'restricted', 'confidential', 'internal', 'public'
  COMMENT = 'Data classification (docs/SECURITY.md)';

CREATE MASKING POLICY IF NOT EXISTS MASK_PII_STRING AS (val STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('ATLAS_PII_READER') THEN val
    WHEN val IS NULL THEN NULL
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('ATLASLAKE.GOVERNANCE.PII_CLASS') = 'restricted'
      THEN REGEXP_REPLACE(val, '.', '*')
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('ATLASLAKE.GOVERNANCE.PII_CLASS') = 'confidential'
      THEN LEFT(val, 1) || '***'
    ELSE val
  END
  COMMENT = 'Masks restricted/confidential string columns for non-PII roles';

-- Attach once to the tag: every column tagged PII_CLASS gets masked automatically.
ALTER TAG PII_CLASS SET MASKING POLICY MASK_PII_STRING;

GRANT USAGE ON SCHEMA ATLASLAKE.GOVERNANCE TO ROLE ATLAS_TRANSFORMER;
GRANT APPLY ON TAG PII_CLASS TO ROLE ATLAS_TRANSFORMER;

-- Classify Gold columns (run after 03_tables.sql). email_masked is already masked upstream, but it is
-- still personal data: tag it 'confidential' so analysts only see the first character.
-- ALTER TABLE ATLASLAKE.GOLD.DIM_CUSTOMER MODIFY COLUMN EMAIL_MASKED SET TAG ATLASLAKE.GOVERNANCE.PII_CLASS = 'confidential';
-- ALTER TABLE ATLASLAKE.GOLD.DIM_CUSTOMER MODIFY COLUMN CITY         SET TAG ATLASLAKE.GOVERNANCE.PII_CLASS = 'internal';
-- ALTER TABLE ATLASLAKE.GOLD.DIM_CUSTOMER MODIFY COLUMN STATE        SET TAG ATLASLAKE.GOVERNANCE.PII_CLASS = 'internal';

-- Verify:
-- USE ROLE ATLAS_ANALYST;    SELECT email_masked FROM ATLASLAKE.GOLD.DIM_CUSTOMER LIMIT 5;  -- o***
-- USE ROLE ATLAS_PII_READER; SELECT email_masked FROM ATLASLAKE.GOLD.DIM_CUSTOMER LIMIT 5;  -- o***@example.com
