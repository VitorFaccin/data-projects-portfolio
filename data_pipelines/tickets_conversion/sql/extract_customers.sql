SELECT
    c.customer_id AS customer_id,
    LOWER(c.email) AS email,
    CASE
        WHEN c.phone_primary ILIKE '+55%' AND LENGTH(c.phone_primary) = 14 THEN SUBSTRING(c.phone_primary, 4)
        WHEN c.phone_primary ILIKE '+55%' AND LENGTH(c.phone_primary) = 13 THEN SUBSTRING(c.phone_primary, 4, 2) || '9' || SUBSTRING(c.phone_primary, 6)
        WHEN c.phone_primary ILIKE '55%' AND LENGTH(c.phone_primary) = 13 THEN SUBSTRING(c.phone_primary, 3)
        WHEN c.phone_primary ILIKE '55%' AND LENGTH(c.phone_primary) = 12 THEN SUBSTRING(c.phone_primary, 3, 2) || '9' || SUBSTRING(c.phone_primary, 5)
        WHEN LENGTH(c.phone_primary) = 11 THEN c.phone_primary
        WHEN LENGTH(c.phone_primary) = 10 THEN SUBSTRING(c.phone_primary, 1, 2) || '9' || SUBSTRING(c.phone_primary, 3)
        WHEN LENGTH(c.phone_primary) > 13 THEN SUBSTRING(c.phone_primary, LENGTH(c.phone_primary) - 10)
        ELSE c.phone_primary
    END AS phone_primary,
    CASE
        WHEN c.phone_secondary ILIKE '+55%' AND LENGTH(c.phone_secondary) = 14 THEN SUBSTRING(c.phone_secondary, 4)
        WHEN c.phone_secondary ILIKE '+55%' AND LENGTH(c.phone_secondary) = 13 THEN SUBSTRING(c.phone_secondary, 4, 2) || '9' || SUBSTRING(c.phone_secondary, 6)
        WHEN c.phone_secondary ILIKE '55%' AND LENGTH(c.phone_secondary) = 13 THEN SUBSTRING(c.phone_secondary, 3)
        WHEN c.phone_secondary ILIKE '55%' AND LENGTH(c.phone_secondary) = 12 THEN SUBSTRING(c.phone_secondary, 3, 2) || '9' || SUBSTRING(c.phone_secondary, 5)
        WHEN LENGTH(c.phone_secondary) = 11 THEN c.phone_secondary
        WHEN LENGTH(c.phone_secondary) = 10 THEN SUBSTRING(c.phone_secondary, 1, 2) || '9' || SUBSTRING(c.phone_secondary, 3)
        WHEN LENGTH(c.phone_secondary) > 13 THEN SUBSTRING(c.phone_secondary, LENGTH(c.phone_secondary) - 10)
        ELSE c.phone_secondary
    END AS phone_secondary
FROM generic.customer_reference AS c
WHERE c.marketplace_origin <> 'third_party_only'
  AND (
      c.email IS NOT NULL
      OR c.phone_primary IS NOT NULL
      OR c.phone_secondary IS NOT NULL
  )
  AND c.customer_type <> 'inactive'
  AND c.active_flag <> 0
