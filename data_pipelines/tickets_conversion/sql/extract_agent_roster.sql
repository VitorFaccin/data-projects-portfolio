SELECT
    r.agent_id AS agent_id,
    r.agent_name AS agent_name,
    r.team_name AS team_name,
    r.supervisor_name AS supervisor_name,
    LOWER(r.agent_email) AS agent_email,
    CAST(r.reference_year::text || '-' || r.reference_month::text || '-01' AS date) AS month_ref
FROM generic.agent_roster AS r
WHERE r.agent_email IS NOT NULL
