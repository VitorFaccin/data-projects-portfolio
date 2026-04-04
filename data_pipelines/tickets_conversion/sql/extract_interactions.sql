WITH interaction_attributes AS (
    SELECT
        i.interaction_id AS interaction_id,
        MAX(CASE WHEN ia.attribute_name = 'contact_email' THEN LOWER(ia.attribute_value) END) AS contact_email,
        MAX(CASE WHEN ia.attribute_name = 'service_channel' THEN ia.attribute_value END) AS raw_service_channel
    FROM generic.support_interactions AS i
    LEFT JOIN generic.support_interaction_attributes AS ia
        ON i.interaction_id = ia.interaction_id
    GROUP BY
        i.interaction_id
)
SELECT
    i.interaction_id AS interaction_id,
    i.created_at AS created_at,
    i.updated_at AS updated_at,
    a.customer_key AS customer_key,
    CASE
        WHEN a.requester_phone ILIKE '+55%' AND LENGTH(a.requester_phone) = 14 THEN SUBSTRING(a.requester_phone, 4)
        WHEN a.requester_phone ILIKE '+55%' AND LENGTH(a.requester_phone) = 13 THEN SUBSTRING(a.requester_phone, 4, 2) || '9' || SUBSTRING(a.requester_phone, 6)
        WHEN a.requester_phone ILIKE '55%' AND LENGTH(a.requester_phone) = 13 THEN SUBSTRING(a.requester_phone, 3)
        WHEN a.requester_phone ILIKE '55%' AND LENGTH(a.requester_phone) = 12 THEN SUBSTRING(a.requester_phone, 3, 2) || '9' || SUBSTRING(a.requester_phone, 5)
        WHEN LENGTH(a.requester_phone) = 11 THEN a.requester_phone
        WHEN LENGTH(a.requester_phone) = 10 THEN SUBSTRING(a.requester_phone, 1, 2) || '9' || SUBSTRING(a.requester_phone, 3)
        WHEN LENGTH(a.requester_phone) > 13 THEN SUBSTRING(a.requester_phone, LENGTH(a.requester_phone) - 10)
        ELSE a.requester_phone
    END AS customer_phone,
    LOWER(a.requester_email) AS system_customer_email,
    attrs.contact_email AS contact_email,
    LOWER(a.assigned_agent_email) AS agent_email,
    attrs.raw_service_channel AS raw_service_channel,
    DATE_TRUNC('month', i.created_at::timestamp)::date AS month_ref,
    CASE
        WHEN i.group_type = 'outbound_group' THEN 'outbound'
        ELSE 'inbound'
    END AS interaction_type
FROM generic.support_interactions AS i
LEFT JOIN generic.support_users AS a
    ON a.user_id = i.requester_user_id
LEFT JOIN interaction_attributes AS attrs
    ON attrs.interaction_id = i.interaction_id
WHERE i.created_at::date {date_condition}
  AND i.workflow_type IN ('service_form', 'active_group')
  AND i.tags NOT LIKE '%internal_outbound_only%'
