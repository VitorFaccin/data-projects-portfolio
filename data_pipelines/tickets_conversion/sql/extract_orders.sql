SELECT
    o.purchase_timestamp AS purchase_timestamp,
    o.agent_id AS order_agent_id,
    o.agent_name AS order_agent_name,
    o.supervisor_name AS order_supervisor_name,
    o.team_name AS order_team_name,
    o.approval_timestamp AS approval_timestamp,
    o.order_group_id AS order_group_id,
    o.customer_id AS customer_id,
    o.payment_type AS payment_type,
    o.order_status AS order_status,
    SUM(o.order_value) AS order_value
FROM generic.order_history AS o
WHERE o.purchase_date::date {date_condition}
  AND o.sales_channel = 'assisted_sales'
  AND o.business_unit <> 'excluded_unit'
GROUP BY
    o.purchase_timestamp,
    o.agent_id,
    o.agent_name,
    o.supervisor_name,
    o.team_name,
    o.approval_timestamp,
    o.order_group_id,
    o.customer_id,
    o.payment_type,
    o.order_status
