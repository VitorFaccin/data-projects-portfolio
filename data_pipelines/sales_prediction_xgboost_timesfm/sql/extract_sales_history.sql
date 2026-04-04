SELECT
    CAST(s.order_date AS DATE) AS date_ref,
    s.product_id AS product_id,
    p.category_group AS category_group,
    p.category_name AS category_name,
    s.sales_channel AS sales_channel,
    COALESCE(SUM(s.gmv), 0) AS gmv
FROM generic.sales_history AS s
LEFT JOIN generic.product_dimension AS p
    ON s.product_id = p.product_id
WHERE s.order_date >= DATEADD(month, -{history_months}, DATE_TRUNC('month', GETDATE()))
  AND s.order_date < DATE_TRUNC('day', GETDATE())
  AND s.order_status ILIKE 'approved'
  AND s.sale_type ILIKE 'first_party'
  {product_filter}
GROUP BY
    CAST(s.order_date AS DATE),
    s.product_id,
    p.category_group,
    p.category_name,
    s.sales_channel
