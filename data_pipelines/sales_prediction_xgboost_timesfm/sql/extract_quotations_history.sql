SELECT
    CAST(q.quote_date AS DATE) AS date_ref,
    q.product_id AS product_id,
    q.sales_channel AS sales_channel,
    SUM(q.price_amount)::float / SUM(q.quotation_count) AS price,
    SUM(q.leadtime_amount)::float / SUM(q.quotation_count) AS leadtime,
    SUM(q.freight_amount)::float / SUM(q.quotation_count) AS freight
FROM generic.quotation_history AS q
WHERE q.quote_date >= DATEADD(month, -{history_months}, DATE_TRUNC('month', GETDATE()))
  AND q.quote_date < DATE_TRUNC('day', GETDATE())
  AND q.leadtime_amount IS NOT NULL
  AND q.quotation_count > 0
  {product_filter}
GROUP BY
    CAST(q.quote_date AS DATE),
    q.product_id,
    q.sales_channel
