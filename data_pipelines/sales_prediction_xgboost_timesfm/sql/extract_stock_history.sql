SELECT
    CAST(st.snapshot_date AS DATE) AS date_ref,
    st.product_id AS product_id,
    AVG(st.stock_available)::float AS stock_available
FROM generic.stock_history AS st
WHERE st.snapshot_date >= DATEADD(month, -{history_months}, DATE_TRUNC('month', GETDATE()))
  AND st.snapshot_date < DATE_TRUNC('day', GETDATE())
  {product_filter}
GROUP BY
    CAST(st.snapshot_date AS DATE),
    st.product_id
