WITH delivered_items AS (
    SELECT
        CAST(o.order_purchase_timestamp AS DATE) AS data_date,
        oi.product_id AS product_id,
        c.customer_state AS sale_location,
        COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS product_category,
        oi.price AS item_price,
        DATE_DIFF(
            'day',
            CAST(o.order_purchase_timestamp AS TIMESTAMP),
            CAST(o.order_estimated_delivery_date AS TIMESTAMP)
        ) AS estimated_delivery_days
    FROM read_csv_auto('{order_items_csv_path}', header = true) AS oi
    INNER JOIN read_csv_auto('{orders_csv_path}', header = true) AS o
        ON oi.order_id = o.order_id
    INNER JOIN read_csv_auto('{customers_csv_path}', header = true) AS c
        ON o.customer_id = c.customer_id
    LEFT JOIN read_csv_auto('{products_csv_path}', header = true) AS p
        ON oi.product_id = p.product_id
    LEFT JOIN read_csv_auto('{translation_csv_path}', header = true) AS t
        ON p.product_category_name = t.product_category_name
    WHERE o.order_status = 'delivered'
        AND o.order_purchase_timestamp IS NOT NULL
        AND o.order_estimated_delivery_date IS NOT NULL
        AND oi.price > 0
        AND EXTRACT(MONTH FROM CAST(o.order_purchase_timestamp AS TIMESTAMP)) != 11
        AND CAST(o.order_purchase_timestamp AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
),
daily_metrics AS (
    SELECT
        data_date,
        product_id,
        sale_location,
        product_category,
        COUNT(*) AS quantidade_produto,
        AVG(item_price) AS preco,
        AVG(estimated_delivery_days) AS prazo
    FROM delivered_items
    WHERE estimated_delivery_days > 0
    GROUP BY
        data_date,
        product_id,
        sale_location,
        product_category
)
SELECT
    data_date,
    product_id,
    sale_location,
    product_category,
    quantidade_produto,
    preco,
    prazo
FROM daily_metrics
ORDER BY data_date, product_id, sale_location;
