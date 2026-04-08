with order_level as (
    select distinct
        order_id,
        customer_id,
        customer_unique_id,
        order_purchase_timestamp,
        total_order_value
    from {{ ref('stg_orders_enriched') }}
),
customer_order_stats as (
    select
        customer_id,
        customer_unique_id,
        count(distinct order_id) as total_orders,
        avg(total_order_value) as avg_ticket,
        min(order_purchase_timestamp) as first_purchase_ts,
        max(order_purchase_timestamp) as last_purchase_ts
    from order_level
    group by 1, 2
)

select
    customer_id,
    customer_unique_id,
    total_orders,
    avg_ticket,
    first_purchase_ts,
    last_purchase_ts,
    case
        when datediff('day', first_purchase_ts, last_purchase_ts) = 0 then total_orders
        else total_orders / nullif(datediff('day', first_purchase_ts, last_purchase_ts), 0)
    end as purchase_frequency
from customer_order_stats
order by total_orders desc
