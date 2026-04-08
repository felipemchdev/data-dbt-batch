with order_level as (
    select distinct
        order_id,
        cast(order_purchase_timestamp as date) as order_date,
        total_order_value
    from {{ ref('stg_orders_enriched') }}
),
base as (
    select
        order_date,
        sum(total_order_value) as revenue
    from order_level
    group by 1
)

select *
from base
order by order_date
