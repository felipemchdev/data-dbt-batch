with seller_agg as (
    select
        seller_id,
        seller_city,
        seller_state,
        count(distinct order_id) as orders_count,
        sum(item_price + freight_value) as seller_revenue
    from {{ ref('stg_orders_enriched') }}
    where seller_id is not null
    group by 1, 2, 3
)

select *
from seller_agg
order by seller_revenue desc
