with delivery as (
    select distinct
        order_id,
        customer_state,
        delivery_time_days,
        delayed_flag
    from {{ ref('stg_orders_enriched') }}
    where order_id is not null
)

select
    coalesce(customer_state, 'unknown') as customer_state,
    avg(delivery_time_days) as avg_delivery_time_days,
    sum(coalesce(delayed_flag, 0)) as delayed_orders,
    count(distinct order_id) as orders_count,
    sum(coalesce(delayed_flag, 0)) * 1.0 / nullif(count(distinct order_id), 0) as delay_rate
from delivery
group by 1
order by delay_rate desc
