with base as (
    select
        product_id,
        coalesce(product_category_name, 'unknown') as product_category_name,
        count(*) as units_sold,
        sum(item_price + freight_value) as gross_revenue
    from {{ ref('stg_orders_enriched') }}
    where product_id is not null
    group by 1, 2
)

select *
from base
order by gross_revenue desc
