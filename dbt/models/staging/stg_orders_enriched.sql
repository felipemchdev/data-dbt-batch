with orders as (
    select distinct
        cast(order_id as varchar) as order_id,
        cast(customer_id as varchar) as customer_id,
        lower(trim(cast(order_status as varchar))) as order_status_clean,
        try_cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
        try_cast(order_approved_at as timestamp) as order_approved_at,
        try_cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        try_cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date
    from {{ source('bronze', 'orders') }}
    where order_id is not null
      and customer_id is not null
),
customers as (
    select distinct
        cast(customer_id as varchar) as customer_id,
        cast(customer_unique_id as varchar) as customer_unique_id,
        cast(customer_city as varchar) as customer_city,
        cast(customer_state as varchar) as customer_state
    from {{ source('bronze', 'customers') }}
    where customer_id is not null
),
payments as (
    select
        cast(order_id as varchar) as order_id,
        sum(coalesce(try_cast(payment_value as double), 0)) as total_order_value
    from {{ source('bronze', 'order_payments') }}
    where order_id is not null
    group by 1
),
items as (
    select distinct
        cast(order_id as varchar) as order_id,
        cast(order_item_id as integer) as order_item_id,
        cast(product_id as varchar) as product_id,
        cast(seller_id as varchar) as seller_id,
        coalesce(try_cast(price as double), 0) as item_price,
        coalesce(try_cast(freight_value as double), 0) as freight_value
    from {{ source('bronze', 'order_items') }}
    where order_id is not null
),
products as (
    select distinct
        cast(product_id as varchar) as product_id,
        cast(product_category_name as varchar) as product_category_name
    from {{ source('bronze', 'products') }}
    where product_id is not null
),
sellers as (
    select distinct
        cast(seller_id as varchar) as seller_id,
        cast(seller_city as varchar) as seller_city,
        cast(seller_state as varchar) as seller_state
    from {{ source('bronze', 'sellers') }}
    where seller_id is not null
)

select distinct
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status_clean,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    i.order_item_id,
    i.product_id,
    p.product_category_name,
    i.seller_id,
    s.seller_city,
    s.seller_state,
    i.item_price,
    i.freight_value,
    coalesce(pay.total_order_value, 0) as total_order_value,
    case
        when o.order_delivered_customer_date is null then null
        else datediff('day', o.order_purchase_timestamp, o.order_delivered_customer_date)
    end as delivery_time_days,
    case
        when o.order_delivered_customer_date is null
          or o.order_estimated_delivery_date is null then null
        when o.order_delivered_customer_date > o.order_estimated_delivery_date then 1
        else 0
    end as delayed_flag
from orders o
left join customers c on o.customer_id = c.customer_id
left join payments pay on o.order_id = pay.order_id
left join items i on o.order_id = i.order_id
left join products p on i.product_id = p.product_id
left join sellers s on i.seller_id = s.seller_id
where o.order_purchase_timestamp is not null
