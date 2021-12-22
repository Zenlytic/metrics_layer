with source_data as (

    select

        order_id,
        customer_id,
        order_date,
        days_between_orders,
        revenue,
        sub_channel,
        new_vs_repeat

    from raw.shopify.orders

)

select * from source_data
