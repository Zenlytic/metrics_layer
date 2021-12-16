with source_data as (

    select

        order_line_id,
        order_id,
        customer_id,
        order_date,
        product_revenue,
        channel,
        parent_channel,
        product_name

    from raw.shopify.order_lines

)

select * from source_data