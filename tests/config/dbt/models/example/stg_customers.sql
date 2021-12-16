with source_data as (

    select

        customer_id,
        first_order_date,
        second_order_date,
        region,
        gender,
        last_product_purchased,
        customer_ltv,
        total_sessions

    from raw.shopify.customers

)

select * from source_data