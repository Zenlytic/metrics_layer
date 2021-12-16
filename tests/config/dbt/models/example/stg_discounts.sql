with source_data as (

    select
        discount_id,
        order_id,
        country,
        order_date,
        discount_code,
        discount_amt
    from raw.shopify.discounts

)

select * from source_data
