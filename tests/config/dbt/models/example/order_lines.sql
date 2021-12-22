with source_data as (

    select

        -- Backbone order lines information
        order_lines.order_line_id,
        order_lines.order_id,
        order_lines.order_date,
        order_lines.product_revenue,
        order_lines.channel,
        order_lines.parent_channel,
        order_lines.product_name,

        -- Order information
        orders.customer_id,
        orders.days_between_orders,
        orders.revenue,
        orders.sub_channel,
        orders.new_vs_repeat,

        -- Customer information
        customers.first_order_date,
        customers.second_order_date,
        customers.region,
        customers.gender,
        customers.last_product_purchased,
        customers.customer_ltv,
        customers.total_sessions

    from {{ ref('stg_order_lines')}} order_lines
        left join {{ ref('stg_orders')}} orders
            on order_lines.order_id=orders.order_id
        left join {{ ref('stg_customers')}} customers
            on orders.customer_id=customers.customer_id

)

select * from source_data
