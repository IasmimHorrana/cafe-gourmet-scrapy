with source as (
    select * from {{ source('raw', 'coffee_products') }}
),

transform as (
    select
        -- Chave primária
        id,

        -- Dados do produto
        name,
        old_price,
        new_price,
        promotion_label,
        promotion_value,
        reviews_amount,
        reviews_rating_number,

        -- Metadados
        current_timestamp as etl_inserted_at
    from source
)

select * from transform