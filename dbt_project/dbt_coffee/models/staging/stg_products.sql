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
        coalesce(reviews_amount, 0) as reviews_amount, -- NULL vira 0
        reviews_rating_number,                          -- mantém NULL se não houver avaliações

        -- Metadados
        current_timestamp as etl_inserted_at
    from source
)

select * from transform