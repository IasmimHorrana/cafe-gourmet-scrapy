{{
    config(
        materialized = 'table',
        unique_key = 'sk_fato_produto',
        tags = ['intermediate', 'fact']
    )
}}

with produtos as (
    select * from {{ ref('stg_products') }}
),

dim_produtos as (
    select sk_product, product_id as id_produto
    from {{ ref('int_dim_products') }}
),

dim_date as (
    select date_day
    from {{ ref('int_dim_date') }}
)

select
    -- Chave substituta da fact
    {{ dbt_utils.generate_surrogate_key(['p.id', 'p.etl_inserted_at']) }} as sk_fato_produto,

    -- Chaves estrangeiras
    dp.sk_product as fk_produto,

    -- Chave de negócio
    p.id as id_produto,

    -- Dimensão de tempo (quando foi coletado o dado)
    p.etl_inserted_at,
    date_trunc('day', p.etl_inserted_at) as data_coleta,

    -- Métricas (fatos mensuráveis)
    p.old_price,
    p.new_price,
    p.promotion_value,
    p.reviews_amount,
    p.reviews_rating_number,

    -- Metadados
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at

from produtos p
left join dim_produtos dp 
    on p.id = dp.id_produto
left join dim_date dd 
    on date_trunc('day', p.etl_inserted_at) = dd.date_day