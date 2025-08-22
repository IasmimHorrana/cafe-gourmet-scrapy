{{
    config(
        materialized = 'table',
        unique_key = 'sk_product',
        tags = ['intermediate', 'dimension']
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
)

select
    -- Chave substituta (surrogate key)
    {{ dbt_utils.generate_surrogate_key(['id']) }} as sk_product,

    -- Chave de negócio (id original do Mercado Livre)
    id as product_id,

    -- Atributos descritivos
    name,
    promotion_label,

    -- Metadados
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from products