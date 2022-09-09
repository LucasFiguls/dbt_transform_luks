
{{ config(materialized='table') }}
with
    monedas
    as
    (
        select
            _airbyte_data ->> 'Descripcion' as "descripcion",
            _airbyte_data ->> 'CodMoneda' as "codmoneda",
            cast(_airbyte_data ->> 'EstaAnulado' as integer) as "estaanulado"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_monedas') }}
)

select *
from monedas