{{ config (materialized='table') }}

with
    emisores
    as
    (
        select
            _airbyte_data ->> 'CodEmisor' as "codemisor", 
            _airbyte_data ->> 'Descripcion' as "descripcion"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_emisores') }}
)

select *
from emisores

{# 
"CodPais": 1,
    "CodEmisor": 192,
    "Descripcion": "BANCO DE ITAU ARG SA",
    "EstaAnulado": 0,
    "CodAuditoriaRef": 105434 #}