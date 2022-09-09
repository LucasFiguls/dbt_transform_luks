
{{ config(materialized='table') }}
with
    entliquidacion
    as
    (
        select
            _airbyte_data ->> 'Descripcion' as "descripcion",
            _airbyte_data ->> 'CodEntLiquidacion' as "codentliquidacion",
            cast(_airbyte_data ->> 'EstaAnulado' as integer) as "estaanulado"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_entliquidacion') }}
)

select *
from entliquidacion

{# 
"Calle": "AVDA. L.N ALEM",
    "CodPais": 1,
    "Localidad": "CABA",
    "TimeStamp": "\u0003/=",
    "AlturaCalle": "651",
    "Descripcion": "BANCO DE GALICIA",
    "EstaAnulado": 0,
    "PermiteParc": 0,
    "CodProvincia": 6,
    "CodigoPostal": "1001",
    "CodAuditoriaRef": 53204,
    "CodEntLiquidacion": 226,
    "CodTpEntLiquidacion": "BA" #}