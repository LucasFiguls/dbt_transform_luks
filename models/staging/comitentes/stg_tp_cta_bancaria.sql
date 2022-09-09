
{{ config(materialized='table') }}
with
    tpctabancaria
    as
    (
        select
            _airbyte_data ->> 'CodTpCtaBancaria' as "codtpctabancaria",
            _airbyte_data ->> 'Descripcion' as "descripcion",
            cast(_airbyte_data ->> 'EstaAnulado' as integer) as "estaanulado"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_tpctabancaria') }}
)

select *
from tpctabancaria


{# "TimeStamp": "\u0003��",
    "CodInterfaz": "",
    "Descripcion": "Cuenta Especial",
    "EstaAnulado": 0,
    "Observaciones": "",
    "CodAuditoriaRef": 94829,
    "CodTpCtaBancaria": 5 #}