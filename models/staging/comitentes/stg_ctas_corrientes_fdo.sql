{{ config(materialized='table') }}
with
    ctascorrientesfdo
    as
    (
        select
            cast(_airbyte_data -> 'CodComitenteRel' as integer) as "codcomitenterel",
            cast(_airbyte_data -> 'CodFondo' as integer) as "codfondo",
            cast(_airbyte_data -> 'CodMoneda' as integer) as "codmoneda",
            cast(_airbyte_data -> 'Cantidad' as decimal) as "cantidad",
            cast(_airbyte_data -> 'CodCtaCorriente' as integer) as "codctacorriente",
            cast(_airbyte_data ->> 'FechaLiquidacion' as TimeStamp) as "fechaliquidacion"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_ctascorrientesfdo') }}
)

select * from ctascorrientesfdo


    {# "EsACDI": -1,
    "Cantidad": 149785.6432,
    "CodFondo": 228,
    "CodMoneda": 1,
    "CodAgenteDepo": 5,
    "CodComitenteRel": 5,
    "CodCtaCorriente": 21503,
    "FechaLiquidacion": "2022-08-10T00:00:00.000000Z",
    "CodCtaCorrienteFdo": 1138  #}
