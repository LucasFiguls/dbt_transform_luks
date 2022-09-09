{{ config(materialized='table') }}
with
    ctascorrientesfdo
    as
    (
        select
            _airbyte_data ->> 'CodComitenteRel' as "codcomitenterel",
            _airbyte_data ->> 'CodFondo' as "codfondo",
            _airbyte_data ->> 'CodMoneda' as "codmoneda",
            cast(_airbyte_data ->> 'Cantidad' as decimal) as "cantidad",
            _airbyte_data ->> 'CodCtaCorriente' as "codctacorriente",
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
