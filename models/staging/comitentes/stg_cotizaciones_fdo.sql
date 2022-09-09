
{{ config(materialized='table') }}
with
    cotizacionesfdo
    as
    (
        select
            cast(_airbyte_data ->> 'Fecha' as TimeStamp) as "fecha",
            cast(_airbyte_data ->> 'Cotizacion' as decimal) as "cotizacion",
            _airbyte_data ->> 'CodFondo' as "codfondo",
            _airbyte_data ->> 'CodCotizacionFdo' as "codcotizacionfdo",
            cast(_airbyte_data ->> 'EstaAnulado' as integer) as "estaanulado"
            
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_cotizacionesfdo') }}
)

select *
from cotizacionesfdo


{# "Fecha": "2022-03-07T00:00:00.000000Z",
    "CodFondo": 521,
    "EsTeorica": 0,
    "TimeStamp": "\u0004�\t",
    "Cotizacion": 3.805614,
    "EstaAnulado": 0,
    "CodAuditoriaRef": 121277,
    "TpCambioCotPais": 1,
    "CodCotizacionFdo": 5 #}