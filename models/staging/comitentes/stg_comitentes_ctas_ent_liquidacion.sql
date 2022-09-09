{{ config(materialized='table') }}
with
    cmtctasentliquidacion
    as
    (
        select
            cast(_airbyte_data -> 'CodComitente' as integer) as "codcomitente",
            cast(_airbyte_data -> 'CodMoneda' as integer) as "codmoneda",
            cast(_airbyte_data -> 'CodTpCtaBancaria' as integer) as "codtpctabancaria",
            cast(_airbyte_data -> 'CodEntLiquidacion' as integer) as "codentliquidacion",
            _airbyte_data ->> 'CBU' as "cbu",
            _airbyte_data ->> 'Alias' as "alias",
            _airbyte_data ->> 'CuitTitular' as "cuittitular",
            cast(_airbyte_data ->> 'EstaAnulado' as integer) as "estaanulado"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_cmtctasentliquidacion') }}
)

select * from cmtctasentliquidacion
