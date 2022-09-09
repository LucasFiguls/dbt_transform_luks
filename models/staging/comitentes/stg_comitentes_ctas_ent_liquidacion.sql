{{ config(materialized='table') }}
with
    cmtctasentliquidacion
    as
    (
        select
            _airbyte_data ->> 'CodComitente' as "codcomitente",
            _airbyte_data ->> 'CodMoneda' as "codmoneda",
            _airbyte_data ->> 'CodTpCtaBancaria' as "codtpctabancaria",
            _airbyte_data ->> 'CodEntLiquidacion' as "codentliquidacion",
            _airbyte_data ->> 'CBU' as "cbu",
            _airbyte_data ->> 'Alias' as "alias",
            _airbyte_data ->> 'CuitTitular' as "cuittitular",
            cast(_airbyte_data ->> 'EstaAnulado' as integer) as "estaanulado"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_cmtctasentliquidacion') }}
)

select * from cmtctasentliquidacion
