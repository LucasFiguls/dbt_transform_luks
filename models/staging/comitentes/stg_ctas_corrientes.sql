{{ config(materialized='table') }}
with
    ctascorrientes
    as
    (
        select
            _airbyte_data ->> 'CodCtaCorriente' as "codctacorriente",
            cast(_airbyte_data ->> 'EstaAnulado' as integer) as "estaanulado"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_ctascorrientes') }}
)

select * from ctascorrientes


    {# "Origen": "TESMOV",
    "TimeStamp": "ɘ�",
    "Descripcion": "Comprobante de Pago / 60898",
    "EstaAnulado": 0,
    "CodComitente": 3,
    "CodAuditoriaRef": 614604,
    "CodCtaCorriente": 23784,
    "CodTesoreriaMov": 1856,
    "FechaConcertacion": "2022-08-12T00:00:00.000000Z" #}
