
{{ config(materialized='table') }}
with
    fondos
    as
    (
        select
            cast(_airbyte_data -> 'CodFondo' as integer) as "codfondo",
            cast(_airbyte_data -> 'CodEmisor' as integer) as "codemisor",
            _airbyte_data ->> 'Descripcion' as "descripcion",
            cast(_airbyte_data -> 'PlazoResc' as integer)  as "plazoresc",
            cast(_airbyte_data -> 'EstaAnulado' as integer) as "estaanulado"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_fondos') }}
)

select *
from fondos


{# 
"CodFondo": 4,
    "EsFisico": 0,
    "CodAgente": 224,
    "CodEmisor": 334,
    "CodFmtImp": 19,
    "CodMoneda": 1,
    "PlazoResc": 2,
    "PlazoSusc": 0,
    "TimeStamp": "\t�n",
    "EsFCI_ACDI": -1,
    "TpRedondeo": "0",
    "Abreviatura": "14943",
    "CodInterfaz": "1142",
    "Descripcion": "TORONTO TRUST RENTA FIJA PLUS Clase B",
    "EstaAnulado": 0,
    "HoraTopeMov": "1900-01-01T00:00:00.000000Z",
    "CntDecimales": 6,
    "ResolucionCNV": "18612",
    "CodCtaBancaria": 12,
    "CodFmtImpAAPIC": 28,
    "EsCtrolMandato": 0,
    "NumRegistroCNV": 701,
    "CodAuditoriaRef": 105466,
    "EsVBHomeVisible": -1,
    "EsFisicoJuridico": -1,
    "FechaRegistroCNV": "2017-04-12T00:00:00.000000Z",
    "FechaActualizacion": "2022-02-01T00:00:00.000000Z",
    "TpRedondeoResImpor": "0",
    "CodTpAdjudicacionCp": "FR",
    "CodInterfazBloomberg": "340324",
    "ImpMinimoPrimeraSusc": 1000.0,
    "CodCtaBancariaRescate": 12,
    "CodInterfazCajaValores": "14943",
    "CodCtaBancariaRescateACDI": 12,
    "CodCtaContableRescateSFCI": 288,
    "CodCtaBancariaSuscripcionACDI": 12,
    "CodCtaContableSuscripcionSFCI": 288 #}