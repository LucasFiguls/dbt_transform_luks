{{ config (materialized='table') }}

with
    comitentes
    as
    (
        select
            cast(_airbyte_data -> 'CodComitente' as integer) as "codcomitente", 
            cast(_airbyte_data -> 'NumComitente' as integer) as "numcomitente", 
            _airbyte_data ->> 'Descripcion' as "descripcion", 
            _airbyte_data ->> 'EmailsInfo' as "emailsinfo",
            cast(_airbyte_data -> 'EstaAnulado' as integer) as "estaanulado"
        from {{ source
    ('src_raw_vbmaxcapital', '_airbyte_raw_comitentes') }}
)

select *
from comitentes

{# "EsFisico": -1,
    "EsMercado": 0,
    "TimeStamp": "�\u000fW",
    "EmailsInfo": "hernanlopezleon@hotmail.com",
    "TieneInter": 0,
    "CodGrupoEco": 1,
    "Descripcion": "LOPEZ LEON HERNAN FERNANDO",
    "EstaAnulado": 0,
    "CodComitente": 1,
    "CodPerfilCmt": 5211,
    "FechaIngreso": "2016-05-11T00:00:00.000000Z",
    "NoPresencial": 0,
    "NumComitente": 10001,
    "TieneNoInter": 0,
    "CodNumeracion": "CMT",
    "EsAseguradora": 0,
    "CodTpComitente": 7,
    "FondosTerceros": 0,
    "NivelSeguridad": 0,
    "CodAuditoriaRef": 11846,
    "EsCarteraPropia": 0,
    "EsInstitucional": 0,
    "EsSujetoRegulado": 0,
    "CodNumeracionPeri": 1,
    "CodGrupoArOperBurs": 4,
    "EsAsigCatUIFManual": 0,
    "EsAperturaProvisoria": 0,
    "EsCtaOperativaBackUp": 0,
    "CodTpContribRetencion": "IN",
    "RequiereFirmaConjunta": 0 #}