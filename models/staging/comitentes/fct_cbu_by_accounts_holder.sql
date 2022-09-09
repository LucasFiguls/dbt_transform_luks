with comitentes as (

    select * from {{ ref('stg_comitentes')}}

),

cmtctasentliquidacion as (

    select * from {{ ref('stg_comitentes_ctas_ent_liquidacion') }}

),

entliquidacion as (

    select * from {{ ref('stg_ent_liquidacion') }}

),

monedas as (

    select * from {{ ref('stg_monedas') }}

),

tpctabancaria as (

    select * from {{ ref('stg_tp_cta_bancaria') }}

),

final as (
select comitentes.numcomitente,
       comitentes.descripcion        as descrcomitente,
       monedas.descripcion        as moneda,
       upper(tpctabancaria.descripcion) as cuentatipo,
       entliquidacion.descripcion        as banco,
       cmtctasentliquidacion.cbu,
       cmtctasentliquidacion.alias            as aliascbu,
       cmtctasentliquidacion.cuittitular      as cuittitular 
from cmtctasentliquidacion
         inner join entliquidacion on entliquidacion.codentliquidacion = cmtctasentliquidacion.codentliquidacion
         inner join tpctabancaria on tpctabancaria.codtpctabancaria = cmtctasentliquidacion.codtpctabancaria
         inner join comitentes on comitentes.codcomitente = cmtctasentliquidacion.codcomitente
         inner join monedas on monedas.codmoneda = cmtctasentliquidacion.codmoneda
where cmtctasentliquidacion.estaanulado = 0
and entliquidacion.estaanulado = 0
and tpctabancaria.estaanulado = 0
and comitentes.estaanulado = 0
and monedas.estaanulado = 0
--order by comitentes.numcomitente desc

)

select * from final
