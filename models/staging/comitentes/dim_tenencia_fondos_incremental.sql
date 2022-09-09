{{ config(materialized='incremental') }}
--no defino una unique_key ya que el modelo corré a diario y todos las filas deben apendear. 

with
  ctascorrientesfdo as (
    select
      *
    from
      {{ ref('stg_ctas_corrientes_fdo') }}
  ),
  ctascorrientes as (
    select
      *
    from
      {{ ref('stg_ctas_corrientes') }}
  ),
  cotizacionesfdo as (
    select
      *
    from
      {{ ref('stg_cotizaciones_fdo') }}
  ),
  fondos as (
    select
      *
    from
      {{ ref('stg_fondos') }}
  ),
  tpctabancaria as (
    select
      *
    from
      {{ ref('stg_tp_cta_bancaria') }}
  ),
  emisores as (
    select
      *
    from
      {{ ref('stg_emisores') }}
  ),
  comitentes as (
    select
      *
    from
      {{ ref('stg_comitentes') }}
  ),
  monedas as (
    select
      *
    from
      {{ ref('stg_monedas') }}
  ),

  final as (
    SELECT
        '2022-08-02' as Fecha,
        fondo.Descripcion as nombreFondo,
        c.NumComitente as numComitente,
        SUM(ctaCteFondo.Cantidad) as CantidadCuotaparte,
        cotizacion.Cotizacion as vcp,
        Round(SUM(ctaCteFondo.Cantidad) * cotizacion.Cotizacion, 8) as valuacionorigen,
        m.Descripcion as moneda,
        CONCAT('T+', fondo.PlazoResc) as plazoFondo,
        e.Descripcion as sociedadGerente
      FROM ctascorrientesfdo ctaCteFondo
      JOIN ctascorrientes ctaCte on ctaCte.CodCtaCorriente = ctaCteFondo.CodCtaCorriente
      JOIN cotizacionesfdo cotizacion on cotizacion.CodFondo = ctaCteFondo.CodFondo
      JOIN (select max(c2.CodCotizacionFdo) as CodCotizacionFdo, c2.CodFondo
      FROM cotizacionesfdo c2
        WHERE c2.estaanulado = 0 AND c2.Fecha <= '2022-08-02'
        GROUP BY c2.CodFondo) ultimaCotizacion ON ultimaCotizacion.CodCotizacionFdo = cotizacion.CodCotizacionFdo
        JOIN fondos fondo ON fondo.CodFondo = cotizacion.CodFondo
      JOIN comitentes c on c.CodComitente = ctaCteFondo.CodComitenteRel
      JOIN emisores e on e.CodEmisor = fondo.CodEmisor
      JOIN monedas m on ctaCteFondo.CodMoneda = m.CodMoneda
      WHERE ctaCte.EstaAnulado = 0
      AND cotizacion.EstaAnulado = 0
      AND fondo.EstaAnulado = 0
      AND ctaCteFondo.FechaLiquidacion <= '2022-08-02'
  
GROUP BY
  fondo.Descripcion,
  c.NumComitente,
  cotizacion.Cotizacion,
  m.Descripcion,
  CONCAT('T+', fondo.PlazoResc),
  e.Descripcion
)

select
  *
from
  final