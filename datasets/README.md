# Datasets - Fuentes de Datos

## APIs Publicas utilizadas como insumo del ETL

### 1. BCRP - Banco Central de Reserva del Peru
- **URL**: https://estadisticas.bcrp.gob.pe/estadisticas/series/api
- **Formato**: JSON/API REST
- **Series utilizadas**:
  | Codigo | Indicador | Categoria |
  |--------|-----------|-----------|
  | PN01288PM | PBI Global (var % 12 meses) | produccion |
  | PN01270PM | IPC Lima (var % mensual) | precios |
  | PD04638PD | Tipo de Cambio Venta | tipo_cambio |
  | PD04639PD | Tipo de Cambio Compra | tipo_cambio |
  | PN00015MM | Tasa de Referencia BCRP | tasas |
  | PN01207PM | Balanza Comercial (mill US$) | comercio_exterior |
- **Periodo**: 2015 - presente
- **Endpoint ejemplo**: `https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PN01288PM/json/2015-1/2026-3`

### 2. World Bank - Indicadores de Desarrollo
- **URL**: https://api.worldbank.org/v2/
- **Formato**: JSON/API REST
- **Indicadores utilizados**:
  | Codigo | Indicador |
  |--------|-----------|
  | SP.POP.TOTL | Poblacion Total |
  | NY.GDP.MKTP.CD | PBI (US$ corrientes) |
  | NY.GDP.PCAP.CD | PBI per capita |
  | FP.CPI.TOTL.ZG | Inflacion (% anual) |
  | SL.UEM.TOTL.ZS | Tasa de Desempleo |
  | SI.POV.NAHC | Pobreza (linea nacional) |
  | SE.XPD.TOTL.GD.ZS | Gasto en Educacion (% PBI) |
  | SH.XPD.CHEX.GD.ZS | Gasto en Salud (% PBI) |
  | BX.KLT.DINV.CD.WD | Inversion Extranjera Directa |
  | NE.EXP.GNFS.ZS | Exportaciones (% PBI) |
- **Pais**: Peru (PE)
- **Periodo**: 2000 - presente
- **Endpoint ejemplo**: `https://api.worldbank.org/v2/country/PE/indicator/NY.GDP.MKTP.CD?format=json&per_page=100`

### 3. UN Comtrade - Comercio Exterior
- **URL**: https://comtradeapi.un.org/
- **Formato**: JSON/API REST
- **Configuracion**:
  - Reporter: Peru (604)
  - Partners: China (156), EEUU (842), Brasil (76), Chile (152), Colombia (170)
  - Flujos: Importaciones (M) y Exportaciones (X)
  - Productos: TOTAL, 27 (combustibles), 26 (minerales), 71 (perlas/piedras), 08 (frutas)
- **Periodo**: 2018 - 2023
