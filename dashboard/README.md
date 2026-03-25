# Dashboard - Panorama Macroeconomico del Peru

## Destino de visualizacion: Power BI

### Tablas disponibles para el dashboard (Azure SQL - Gold Layer)

| Tabla | Tipo | Descripcion |
|-------|------|-------------|
| `dim_date` | Dimension | Calendario 2015-2026 (date_key, year, quarter, month, day) |
| `dim_indicator` | Dimension | Catalogo de indicadores economicos (BCRP + World Bank) |
| `dim_trade_partner` | Dimension | Socios comerciales (China, EEUU, Brasil, Chile, Colombia) |
| `fact_economic_indicators` | Hecho | Indicadores mensuales con variacion YoY |
| `fact_trade_yearly` | Hecho | Comercio exterior anual por socio y producto |

### Conexion
- **Tipo**: Azure SQL Database
- **Servidor**: `sql-smartdata-{env}.database.windows.net`
- **Base de datos**: `sqldb-gold-smartdata-{env}`
- **Autenticacion**: SQL Server Authentication

### Graficos sugeridos
1. Evolucion del PBI (linea temporal)
2. Inflacion vs Tipo de Cambio (doble eje)
3. Balanza Comercial por socio (barras apiladas)
4. Mapa de calor de indicadores por anio
5. Top socios comerciales (treemap)

> **Nota**: Colocar archivos .pbix, .png de capturas y .txt con enlace al dashboard publicado.
