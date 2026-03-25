-- =============================================================
-- Seguridad: Grants Unity Catalog
-- Proyecto: Smart Data - Panorama Macroeconomico del Peru
-- =============================================================
-- Grupos registrados en Microsoft Entra ID y Databricks Identity:
--   datareaders   - Analistas (solo lectura)
--   dataengineers - Ingenieros de datos (lectura/escritura)
--   dataadmins    - Administradores (acceso total)
-- =============================================================

USE CATALOG catalog_smartdata_dev;

-- =============================================================
-- 1. Grants a nivel de Catalogo
-- =============================================================
GRANT USE CATALOG ON CATALOG catalog_smartdata_dev TO `datareaders`;
GRANT USE CATALOG ON CATALOG catalog_smartdata_dev TO `dataengineers`;
GRANT ALL PRIVILEGES ON CATALOG catalog_smartdata_dev TO `dataadmins`;

-- =============================================================
-- 2. Grants a nivel de Schema
-- =============================================================

-- Bronze
GRANT USE SCHEMA ON SCHEMA catalog_smartdata_dev.bronze TO `datareaders`;
GRANT SELECT ON SCHEMA catalog_smartdata_dev.bronze TO `datareaders`;
GRANT USE SCHEMA ON SCHEMA catalog_smartdata_dev.bronze TO `dataengineers`;
GRANT SELECT ON SCHEMA catalog_smartdata_dev.bronze TO `dataengineers`;
GRANT MODIFY ON SCHEMA catalog_smartdata_dev.bronze TO `dataengineers`;
GRANT CREATE TABLE ON SCHEMA catalog_smartdata_dev.bronze TO `dataengineers`;

-- Silver
GRANT USE SCHEMA ON SCHEMA catalog_smartdata_dev.silver TO `datareaders`;
GRANT SELECT ON SCHEMA catalog_smartdata_dev.silver TO `datareaders`;
GRANT USE SCHEMA ON SCHEMA catalog_smartdata_dev.silver TO `dataengineers`;
GRANT SELECT ON SCHEMA catalog_smartdata_dev.silver TO `dataengineers`;
GRANT MODIFY ON SCHEMA catalog_smartdata_dev.silver TO `dataengineers`;
GRANT CREATE TABLE ON SCHEMA catalog_smartdata_dev.silver TO `dataengineers`;

-- Gold
GRANT USE SCHEMA ON SCHEMA catalog_smartdata_dev.gold TO `datareaders`;
GRANT SELECT ON SCHEMA catalog_smartdata_dev.gold TO `datareaders`;
GRANT USE SCHEMA ON SCHEMA catalog_smartdata_dev.gold TO `dataengineers`;
GRANT SELECT ON SCHEMA catalog_smartdata_dev.gold TO `dataengineers`;
GRANT MODIFY ON SCHEMA catalog_smartdata_dev.gold TO `dataengineers`;
GRANT CREATE TABLE ON SCHEMA catalog_smartdata_dev.gold TO `dataengineers`;

-- =============================================================
-- 3. Grants en External Locations
-- =============================================================
GRANT READ FILES ON EXTERNAL LOCATION `exlt_bronze_dev` TO `datareaders`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt_bronze_dev` TO `dataengineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION `exlt_bronze_dev` TO `dataengineers`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `exlt_bronze_dev` TO `dataadmins`;

GRANT READ FILES ON EXTERNAL LOCATION `exlt_silver_dev` TO `datareaders`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt_silver_dev` TO `dataengineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION `exlt_silver_dev` TO `dataengineers`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `exlt_silver_dev` TO `dataadmins`;

GRANT READ FILES ON EXTERNAL LOCATION `exlt_gold_dev` TO `datareaders`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt_gold_dev` TO `dataengineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION `exlt_gold_dev` TO `dataengineers`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `exlt_gold_dev` TO `dataadmins`;

-- =============================================================
-- 4. Grants especificos en tablas Gold (Power BI / Analistas)
-- =============================================================
GRANT SELECT ON TABLE catalog_smartdata_dev.gold.dim_date TO `datareaders`;
GRANT SELECT ON TABLE catalog_smartdata_dev.gold.dim_indicator TO `datareaders`;
GRANT SELECT ON TABLE catalog_smartdata_dev.gold.dim_trade_partner TO `datareaders`;
GRANT SELECT ON TABLE catalog_smartdata_dev.gold.fact_economic_indicators TO `datareaders`;
GRANT SELECT ON TABLE catalog_smartdata_dev.gold.fact_trade_yearly TO `datareaders`;

-- =============================================================
-- 5. Verificar grants
-- =============================================================
SHOW GRANTS ON CATALOG catalog_smartdata_dev;
SHOW GRANTS ON SCHEMA catalog_smartdata_dev.bronze;
SHOW GRANTS ON SCHEMA catalog_smartdata_dev.silver;
SHOW GRANTS ON SCHEMA catalog_smartdata_dev.gold;
