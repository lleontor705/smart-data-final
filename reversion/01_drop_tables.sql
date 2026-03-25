-- =============================================================
-- Reversion: Eliminar tablas logicas y rutas fisicas
-- Proyecto: Smart Data - Panorama Macroeconomico del Peru
-- Ejecutar en Databricks SQL para limpiar el ambiente
-- =============================================================

-- Parametro: cambiar segun el ambiente
-- SET catalog_name = catalog_smartdata_dev;

-- =============================================================
-- 1. DROP tablas Gold
-- =============================================================
DROP TABLE IF EXISTS catalog_smartdata_dev.gold.fact_trade_yearly;
DROP TABLE IF EXISTS catalog_smartdata_dev.gold.fact_economic_indicators;
DROP TABLE IF EXISTS catalog_smartdata_dev.gold.dim_trade_partner;
DROP TABLE IF EXISTS catalog_smartdata_dev.gold.dim_indicator;
DROP TABLE IF EXISTS catalog_smartdata_dev.gold.dim_date;

-- =============================================================
-- 2. DROP tablas Silver
-- =============================================================
DROP TABLE IF EXISTS catalog_smartdata_dev.silver.trade_data;
DROP TABLE IF EXISTS catalog_smartdata_dev.silver.economic_indicators;

-- =============================================================
-- 3. DROP schemas
-- =============================================================
DROP SCHEMA IF EXISTS catalog_smartdata_dev.gold CASCADE;
DROP SCHEMA IF EXISTS catalog_smartdata_dev.silver CASCADE;
DROP SCHEMA IF EXISTS catalog_smartdata_dev.bronze CASCADE;

-- =============================================================
-- 4. DROP catalog
-- =============================================================
DROP CATALOG IF EXISTS catalog_smartdata_dev CASCADE;

-- =============================================================
-- 5. DROP external locations
-- =============================================================
DROP EXTERNAL LOCATION IF EXISTS exlt_gold_dev;
DROP EXTERNAL LOCATION IF EXISTS exlt_silver_dev;
DROP EXTERNAL LOCATION IF EXISTS exlt_bronze_dev;

-- =============================================================
-- 6. DROP storage credential
-- =============================================================
-- DROP STORAGE CREDENTIAL IF EXISTS `credential-smartdata-dev`;
-- NOTA: Descomentar solo si se desea eliminar la credencial
