-- =============================================================================
-- Gold Layer Schema - Azure SQL Database
-- Star schema optimizado para Power BI
-- Dashboard: Panorama Macroeconomico (BCRP + World Bank + Comtrade)
-- =============================================================================

-- Dimension: Fecha
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_date')
CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    month_name      NVARCHAR(20) NOT NULL,
    week_of_year    INT NOT NULL,
    day_of_week     INT NOT NULL,
    day_name        NVARCHAR(20) NOT NULL,
    is_weekend      BIT NOT NULL
);

-- Dimension: Indicador Economico
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_indicator')
CREATE TABLE dim_indicator (
    indicator_key   NVARCHAR(50) PRIMARY KEY,
    indicator_name  NVARCHAR(200) NOT NULL,
    category        NVARCHAR(50) NOT NULL,
    source          NVARCHAR(50) NOT NULL,
    unit            NVARCHAR(50)
);

-- Dimension: Socio Comercial
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_trade_partner')
CREATE TABLE dim_trade_partner (
    partner_key     INT PRIMARY KEY,
    partner_name    NVARCHAR(100) NOT NULL,
    region          NVARCHAR(50)
);

-- Fact: Indicadores Economicos
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_economic_indicators')
CREATE TABLE fact_economic_indicators (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    date_key        INT NOT NULL REFERENCES dim_date(date_key),
    indicator_key   NVARCHAR(50) NOT NULL REFERENCES dim_indicator(indicator_key),
    source          NVARCHAR(50),
    category        NVARCHAR(50),
    value_numeric   DECIMAL(18,4),
    year            INT,
    month           INT,
    prev_year_value DECIMAL(18,4),
    yoy_change      DECIMAL(10,2),
    load_timestamp  DATETIME2 DEFAULT GETUTCDATE()
);

-- Fact: Comercio Exterior Anual
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_trade_yearly')
CREATE TABLE fact_trade_yearly (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    date_key            INT NOT NULL REFERENCES dim_date(date_key),
    partner_key         INT NOT NULL REFERENCES dim_trade_partner(partner_key),
    flow_code           NVARCHAR(5) NOT NULL,
    flow_desc           NVARCHAR(50),
    cmd_code            NVARCHAR(20),
    cmd_desc            NVARCHAR(200),
    trade_value         DECIMAL(18,2),
    trade_value_millions DECIMAL(12,2),
    net_weight          DECIMAL(18,2),
    year                INT,
    prev_year_value     DECIMAL(18,2),
    yoy_change_pct      DECIMAL(10,2),
    load_timestamp      DATETIME2 DEFAULT GETUTCDATE()
);

-- Indices para performance en Power BI
CREATE NONCLUSTERED INDEX IX_fact_econ_date ON fact_economic_indicators(date_key);
CREATE NONCLUSTERED INDEX IX_fact_econ_indicator ON fact_economic_indicators(indicator_key);
CREATE NONCLUSTERED INDEX IX_fact_econ_category ON fact_economic_indicators(category);
CREATE NONCLUSTERED INDEX IX_fact_trade_date ON fact_trade_yearly(date_key);
CREATE NONCLUSTERED INDEX IX_fact_trade_partner ON fact_trade_yearly(partner_key);
CREATE NONCLUSTERED INDEX IX_fact_trade_flow ON fact_trade_yearly(flow_code);

PRINT 'Gold layer schema created successfully - Panorama Macroeconomico';
