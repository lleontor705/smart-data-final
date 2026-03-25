# Smart Data Pipeline - Panorama Macroeconomico del Peru

Pipeline ETL end-to-end con arquitectura **Medallion (Bronze / Silver / Gold)** sobre Azure Databricks con **Unity Catalog**, desplegado mediante CI/CD con GitHub Actions en tres ambientes (dev / pre / pro).

---

## Arquitectura General

```
                          ┌───────────────────────────────────┐
                          │       GitHub Actions CI/CD        │
                          │    develop → dev | main → pro     │
                          └──────────┬──────────┬─────────────┘
                                     │ Terraform│ Notebooks
                          ┌──────────▼──────────▼─────────────┐
                          │          AZURE CLOUD               │
  ┌────────────┐          │                                    │
  │  BCRP API  │──┐       │  ┌────────────┐   ┌────────────┐  │
  └────────────┘  │       │  │  Key Vault │   │  Entra ID  │  │
  ┌────────────┐  │       │  │ (Secretos) │   │ (Grupos)   │  │
  │ World Bank │──┼──────►│  └─────┬──────┘   └─────┬──────┘  │
  └────────────┘  │       │        │                 │         │
  ┌────────────┐  │       │  ┌─────▼─────────────────▼──────┐  │
  │ UN Comtrade│──┘       │  │    DATABRICKS (Premium)      │  │
  └────────────┘          │  │    Unity Catalog + PySpark    │  │
                          │  │                               │  │
                          │  │  ┌─────────┐  BRONZE          │  │
                          │  │  │CosmosDB │◄─ Ingesta APIs   │  │
                          │  │  └────┬────┘                  │  │
                          │  │       │                       │  │
                          │  │  ┌────▼────┐  SILVER          │  │
                          │  │  │  Delta  │◄─ Limpieza       │  │
                          │  │  │  Lake   │  Estandarizacion │  │
                          │  │  └────┬────┘                  │  │
                          │  │       │                       │  │
                          │  │  ┌────▼────┐  GOLD            │  │
                          │  │  │  Delta  │◄─ Star Schema    │  │
                          │  │  │  + SQL  │  Dimensiones     │  │
                          │  │  └────┬────┘  Hechos          │  │
                          │  │       │                       │  │
                          │  └───────┼───────────────────────┘  │
                          │          │                          │
                          │  ┌───────▼────────┐                 │
                          │  │  Azure SQL DB   │                │
                          │  └───────┬─────────┘                │
                          └──────────┼──────────────────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │      Power BI       │
                          │   (Dashboards)      │
                          └─────────────────────┘
```

---

## Fuentes de Datos

| Fuente | API | Indicadores | Capa Raw |
|--------|-----|-------------|----------|
| **BCRP** | [estadisticas.bcrp.gob.pe](https://estadisticas.bcrp.gob.pe/estadisticas/series/api) | PBI, IPC, tipo de cambio, tasa de referencia, balanza comercial | CosmosDB `bcrp_indicators` |
| **World Bank** | [api.worldbank.org](https://api.worldbank.org/v2/) | Poblacion, PBI, PBI per capita, inflacion, desempleo, pobreza, educacion, salud, IED, exportaciones | CosmosDB `worldbank_indicators` |
| **UN Comtrade** | [comtradeapi.un.org](https://comtradeapi.un.org/) | Exportaciones/importaciones Peru con China, EEUU, Brasil, Chile, Colombia | CosmosDB `comtrade_peru` |

---

## Arquitectura Medallion

### Bronze - Ingesta (CosmosDB)
- Datos crudos sin transformacion desde APIs publicas
- Almacenamiento JSON nativo en CosmosDB
- Particionado por categoria
- Timestamp de ingesta para trazabilidad

### Silver - Limpieza (Delta Lake - Unity Catalog)
- Deduplicacion y validacion de tipos
- Estandarizacion de periodos (BCRP: `Ene.2020` → `2020-01-01`)
- Manejo de nulos y valores atipicos
- Union de fuentes (BCRP + World Bank → `economic_indicators`)
- Tablas: `economic_indicators`, `trade_data`

### Gold - Modelo Dimensional (Delta Lake + Azure SQL)
- **Dimensiones**: `dim_date`, `dim_indicator`, `dim_trade_partner`
- **Hechos**: `fact_economic_indicators` (grano mensual), `fact_trade_yearly` (grano anual)
- Calculo de variacion interanual (YoY)
- Escritura dual: Unity Catalog (Delta) + Azure SQL (Power BI)

---

## Estructura del Proyecto

```
partial-smart-data/
├── .github/workflows/                 # CI/CD (YAML)
│   ├── deploy-infra.yml              #   Terraform (dev/pre/pro)
│   └── deploy-notebooks.yml          #   Databricks notebooks + workflow
├── datasets/                          # Insumos del ETL
│   └── README.md                     #   Documentacion de APIs y fuentes
├── dashboard/                         # Visualizacion (Power BI)
│   └── README.md                     #   Conexion y graficos sugeridos
├── reversion/                         # Scripts de limpieza (DROP)
│   ├── 01_drop_tables.sql            #   SQL para eliminar tablas y catalog
│   └── 02_drop_tables.py             #   Notebook Databricks de reversion
├── seguridad/                         # GRANTS, usuarios y grupos
│   └── 01_grants.sql                 #   SQL con GRANT por grupo Entra ID
├── PrepAmb/                           # Preparacion de ambiente
│   ├── 00_create_catalog.py          #   Unity Catalog: catalog + schemas + tablas
│   └── 01_grants.py                  #   Notebook de grants (ejecutable)
├── proceso/                           # Notebooks ETL (incluye PrepAmb y Grants)
│   ├── 00_config.py                  #   Configuracion compartida
│   ├── 01_ingest_bcrp.py             #   Bronze: Ingesta BCRP (6 series)
│   ├── 02_ingest_worldbank.py        #   Bronze: Ingesta World Bank (10 indicadores)
│   ├── 03_ingest_comtrade.py         #   Bronze: Ingesta UN Comtrade (5 socios)
│   ├── 04_silver_clean_economic.py   #   Silver: Limpieza indicadores economicos
│   ├── 05_silver_clean_trade.py      #   Silver: Limpieza comercio exterior
│   ├── 06_gold_dim_tables.py         #   Gold: Dimensiones (date, indicator, partner)
│   ├── 07_gold_fact_tables.py        #   Gold: Hechos + SQL Server
│   ├── 08_prepamb_create_catalog.py  #   PrepAmb: Creacion de catalogo
│   └── 09_grants.py                  #   Seguridad: Grants por grupo
├── certificaciones/                   # Certificaciones (.png y enlaces)
├── evidencias/                        # Capturas de ejecucion (.png)
│   └── README.md                     #   Guia de evidencias requeridas
├── infra/                             # Terraform IaC
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
│       ├── databricks/               #   Workspace + cluster + secret scope
│       ├── cosmosdb/                 #   CosmosDB account + database + containers
│       ├── sql-server/               #   Azure SQL Server + database
│       ├── keyvault/                 #   Key Vault + access policies
│       ├── storage/                  #   ADLS Gen2 (bronze/silver/gold containers)
│       └── data-factory/             #   Azure Data Factory
├── config/
│   ├── datasets.yaml                 # Definicion de fuentes y mapeo medallion
│   └── environments/
│       ├── dev.tfvars                # Desarrollo (Premium, DS3_v2, 1-2 workers)
│       ├── pre.tfvars                # Pre-produccion (Premium, DS3_v2, 1-4 workers)
│       └── pro.tfvars                # Produccion (Premium, DS4_v2, 2-8 workers)
├── pipelines/databricks/              # Notebooks originales (usados por CI/CD)
│   ├── setup/                        #   00_create_catalog.py + 01_grants.py
│   ├── bronze/                       #   Ingesta: BCRP, WorldBank, Comtrade
│   ├── silver/                       #   Limpieza: economic, trade
│   └── gold/                         #   Modelo: dimensiones + hechos
├── scripts/
│   ├── setup-infra.sh                # Setup manual con AZ CLI
│   └── init_gold_schema.sql          # DDL Azure SQL (dims + facts + indices)
├── tests/
│   └── test_data_quality.py          # Validacion de configuracion
└── README.md                          # Documentacion del proyecto
```

---

## Workflow DAG (Databricks)

```
                    ┌──────────────────┐
                    │  setup_catalog   │
                    │ (Unity Catalog)  │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
     ┌────────────┐ ┌────────────┐ ┌────────────┐
     │  bronze    │ │  bronze    │ │  bronze    │
     │  BCRP     │ │ WorldBank  │ │ Comtrade   │
     └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
           │              │              │
           └──────┬───────┘              │
                  ▼                      ▼
         ┌────────────┐         ┌────────────┐
         │  silver    │         │  silver    │
         │ economic   │         │  trade     │
         └─────┬──────┘         └─────┬──────┘
               │                      │
               └──────────┬───────────┘
                          ▼
                 ┌────────────────┐
                 │  gold          │
                 │  dim_tables    │
                 └────────┬───────┘
                          ▼
                 ┌────────────────┐
                 │  gold          │
                 │  fact_tables   │
                 └────────┬───────┘
                          ▼
                 ┌────────────────┐
                 │  security      │
                 │  grants        │
                 └────────────────┘
```

---

## Seguridad y Gobernanza

### Unity Catalog
- **Catalog**: `catalog_smartdata_{env}` por ambiente
- **Schemas**: `bronze`, `silver`, `gold`
- **External Tables**: Delta Lake con `LOCATION` en ADLS Gen2
- **Storage Credential**: Managed Identity via Access Connector

### Grupos (Microsoft Entra ID + Databricks Identity)

| Grupo | Permisos Catalog | Permisos Schema | External Locations |
|-------|-----------------|-----------------|-------------------|
| `datareaders` | USE CATALOG | USE SCHEMA + SELECT | READ FILES |
| `dataengineers` | USE CATALOG | USE SCHEMA + SELECT + MODIFY + CREATE TABLE | READ + WRITE FILES |
| `dataadmins` | ALL PRIVILEGES | (hereda) | ALL PRIVILEGES |

### Secretos (Azure Key Vault)
- `cosmos-endpoint`, `cosmos-primary-key`
- `storage-account-name`, `storage-account-key`
- `sql-connection-string`, `sql-admin-password`

---

## Entornos de Despliegue

| Entorno | Branch | Trigger | Databricks | CosmosDB | SQL |
|---------|--------|---------|-----------|----------|-----|
| **dev** | `develop` | Push | Premium, 1-2 workers | 400 RU/s | Basic, 2 GB |
| **pre** | `main` (PR) | Pull Request | Premium, 1-4 workers | 800 RU/s | S0, 10 GB |
| **pro** | `main` | Merge + aprobacion | Premium, 2-8 workers | 2000 RU/s | S1, 50 GB |

---

## Stack Tecnologico

| Componente | Tecnologia | Proposito |
|-----------|-----------|-----------|
| Procesamiento | Azure Databricks (PySpark) | ETL medallion |
| Gobernanza | Unity Catalog | Catalogo, linaje, permisos |
| Bronze Storage | Azure CosmosDB | Datos crudos JSON |
| Silver/Gold Storage | Delta Lake (ADLS Gen2) | Tablas externas |
| Gold Serving | Azure SQL Database | Modelo estrella para BI |
| Orquestacion | Azure Data Factory | Pipelines |
| Secretos | Azure Key Vault | Credenciales y conexiones |
| Identidad | Microsoft Entra ID | Grupos y roles |
| Visualizacion | Power BI | Dashboards |
| IaC | Terraform | Infraestructura multi-env |
| CI/CD | GitHub Actions | Deploy automatizado |

---

## Quick Start

```bash
# 1. Login en Azure
az login
az account set --subscription <subscription-id>

# 2. Desplegar infraestructura (dev)
cd infra
terraform init \
  -backend-config="resource_group_name=rg-tfstate" \
  -backend-config="storage_account_name=stsmartdatatfstate" \
  -backend-config="container_name=tfstate" \
  -backend-config="key=smartdata-dev.tfstate"
terraform plan -var-file="../config/environments/dev.tfvars"
terraform apply -var-file="../config/environments/dev.tfvars"

# 3. O usar el script de setup manual
bash scripts/setup-infra.sh

# 4. Los notebooks se despliegan automaticamente al hacer push a develop
git push origin develop
```

---

## CI/CD Pipelines

### `deploy-infra.yml`
1. Terraform Plan (valida cambios)
2. Terraform Apply (despliega recursos)
3. Promote to PRO (requiere aprobacion manual)

### `deploy-notebooks.yml`
1. Valida conectividad con Databricks
2. Crea directorios en workspace (`/smartdata/{env}/`)
3. Despliega notebooks (setup, bronze, silver, gold)
4. Crea workflow con DAG de dependencias
5. Ejecuta ETL completo y monitorea (timeout: 60 min)
6. Aplica grants de seguridad al finalizar

---

## Autor

**Luis Humberto Leon Torres**
Ingenieria de Datos con Databricks - Parcial Final
