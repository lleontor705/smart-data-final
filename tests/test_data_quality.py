"""Data quality tests for the medallion pipeline - Panorama Macroeconomico."""

import pytest
import yaml
from pathlib import Path


def test_datasets_config_valid():
    """Verify datasets.yaml is valid and has all 3 data sources."""
    config_path = Path(__file__).parent.parent / "config" / "datasets.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    assert "datasets" in config
    assert "medallion" in config

    datasets = config["datasets"]
    assert "bcrp_economic" in datasets
    assert "worldbank_peru" in datasets
    assert "comtrade_peru" in datasets


def test_bcrp_series_defined():
    """Verify BCRP series are properly configured."""
    config_path = Path(__file__).parent.parent / "config" / "datasets.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    bcrp = config["datasets"]["bcrp_economic"]
    assert bcrp["source"] == "Banco Central de Reserva del Peru"
    assert bcrp["frequency"] == "monthly"

    series = bcrp["series"]
    assert len(series) == 6

    series_ids = [s["id"] for s in series]
    assert "PN01288PM" in series_ids  # PBI
    assert "PN01270PM" in series_ids  # IPC
    assert "PD04638PD" in series_ids  # TC Venta
    assert "PD04639PD" in series_ids  # TC Compra
    assert "PN00015MM" in series_ids  # Tasa Referencia
    assert "PN01207PM" in series_ids  # Balanza Comercial


def test_worldbank_indicators_defined():
    """Verify World Bank indicators are properly configured."""
    config_path = Path(__file__).parent.parent / "config" / "datasets.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    wb = config["datasets"]["worldbank_peru"]
    assert wb["source"] == "World Bank Open Data"
    assert wb["frequency"] == "yearly"
    assert wb["country_code"] == "PE"
    assert len(wb["indicators"]) == 10


def test_comtrade_config_defined():
    """Verify Comtrade trade data is properly configured."""
    config_path = Path(__file__).parent.parent / "config" / "datasets.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    ct = config["datasets"]["comtrade_peru"]
    assert ct["source"] == "UN Comtrade Database"
    assert ct["reporter_code"] == 604
    assert len(ct["flow_codes"]) == 2
    assert len(ct["top_partners"]) == 5


def test_medallion_layers_defined():
    """Verify Bronze/Silver/Gold layers are configured."""
    config_path = Path(__file__).parent.parent / "config" / "datasets.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    medallion = config["medallion"]
    assert "bronze" in medallion
    assert "silver" in medallion
    assert "gold" in medallion

    assert medallion["bronze"]["storage"] == "cosmosdb"
    assert len(medallion["bronze"]["collections"]) == 3

    assert medallion["silver"]["storage"] == "databricks_delta"
    assert len(medallion["silver"]["tables"]) == 2

    assert medallion["gold"]["storage"] == "azure_sql"
    assert len(medallion["gold"]["tables"]) == 5


def test_environment_configs_exist():
    """Verify environment configs exist for dev, pre, pro."""
    config_dir = Path(__file__).parent.parent / "config" / "environments"
    assert (config_dir / "dev.tfvars").exists()
    assert (config_dir / "pre.tfvars").exists()
    assert (config_dir / "pro.tfvars").exists()
