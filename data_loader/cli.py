import click
import os
import json
from data_loader.loader import get_data_loader

# Define the path to the mappings file
MAPPINGS_FILE = os.path.join(os.path.dirname(__file__), "../config/mappings.json")

@click.command()
@click.option("--data_path", required=True, help="Path to data to map")
@click.option("--mappings", required=True, type=click.Path(exists=True), help="Path to the mappings JSON file")
def load_dataset(data_path, mappings):
    """
    CLI command to load mappings and process a given dataset.
    
    Example Usage:
        data_loader --dataset_name sales --data path/to/file.csv
    """
    data_loader = get_data_loader()

    try:
        with open(mappings, "r") as f:
            mapping_config = json.load(f)
    except Exception as e:
        click.echo(f"❌ Failed to load mappings from {mappings}: {e}")
        print('eep')
        return

    required_keys = {"table_name", "column_mappings"}
    if not required_keys.issubset(mapping_config.keys()):
        click.echo(f"❌ Invalid mappings format in {mappings}. Expected keys: {required_keys}")
        return

    table_name = mapping_config["table_name"]
    column_mappings = mapping_config["column_mappings"]
    
    dataset_config = {
        "csv_path": data_path,
        "table_name": table_name,
        "column_mappings": column_mappings
    }

    try:
        click.echo(f"Loading dataset from {data_path} using mappings {mappings}")
        data_loader.process_csv(dataset_config)
        click.echo(f"Successfully loaded dataset into table: {table_name}")
    except Exception as e:
        click.echo(f"❌ Error processing dataset: {e}")


def main():
    load_dataset()

if __name__ == "__main__":
    main()