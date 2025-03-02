import click
import os
import json
from data_loader.loader import get_data_loader

# Define the path to the mappings file
MAPPINGS_FILE = os.path.join(os.path.dirname(__file__), "../config/mappings.json")

@click.command()
@click.option("--dataset_name", required=True, help="Name of the dataset to process")
@click.option("--data_path", required=True, type=click.Path(exists=True), help="Path to the CSV file")
def load_dataset(dataset_name, data_path):
    """
    CLI command to load mappings and process a given dataset.
    
    Example Usage:
        data_loader --dataset_name sales --data path/to/file.csv
    """
    data_loader = get_data_loader()

    # Load mappings from JSON
    try:
        with open(MAPPINGS_FILE, "r") as f:
            mappings = json.load(f)
    except Exception as e:
        click.echo(f"❌ Failed to load mappings: {e}")
        return

    if dataset_name not in mappings:
        click.echo(f"❌ Dataset '{dataset_name}' not found in mappings.json")
        return

    # Process the dataset
    try:
        click.echo(f"🚀 Loading dataset: {dataset_name} from {data_path}")
        data_loader.process_csv(MAPPINGS_FILE, dataset_name, data_path)
        click.echo(f"✅ Successfully loaded dataset: {dataset_name}")
    except Exception as e:
        click.echo(f"❌ Error processing dataset '{dataset_name}': {e}")

def main():
    load_dataset()

if __name__ == "__main__":
    main()