import click
import os
import json
from data_loader.loader import get_data_loader

# Define the path to the mappings file
MAPPINGS_FILE = os.path.join(os.path.dirname(__file__), "../config/mappings.json")

@click.command()
@click.option("--data-dir", required=True, type=click.Path(exists=True), help="Directory where dependency TSV files are stored")
@click.option("--mappings", required=True, type=click.Path(exists=True), help="Path to the mappings JSON file")
def load_dataset(mappings, data_dir):
    """
    CLI command to load mappings and process a given dataset.
    
    Example Usage:
        data_loader --dataset_name sales --data path/to/file.csv
    """
    data_loader = get_data_loader()
    try:
        click.echo(f"🚀 Processing dataset using mappings: {mappings}")
        data_loader.process_mappings(mappings, data_dir)
        click.echo(f"✅ Successfully processed dataset!")
    except Exception as e:
        click.echo(f"❌ Error processing dataset: {e}")


def main():
    load_dataset()

if __name__ == "__main__":
    main()