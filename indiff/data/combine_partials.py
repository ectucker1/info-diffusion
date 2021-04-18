from pathlib import Path
import click
import pandas as pd
import os


@click.command()
def main():
    partial_folder = Path('data/raw')
    dataset_files = partial_folder.glob('*.h5')

    partials = []

    for file in dataset_files:
        print('Reading file', file)
        partials.append(pd.read_hdf(file))

    print('Combining...')
    combined = pd.concat(partials)

    combined.to_hdf('data/combined.h5', 'dataset')


if __name__ == '__main__':
    main()
