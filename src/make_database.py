# -*- coding: utf-8 -*-
import click
import logging
import pandas as pd
import sqlalchemy as sa
import os


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """
    Runs scripts to turn raw data from (../raw) into
    a queryable/indexable form (an SQL database saved in ../raw).
    """
    logger = logging.getLogger(__name__)
    logger.info('making database from raw data')

    raw_data_files = [filename for filename in os.listdir(input_filepath)
                      if filename.endswith('csv.gz')]

    db_path = os.path.join(output_filepath, 'raw_data.sqlite')
    e = sa.create_engine('sqlite:///' + db_path)
    for raw_data_file in sorted(raw_data_files):
        pd.read_csv(f'{input_filepath}/{raw_data_file}',
                    compression='gzip', index_col=0) \
            .to_sql("raw_data", e,
                    if_exists='append', index=False)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    main()
