import click


@click.command()
@click.option('--validation_type', required=True, type=click.Choice(['column_hash', 'row_hash', 'validation_summary']),
              help='Pass the name of validation')
@click.option('--source_table_name', required=True, type=str, help='Pass the fully qualified name of Source Table')
@click.option('--target_table_name', required=True, type=str, help='Pass the fully qualified name of Target Table')
@click.option('--source_name', required=True, type=str,
              help='Pass the name of the source in Infoworks in which source tables are present')
@click.option('--target_name', required=True, type=str,
              help='Pass the name of the source in Infoworks in which target tables are present')
@click.option('--compute_type', required=True, type=click.Choice(['spark', 'bigquery', 'snowflake']),
              help='Pass the compute name on which the validations are run')
@click.option('--group_by_cols_for_checksum', type=str,
              help='Pass the comma separated list of columns on which group by is performed.')
@click.option('--agg_cols_for_checksum', type=str,
              help='Comma separated list of columns on which aggregation is performed. By default all columns part of source table')
@click.option('--distinct_count_cols', type=str,
              help='Pass the comma separated list of columns on which distinct count is performed during row level validation')
@click.option('--join_cols_row_checksum', type=str,
              help='Pass the comma separated list of columns on which the source and target tables are joined in row checksum pipelines')
@click.option('--regex_exp', type=str,
              help='Regular expression to remove ascii characters from data. Default: \'[\\W&&[^\\s+]]\'')
@click.option('--remove_ascii_from_cols', type=str,
              help='List of columns from which ascii characters are to be removed. Default: All string columns if regex_replace is enabled')
@click.option('--regex_replace', required=True, type=click.Choice(["true", "false"]),
              help='true/false. Variable to enable/disable removing of ascii characters from string columns')
@click.option('--data_profiling', required=True, type=click.Choice(["true", "false"]),
              help='true/false. If set to true, data profiling columns will be enabled in the column checksum pipeline')
@click.option('--natural_keys', type=str,
              help='Comma separated list of columns to be configured as natural keys')
@click.option('--pipeline_target_schema_name', required=True, type=str,
              help='Pass the target schema name in which pipeline targets are to be created')
@click.option('--domain_name', type=str, help='Pass the domain name in which pipeline has to be created')
@click.option('--environment_name', required=True, type=str,
              help='Pass the environment name in which pipeline has to be created')
@click.option('--storage_name', required=True, type=str,
              help='Pass the storage name the pipeline has to use')
@click.option('--compute_name', required=True, type=str, help='Pass the compute name the pipeline has to use')
def main(validation_type, source_table_name, target_table_name, source_name, target_name, compute_type,
         group_by_cols_for_checksum,
         agg_cols_for_checksum, distinct_count_cols, join_cols_row_checksum, regex_exp, remove_ascii_from_cols,
         regex_replace, data_profiling, natural_keys, pipeline_target_schema_name, domain_name, environment_name,
         storage_name, compute_name):
    global refresh_token
    refresh_token = config.get('environment_details', 'refresh_token')
    if domain_name is None:
        # Read from config.ini
        domain_id = config.get('environment_details', 'domain_id')
    else:
        # Get the domain id
        domain_id = get_domain_id_from_name()

    if environment_name is None:
        # Read from config.ini
        environment_id = config.get('environment_details', 'environment_id')
    else:
        # Get the domain id
        environment_id = get_environment_id_from_name()

    if storage_name is None:
        # Read from config.ini
        storage_id = config.get('environment_details', 'storage_id')
    else:
        # Get the domain id
        storage_id = get_storage_id_from_name()

    if compute_name is None:
        # Read from config.ini
        compute_id = config.get('environment_details', 'compute_id')
    else:
        # Get the domain id
        compute_id = get_compute_id_from_name()

    for i in range(num_fetch_threads):
        worker = Thread(target=create_import_validation_pipelines, args=(i, job_queue,))
        worker.setDaemon(True)
        worker.start()

    input_file = csv.DictReader(open("input_data.csv"))
    for row in input_file:
        job_queue.put((row, domain_id, environment_id, storage_id, compute_id))

    print('*** Main thread waiting ***')
    job_queue.join()
    print('*** Done ***')


def get_domain_id_from_name():
    return ""


def get_environment_id_from_name():
    return ""


def get_storage_id_from_name():
    return ""


def get_compute_id_from_name():
    return ""


if __name__ == '__main__':
    main()
