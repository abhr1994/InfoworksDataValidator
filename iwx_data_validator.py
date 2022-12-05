import json
import sys
import time
from configparser import ConfigParser
import requests
import csv
import click
import urllib
from colorama import Fore, Style
import sqlparse
MAX_RETRIES = 20
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
session.mount('https://', adapter)
session.mount('http://', adapter)

# Read global variables
config = ConfigParser()
config.read('config.ini')
proxy_host = "{}://{}".format(config.get('infoworks_details', 'protocol'), config.get('infoworks_details', 'host'))
proxy_port = config.get('infoworks_details', 'port')
delegation_token = ""
refresh_token = ""

import queue
from threading import Thread

num_fetch_threads = 10
job_queue = queue.Queue(maxsize=100)


def print_success(message):
    print(Fore.GREEN + message)
    print(Style.RESET_ALL)


def print_error(message):
    print(Fore.RED + message)
    print(Style.RESET_ALL)


def print_warning(message):
    print(Fore.YELLOW + message)
    print(Style.RESET_ALL)


def print_info(message):
    print(Fore.CYAN + message)
    print(Style.RESET_ALL)


def refresh_delegation_token():
    global delegation_token
    global refresh_token
    url = "{ip}:{port}/v3/security/token/access/".format(ip=proxy_host, port=proxy_port)
    headers = {
        'Authorization': 'Basic ' + refresh_token,
        'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, verify=False)
    print_info(response.text)
    delegation_token = response.json().get("result").get("authentication_token")


def poll_job(job_id):
    failed_count = 0
    retries = 3
    polling_frequency = 5
    response = {}
    timeout = time.time() + 300
    while True:
        if time.time() > timeout:
            break
        try:
            job_monitor_url = f"{proxy_host}:{proxy_port}/v3/admin/jobs/{job_id}"
            response = requests.get(job_monitor_url,
                                    headers={'Authorization': 'Bearer ' + delegation_token,
                                             'Content-Type': 'application/json'}, verify=False)
            if response.status_code == 406:
                refresh_delegation_token()
                response = requests.get(job_monitor_url,
                                        headers={'Authorization': 'Bearer ' + delegation_token,
                                                 'Content-Type': 'application/json'}, verify=False)
            parsed_response = json.loads(response.content)
            result = parsed_response.get('result', None)
            if len(result) != 0:
                job_status = result["status"]
                if job_status in ["completed", "failed", "aborted"]:
                    if job_status == "completed":
                        return 0
                    else:
                        return 1
            else:
                if failed_count >= retries - 1:
                    return 1
                failed_count = failed_count + 1
        except Exception as e:
            if failed_count >= retries - 1:
                print(response)
                return 1
            failed_count = failed_count + 1
        time.sleep(polling_frequency)


def trigger_pipeline_metadata_build(domain_id, pipeline_id):
    url_for_pipeline_metadata_build = '{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/jobs'.format(
        proxy_host=proxy_host,
        proxy_port=proxy_port,
        domain_id=domain_id,
        pipeline_id=pipeline_id)

    request_body = {
        "job_type": "pipeline_metadata"
    }

    response = requests.post(url_for_pipeline_metadata_build, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                       'Content-Type': 'application/json'},
                             data=json.dumps(request_body), verify=False)
    if response.status_code == 406:
        refresh_delegation_token()
        response = requests.post(url_for_pipeline_metadata_build,
                                 headers={'Authorization': 'Bearer ' + delegation_token,
                                          'Content-Type': 'application/json'},
                                 data=json.dumps(request_body), verify=False)
    parsed_response = json.loads(response.content)
    result = parsed_response.get('result', None)
    job_id = result["id"]
    if poll_job(job_id) == 0:
        print_success("Pipeline metadata build job finished successfully")
    else:
        print_error("Pipeline metadata build job failed")


def import_sql(sql, domain_id, pipeline_id):
    sql_import_body = {
        "dry_run": False,
        "sql": sql,
        "sql_import_configuration": {
            "quoted_identifier": "DOUBLE_QUOTE",
            "sql_dialect": "LENIENT"
        }
    }
    sql_import_body_json = json.dumps(sql_import_body)
    # print(f"SQL import body json : {sql_import_body_json} \n\n")
    url_for_sql_import = f"{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/sql-import"
    # print("URL for sql Import: ", url_for_sql_import)
    # print("\n")
    try:
        response = session.post(url_for_sql_import, headers={'Authorization': 'Bearer ' + delegation_token,
                                                             'Content-Type': 'application/json'},
                                data=sql_import_body_json,
                                timeout=60, verify=False)
        if response.status_code == 406:
            refresh_delegation_token()
            response = requests.post(url_for_sql_import, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                  'Content-Type': 'application/json'},
                                     data=sql_import_body_json, verify=False, timeout=60)
        parsed_response = json.loads(response.content)
        result = parsed_response.get('result', None)
        if result:
            print_success("pipeline imported successfully! \n")
            return True
        else:
            print_error("Failed to import sql \n")
            print_error(json.dumps(parsed_response))
            return False
    except Exception as e:
        print_error("Failed to import sql \n")
        print_error(str(e))
        return False


def get_all_columns(source_name, table_name):
    all_columns_with_datatype_dict = {}
    filter_condition = json.dumps({"name": source_name})
    url_to_list_source = f'{proxy_host}:{proxy_port}/v3/sources' + f"?filter={{filter_condition}}".format(
        filter_condition=filter_condition)
    response = requests.request("GET", url_to_list_source, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                    'Content-Type': 'application/json'},
                                verify=False)
    if response.status_code == 200 and len(response.json().get("result", [])) > 0:
        result = response.json().get("result", [])
        source_id = result[0]["id"]
        source_type = result[0]["type"].lower()
        source_sub_type = result[0]["sub_type"].lower()
        environment_id = result[0]["environment_id"]
        compute_type = get_environment_type_from_id(environment_id).lower()
        if source_id is not None:
            if source_type == "metadatacrawl" and source_sub_type == "bigquerymetacrawl":
                # For BigQuery as sync source
                datasetNameAtSource, origTableName = table_name.split(".")
                filter_condition_dict = {"datasetNameAtSource": datasetNameAtSource,
                                         "origTableName": origTableName}
            elif source_type == "metadatacrawl" and source_sub_type == "snowflakemetacrawl":
                # For Snowflake as sync source
                db_name, schema_name, table = table_name.split(".")
                filter_condition_dict = {"catalog_name": db_name,
                                         "schemaNameAtSource": schema_name,
                                         "origTableName": table_name}
            elif source_type == "metadatacrawl" and source_sub_type in ["deltatables", "hivecrawl"]:
                # For spark delta sync and hive metadata sync
                schema_name, table = table_name.split(".")
                filter_condition_dict = {"schemaNameAtSource": schema_name,
                                         "origTableName": table_name}
            else:
                if compute_type.lower() == "snowflake":
                    db_name, schema_name, table = table_name.split(".")
                    filter_condition_dict = {"configuration.target_database_name": db_name,
                                             "configuration.target_schema_name": schema_name,
                                             "configuration.target_table_name": table_name}
                elif compute_type.lower() == "bigquery":
                    dataset_name, table = table_name.split(".")
                    filter_condition_dict = {"configuration.target_dataset_name": dataset_name,
                                             "configuration.target_table_name": table_name}
                elif compute_type.lower() in ["databricks", "dataproc"]:
                    schema_name, table = table_name.split(".")
                    filter_condition_dict = {"configuration.target_schema_name": schema_name,
                                             "configuration.target_table_name": table_name}
                else:
                    filter_condition_dict = {}

            filter_condition = json.dumps(filter_condition_dict)
            url_to_list_tables = f'{proxy_host}:{proxy_port}/v3/sources/{source_id}/tables' + f"?filter={{filter_condition}}".format(
                filter_condition=filter_condition)
            response = requests.request("GET", url_to_list_tables,
                                        headers={'Authorization': 'Bearer ' + delegation_token,
                                                 'Content-Type': 'application/json'}, verify=False)
            if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                result = response.json().get("result", [])
                columns = result[0]["columns"]
                all_columns_with_datatype_dict = {}
                for i in columns:
                    if not i["name"].lower().startswith("ziw"):
                        all_columns_with_datatype_dict[i["name"]] = i['target_sql_type']
            else:
                print_error("Failed to get column names for table")

    return all_columns_with_datatype_dict


def prepare_sql_import_query_columnchksum(compute_type, domain_id, pipeline_id, distinct_count_cols, chksum_cols,
                                          group_by_cols,
                                          source_table_name,
                                          target_table_name,
                                          pipeline_tgt_schema_name,
                                          pipeline_tgt_table_name, regex_replace=True, regex_exp=None,
                                          remove_ascii_from_cols=None, src_columns_with_datatype={},
                                          data_profiling=True):
    if regex_replace:
        if regex_exp is None:
            regex_exp = '[\\W&&[^\\s+]]'
    src_schema, src_table = source_table_name.split('.')
    tgt_schema, tgt_table = target_table_name.split('.')
    src_columns = src_columns_with_datatype.keys()
    src_columns = [i for i in src_columns if (not i.lower().startswith("ziw"))]

    # Handle the group by columns.
    grp_by_statement = []
    grp_by_cols_to_select = []
    for item in group_by_cols:
        if item in src_columns:
            grp_by_statement.append(f"{item}")
            grp_by_cols_to_select.append(f"{item}")
        else:
            item_modified = ''.join(e for e in item if e.isalnum() or e in ["_"]).upper()
            grp_by_statement.append(f"{item} as {item_modified}")
            grp_by_cols_to_select.append(f"{item_modified}")

    # For row validation
    if distinct_count_cols == "" or distinct_count_cols is None:
        distinct_count_cols = []
    if len(distinct_count_cols) > 0:
        columns_to_select_row_validation = distinct_count_cols
    else:
        if len(src_columns_with_datatype) > 0:
            columns_to_select_row_validation = [i for i in src_columns if (not i.lower().startswith("ziw"))]
            if compute_type.lower() == "bigquery":
                # Bigquery supports only 1 column in count distinct. Hence, hash the inputs and then pass it to distinct count
                columns_to_select_row_validation = ["sha256(concat( " + ",".join([f"cast({i} as string)" for i in columns_to_select_row_validation]) + " ))"]
        else:
            columns_to_select_row_validation = []
    count_expression = "1"
    distinct_count_expression = f"DISTINCT {','.join(columns_to_select_row_validation)} "

    if len(chksum_cols) == 0:
        columns_to_select = []
        dict_columns = {}
        for i in src_columns_with_datatype:
            if regex_replace and remove_ascii_from_cols is not None:
                if i.lower() in remove_ascii_from_cols:
                    dict_columns[i] = (f"regexp_replace({i}, '{regex_exp}', '')", src_columns_with_datatype[i])
                else:
                    if (not i.lower().startswith("ziw")) and (
                            not i.lower().startswith("ifw")):
                        dict_columns[i] = (i, src_columns_with_datatype[i])
            elif regex_replace and remove_ascii_from_cols is None:
                if src_columns_with_datatype[i] == 12 and (not i.lower().startswith("ziw")) and (
                        not i.lower().startswith("ifw")):
                    dict_columns[i] = (f"regexp_replace({i}, '{regex_exp}', '')", src_columns_with_datatype[i])
                elif (not i.lower().startswith("ziw")) and (
                        not i.lower().startswith("ifw")):
                    dict_columns[i] = (i, src_columns_with_datatype[i])
            else:
                dict_columns[i] = (i, src_columns_with_datatype[i])

            if (not i.lower().startswith("ziw")) and (not i.lower().startswith("ifw")):
                columns_to_select.append(i)
        if len(dict_columns.keys()) == 0:
            for i in columns_to_select:
                dict_columns[i] = i
    else:
        dict_columns = {}
        for i in chksum_cols:
            if regex_replace and remove_ascii_from_cols is not None:
                if i.lower() in remove_ascii_from_cols:
                    dict_columns[i] = (f"regexp_replace({i}, '{regex_exp}', '')", src_columns_with_datatype[i])
            else:
                dict_columns[i] = (i, src_columns_with_datatype[i])

    select_cols_landing = []
    select_cols_staging = []
    select_cols = []
    join_cols = grp_by_cols_to_select
    select_clause = ""

    for j in dict_columns.keys():
        item_modified = j.lower().replace("(", "").replace(")", "")
        select_cols.append(f"chksum_{item_modified}")
        value, datatype = dict_columns[j]
        if compute_type.lower() == "bigquery":
            select_cols_staging.append(
                f'sum(cast(farm_fingerprint(cast({value} as String)) as numeric)) as source_chksum_{item_modified}')
            select_cols_landing.append(
                f'sum(cast(farm_fingerprint(cast({value} as String)) as numeric)) as target_chksum_{item_modified}')
        elif compute_type.lower() == "snowflake":
            select_cols_staging.append(
                f'sum(md5_number_upper64(cast({value} as String))) as source_chksum_{item_modified}')
            select_cols_landing.append(
                f'sum(md5_number_upper64(cast({value} as String))) as target_chksum_{item_modified}')
        else:
            select_cols_staging.append(f'sum(crc32(cast({value} as String))) as source_chksum_{item_modified}')
            select_cols_landing.append(f'sum(crc32(cast({value} as String))) as target_chksum_{item_modified}')

        if data_profiling:
            # Numeric Columns
            if datatype in [3, 7, 8, 4, -5]:
                for func in ["min", "max", "avg", "sum"]:
                    col_name = f"{func}({value})"
                    col_name_modified = ''.join(e for e in col_name if e.isalnum() or e in ["_"]).upper()
                    select_cols_staging.append(f"{col_name} as src_{col_name_modified}")
                    select_cols_landing.append(f"{col_name} as tgt_{col_name_modified}")
                    select_clause = select_clause + f"src_{col_name_modified}, " + f"tgt_{col_name_modified}, "
            elif datatype == 12:
                col_name = f"max(CHAR_LENGTH({value}))"
                col_name_modified = ''.join(e for e in col_name if e.isalnum() or e in ["_"]).upper()
                select_cols_staging.append(f"{col_name} as src_{col_name_modified}")
                select_cols_landing.append(f"{col_name} as tgt_{col_name_modified}")
                select_clause = select_clause + f"src_{col_name_modified}, " + f"tgt_{col_name_modified}, "

    select_cols_staging.append(
        f"COUNT({distinct_count_expression}) AS SOURCE_DISTINCT_COUNT, COUNT({count_expression}) as SOURCE_COUNT")
    select_cols_landing.append(
        f"COUNT({distinct_count_expression}) AS TARGET_DISTINCT_COUNT, COUNT({count_expression}) as TARGET_COUNT")

    join_clause = ""
    for i, item in enumerate(join_cols):
        join_clause = join_clause + f"SRC_BRANCH.{item}=TGT_BRANCH.{item}"
        if i != len(join_cols) - 1:
            join_clause = join_clause + " AND "

    select_clause = select_clause.strip().strip(
        ",") + ",TARGET_DISTINCT_COUNT,TARGET_COUNT,SOURCE_DISTINCT_COUNT,SOURCE_COUNT, "
    COL_CKSUM_VALIDATION = "CASE "
    for item in select_cols:
        COL_CKSUM_VALIDATION = COL_CKSUM_VALIDATION + f" WHEN target_{item} <> source_{item} then " \
                                                      f"'FAIL' "
    COL_CKSUM_VALIDATION = COL_CKSUM_VALIDATION + " ELSE 'PASS' END"

    DUPLICATE_CHECK = """case when source_count <> source_distinct_count then 'Duplicates in source' 
    when target_count <> target_distinct_count then 'Duplicates in target' 
    else ' No Duplicates Identified' 
    END"""
    if len(join_cols) > 0:
        for i in join_cols:
            select_clause = select_clause + f'{"".join(e for e in i if e.isalnum() or e in ["_"]).upper()}' + " , "
    for item in select_cols:
        select_clause = select_clause + f"""
        case when source_{item} - target_{item} = 0 then 'PASS'
        else 'FAIL'
        END as {item}_result
        """ + ","
    select_clause = select_clause.strip(",")
    sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT '{tgt_table}' as ENTITY_NAME, {select_clause} , {COL_CKSUM_VALIDATION} AS COL_CKSUM_VALIDATION, {DUPLICATE_CHECK} as DUPLICATE_CHECK, case when source_count - target_count = 0 then 'PASS' else 'FAIL' END AS COUNT_VALIDATION, {" , ".join(grp_by_cols_to_select)} FROM ( SELECT {",".join(select_cols_staging)}, {" , ".join(grp_by_cols_to_select)} FROM ( SELECT {" , ".join(grp_by_statement)}  FROM "{src_schema}"."{src_table}" ) GROUP BY {",".join(grp_by_cols_to_select)} ) AS SRC_BRANCH FULL OUTER JOIN ( SELECT {",".join(select_cols_landing)}, {" , ".join(grp_by_cols_to_select)} FROM ( SELECT {",".join(grp_by_statement)}  FROM "{tgt_schema}"."{tgt_table}" ) GROUP BY {",".join(grp_by_cols_to_select)} ) AS TGT_BRANCH ON {join_clause} """
    print_info("prepared following sql for column checksum pipeline: ")
    print(sqlparse.format(sql, reindent=True, keyword_case='upper'))
    print("\n\n")
    return import_sql(sql, domain_id, pipeline_id), select_cols, grp_by_cols_to_select


def prepare_sql_import_query_rowhashvalidation(compute_type, domain_id, pipeline_id, source_table_name,
                                               target_table_name, colchksum_table,
                                               pipeline_tgt_schema_name,
                                               pipeline_tgt_table_name, inner_join_cols, full_outer_join_cols,
                                               src_columns_with_datatype={}):
    tgt_schema, tgt_table = target_table_name.split('.')
    src_schema, src_table = source_table_name.split('.')
    colchksum_table_schema, colchksum_table_name = colchksum_table.split('.')

    FULL_OUTER_JOIN_CLAUSE = ""
    for col_name in full_outer_join_cols:
        FULL_OUTER_JOIN_CLAUSE = f"SOURCE_BRANCH.{col_name} = TARGET_BRANCH.{col_name}"

    columns_to_select_src = []
    columns_to_select_tgt = []
    col_chksum_table_columns = []

    src_columns = [i.upper() for i in src_columns_with_datatype.keys()]

    src_inner_join_clause = ""
    tgt_inner_join_clause = ""
    columns_to_select_inner = []
    for i, item in enumerate(inner_join_cols):
        item_modified = ''.join(e for e in item if e.isalnum() or e in ["_"]).upper()
        if item_modified not in src_columns:
            columns_to_select_inner.append(f"{item} as {item_modified}")
        col_chksum_table_columns.append(f"{item_modified}")
        columns_to_select_src.append(f"{item_modified}")
        columns_to_select_tgt.append(f"{item_modified}")

        src_inner_join_clause = src_inner_join_clause + f"SRC_TABLE.{item_modified}=COLCHSUM_TABLE.{item_modified}"
        tgt_inner_join_clause = tgt_inner_join_clause + f"TGT_TABLE.{item_modified}=COLCHSUM_TABLE.{item_modified}"
        if i != len(inner_join_cols) - 1:
            src_inner_join_clause = src_inner_join_clause + " AND "
            tgt_inner_join_clause = tgt_inner_join_clause + " AND "

    columns_to_select = []
    if compute_type.lower() == "bigquery":
        ROW_HASH = "SHA256(CONCAT("
    elif compute_type.lower() == "snowflake":
        ROW_HASH = "SHA2(CONCAT("
    else:
        ROW_HASH = "SHA2(CONCAT("
    for col_name, datatype in src_columns_with_datatype.items():
        if not col_name.lower().startswith("ziw"):
            if col_name.upper() not in columns_to_select_src and col_name.upper() not in columns_to_select_tgt:
                columns_to_select.append(col_name)
                columns_to_select_src.append(f"{col_name} as {col_name}_src")
                columns_to_select_tgt.append(f"{col_name} as {col_name}_tgt")
            col_chksum_table_columns.append(f"chksum_{col_name}_result")

        ROW_HASH = ROW_HASH + f"CAST({col_name}   AS  STRING)" + ","

    ROW_HASH = ROW_HASH.strip(",") + "))"

    if len(columns_to_select_inner) == 0:
        sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT '{tgt_table}' as ENTITY_NAME, {",".join(col_chksum_table_columns)}, {",".join([i.strip() + "_SRC" for i in columns_to_select])}, {",".join([i.strip() + "_TGT" for i in columns_to_select])} , CASE WHEN ROW_HASH_SOURCE != ROW_HASH_TARGET THEN 'FAIL' ELSE 'PASS' END AS ROW_HASH_VALIDATION FROM ( SELECT * FROM ( SELECT {ROW_HASH} AS ROW_HASH_SOURCE , * FROM ( SELECT {",".join(columns_to_select_src)} FROM "{src_schema}"."{src_table}" AS SRC_TABLE INNER JOIN ( SELECT {",".join(col_chksum_table_columns)} FROM "{colchksum_table_schema}"."{colchksum_table_name}" WHERE COL_CKSUM_VALIDATION = 'FAIL' ) AS COLCHSUM_TABLE ON {src_inner_join_clause} ) AS SOURCE_BRANCH ) FULL OUTER JOIN ( SELECT {ROW_HASH} AS ROW_HASH_TARGET , * FROM ( SELECT {",".join(columns_to_select_tgt)} FROM "{tgt_schema}"."{tgt_table}" AS TGT_TABLE INNER JOIN ( SELECT {",".join(col_chksum_table_columns)} FROM "{colchksum_table_schema}"."{colchksum_table_name}" WHERE COL_CKSUM_VALIDATION = 'FAIL' ) AS COLCHSUM_TABLE ON {tgt_inner_join_clause} ) ) AS TARGET_BRANCH ON {FULL_OUTER_JOIN_CLAUSE} )"""
    else:
        sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT '{tgt_table}' as ENTITY_NAME, {",".join(col_chksum_table_columns)}, {",".join([i.strip() + "_SRC" for i in columns_to_select])}, {",".join([i.strip() + "_TGT" for i in columns_to_select])} , CASE WHEN ROW_HASH_SOURCE != ROW_HASH_TARGET THEN 'FAIL' ELSE 'PASS' END AS ROW_HASH_VALIDATION FROM ( SELECT * FROM ( SELECT {ROW_HASH} AS ROW_HASH_SOURCE , * FROM ( SELECT {",".join(columns_to_select_src)} FROM ( select * , {",".join(columns_to_select_inner)} from "{src_schema}"."{src_table}" ) AS SRC_TABLE INNER JOIN ( SELECT {",".join(col_chksum_table_columns)} FROM "{colchksum_table_schema}"."{colchksum_table_name}" WHERE COL_CKSUM_VALIDATION = 'FAIL' ) AS COLCHSUM_TABLE ON {src_inner_join_clause} ) AS SOURCE_BRANCH ) FULL OUTER JOIN ( SELECT {ROW_HASH} AS ROW_HASH_TARGET , * FROM ( SELECT {",".join(columns_to_select_tgt)} FROM ( select * , {",".join(columns_to_select_inner)} from "{tgt_schema}"."{tgt_table}" ) AS TGT_TABLE INNER JOIN ( SELECT {",".join(col_chksum_table_columns)} FROM "{colchksum_table_schema}"."{colchksum_table_name}" WHERE COL_CKSUM_VALIDATION = 'FAIL' ) AS COLCHSUM_TABLE ON {tgt_inner_join_clause} ) ) AS TARGET_BRANCH ON {FULL_OUTER_JOIN_CLAUSE} )"""

    print_info("prepared following sql for row checksum pipeline: ")
    print(sqlparse.format(sql, reindent=True, keyword_case='upper'))
    print("\n\n")
    return import_sql(sql, domain_id, pipeline_id)


def prepare_sql_import_query_datavalidation_summary(domain_id, pipeline_id, colchksum_table, rowchksum_table,
                                                    pipeline_tgt_schema_name,
                                                    pipeline_tgt_table_name,
                                                    colchksum_columns,
                                                    grp_by_cols_to_select,
                                                    entity_name):
    colchksum_table_schema, colchksum_table_name = colchksum_table.split('.')
    rowchksum_table_schema, rowchksum_table_name = rowchksum_table.split('.')
    src_columns_for_chksum = ["source_count", "target_count", "count_validation",
                              "source_distinct_count", "target_distinct_count", "duplicate_check"]
    matched_columns = ""
    mismatch_columns = ""
    for item in colchksum_columns:
        matched_columns = matched_columns + f" case when {item}_result = 'PASS' then 1 else 0 end" + "+"
        mismatch_columns = mismatch_columns + f" case when {item}_result = 'FAIL' then 1 else 0 end" + "+"

    matched_columns = matched_columns.strip("+") + " AS matched_columns"
    mismatch_columns = mismatch_columns.strip("+") + " AS mismatched_columns"
    src_columns_for_chksum.extend([matched_columns, mismatch_columns])

    grp_by_clause = ""
    for i, item in enumerate(grp_by_cols_to_select):
        grp_by_clause = grp_by_clause + f"TABLE_A.{item} = TABLE_B.{item}"
        if i < len(grp_by_cols_to_select) - 1:
            grp_by_clause = grp_by_clause + " AND "

    number_of_mismatched_cols = "case when mismatched_columns is null then 0 else mismatched_columns END AS number_of_mismatched_cols"
    number_of_mismatched_rows = "case when Number_of_mismatched_rows_temp is null then 0 else Number_of_mismatched_rows_temp END AS number_of_mismatched_rows"
    number_of_matched_cols = "case when matched_columns is null then 0 else matched_columns END AS number_of_matched_cols"
    final_columns_to_select = f"source_count as Row_Count, count_validation,duplicate_check,'{entity_name}' as Entity_Name, current_timestamp() as Execution_date, Case when Number_of_mismatched_rows_temp > 0 then 'FAIL' else 'PASS' END as Row_hash_Validation , source_count - Number_of_mismatched_rows_temp as Number_of_matched_rows, {number_of_mismatched_rows}, {number_of_matched_cols}, {number_of_mismatched_cols}"

    sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT {final_columns_to_select} FROM ( SELECT * FROM ( SELECT {','.join(src_columns_for_chksum)}, {','.join(grp_by_cols_to_select)} FROM "{colchksum_table_schema}"."{colchksum_table_name}") TABLE_A FULL OUTER JOIN ( SELECT {','.join(grp_by_cols_to_select)} , COUNT(1) AS NUMBER_OF_MISMATCHED_ROWS_TEMP FROM "{rowchksum_table_schema}"."{rowchksum_table_name}" WHERE "row_hash_validation" = 'FAIL' GROUP BY {','.join(grp_by_cols_to_select)} ) TABLE_B ON {grp_by_clause} )"""

    print_info("prepared following sql for data validation summary pipeline: ")
    print(sqlparse.format(sql, reindent=True, keyword_case='upper'))
    print("\n\n")
    return import_sql(sql, domain_id, pipeline_id)


def create_pipeline(compute_type, domain_id, pipeline_name, env_id, storage_id, compute_id):
    snowflake_warehouse = None
    if compute_type.lower() == "bigquery":
        batch_engine = "BIGQUERY"
    elif compute_type.lower() == "snowflake":
        batch_engine = "SNOWFLAKE"
        snowflake_warehouse = config.get('environment_details', 'snowflake_warehouse')
    else:
        batch_engine = "SPARK"

    pipeline_obj = {"name": str(pipeline_name), "batch_engine": batch_engine, "domain_id": str(domain_id),
                    "environment_id": str(env_id), "run_job_on_data_plane": False}
    if snowflake_warehouse is not None:
        pipeline_obj["snowflake_warehouse"] = snowflake_warehouse
    if batch_engine == "SPARK":
        pipeline_obj["ml_engine"] = "SPARK"
        pipeline_obj["storage_id"] = str(storage_id)
        pipeline_obj["compute_template_id"] = str(compute_id)
        pipeline_obj["run_job_on_data_plane"] = True
    url_for_creating_pipeline = f"{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines"
    pipeline_body = json.dumps(pipeline_obj)

    try:
        response = requests.post(url_for_creating_pipeline, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                     'Content-Type': 'application/json'},
                                 data=pipeline_body, verify=False)
        if response.status_code == 406:
            refresh_delegation_token()
            response = requests.post(url_for_creating_pipeline, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                         'Content-Type': 'application/json'},
                                     data=pipeline_body, verify=False)
        parsed_response = json.loads(response.content)
        result = parsed_response.get('result', None)
        if result:
            print_success("pipeline created successfully! "+ result['id'])
            return str(result['id'])
        else:
            print_error("Failed to create pipeline")
            pipeline_base_url = url_for_creating_pipeline
            filter_condition = json.dumps({"name": pipeline_name})
            pipeline_get_url = pipeline_base_url + f"?filter={{filter_condition}}".format(
                filter_condition=filter_condition)
            response = requests.request("GET", pipeline_get_url, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                          'Content-Type': 'application/json'},
                                        verify=False)
            if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                existing_pipeline_id = response.json().get("result", [])[0]["id"]
                return existing_pipeline_id
            elif response.status_code == 406:
                refresh_delegation_token()
                response = requests.request("GET", pipeline_get_url,
                                            headers={'Authorization': 'Bearer ' + delegation_token,
                                                     'Content-Type': 'application/json'}, verify=False)
                if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                    existing_pipeline_id = response.json().get("result", [])[0]["id"]
                    return existing_pipeline_id
    except Exception as e:
        print_error("Failed to create pipeline")
        print_error(str(e))
        return False


def modify_active_version_pipeline(domain_id, pipeline_id, tgt_properties_to_update):
    url_to_get_pl_configuration = f'{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/config-migration'
    response = requests.get(url_to_get_pl_configuration,
                            headers={'Authorization': 'Bearer ' + delegation_token, 'Content-Type': 'application/json'},
                            verify=False)
    if response.status_code == 200:
        parsed_response = json.loads(response.content)
    elif response.status_code == 406:
        refresh_delegation_token()
        response = requests.get(url_to_get_pl_configuration,
                                headers={'Authorization': 'Bearer ' + delegation_token,
                                         'Content-Type': 'application/json'},
                                verify=False)
        parsed_response = json.loads(response.content)
    else:
        parsed_response = {}

    result_configuration = parsed_response.get('result', None)
    if result_configuration is not None:
        node_keys = result_configuration["configuration"]["pipeline_configs"]["model"]["nodes"].keys()
        for item in node_keys:
            if item.startswith("TARGET"):
                tgt_properties = result_configuration["configuration"]["pipeline_configs"]["model"]["nodes"][item][
                    "properties"]
                for i in tgt_properties_to_update:
                    tgt_properties[i] = tgt_properties_to_update[i]
                tgt_properties["is_existing_dataset"] = False
                result_configuration["configuration"]["pipeline_configs"]["model"]["nodes"][item][
                    "properties"] = tgt_properties
                break
        # Post call to update the details
        import_configs = {
            "run_pipeline_metadata_build": False,
            "is_pipeline_version_active": True,
            "import_data_connection": True,
            "include_optional_properties": True
        }
        response = requests.post(url_to_get_pl_configuration,
                                 headers={'Authorization': 'Bearer ' + delegation_token,
                                          'Content-Type': 'application/json'},
                                 data=json.dumps({"configuration": result_configuration["configuration"],
                                                  "import_configs": import_configs}),
                                 verify=False)
        if response.status_code == 200:
            print_success("Pipeline re-configured")
        elif response.status_code == 406:
            refresh_delegation_token()
            response = requests.post(url_to_get_pl_configuration,
                                     headers={'Authorization': 'Bearer ' + delegation_token,
                                              'Content-Type': 'application/json'},
                                     data=json.dumps(result_configuration),
                                     verify=False)
            if response.status_code == 200:
                print_success("Pipeline re-configured")
        else:
            print_error("Pipeline reconfiguration failed: " + str(response.json()))


def check_column_name_and_datatype_validation(src_table_name, target_table_name, src_columns: dict,
                                              target_columns: dict, skip_columns_validation: list):
    column_names_mismatch = False
    column_datatype_mismatch = False
    src_columns = {k.upper(): v for k, v in src_columns.items()}
    target_columns = {k.upper(): v for k, v in target_columns.items()}
    skip_columns_validation = [i.upper() for i in skip_columns_validation]
    if [i.upper() for i in src_columns.keys() if i not in skip_columns_validation] != [j.upper() for j in
                                                                                       target_columns.keys() if
                                                                                       j not in skip_columns_validation]:
        column_names_mismatch = True
    for src_column in src_columns:
        src_datatype = src_columns[src_column]
        target_datatype = target_columns.get(src_column, "")
        if src_datatype != target_datatype:
            column_datatype_mismatch = True
            break

    result = [src_table_name, target_table_name, column_names_mismatch, column_datatype_mismatch]
    return result


def create_import_validation_pipelines(i, q):
    while True:
        item = q.get()
        row, domain_id, environment_id, storage_id, compute_id = item
        column_checksum_validation = False
        row_checksum_validation = False
        datavalidation_summary = False
        try:
            compute_type = row.get("compute_type", "spark")
            validation_type = row.get("validation_type", "all")
            pipeline_suffix = row.get("pipeline_suffix", "iwx_data_validation")
            source_table_name = row["source_table_name"]
            target_table_name = row["target_table_name"]
            source_name = row['source_name']
            target_name = row['target_name']
            group_by_cols_for_checksum = row['group_by_cols_for_checksum'].split(",") if row['group_by_cols_for_checksum'] != "" and row['group_by_cols_for_checksum'] is not None else []
            agg_cols_for_checksum = row['agg_cols_for_checksum'].split(",") if row['agg_cols_for_checksum'] != "" and row['agg_cols_for_checksum'] is not None else []
            distinct_count_cols = row['distinct_count_cols'].split(",") if row['distinct_count_cols'] != "" and row['distinct_count_cols'] is not None else []
            join_cols_row_checksum = row['join_cols_row_checksum'].split(",") if row['join_cols_row_checksum'] != "" and row['join_cols_row_checksum'] is not None else []
            pipeline_tgt_schema_name = row['pipeline_target_schema_name']
            natural_keys = row['natural_keys'].split(",") if row['natural_keys'] != "" and row['natural_keys'] is not None else []
            regex_exp = row["regex_exp"] if row["regex_exp"] != "" else None
            regex_replace = row["regex_replace"] if row["regex_replace"] != "" else "False"
            if regex_replace.lower() not in ["true", "false"]:
                regex_replace = "False"
            regex_replace = eval(regex_replace.title())
            data_profiling = row["data_profiling"] if row["data_profiling"] != "" else "True"
            if data_profiling.lower() not in ["true", "false"]:
                data_profiling = "False"
            skip_column_name_and_datatype_validation = row["skip_column_name_and_datatype_validation"] if row["skip_column_name_and_datatype_validation"] != "" else "True"
            if skip_column_name_and_datatype_validation.lower() not in ["true", "false"]:
                skip_column_name_and_datatype_validation = eval(skip_column_name_and_datatype_validation.title())
            remove_ascii_from_cols = row["remove_ascii_from_cols"].split(",") if row["remove_ascii_from_cols"] != "" and row['remove_ascii_from_cols'] is not None else None

            tgt_columns_with_datatype = get_all_columns(source_name, target_table_name)
            src_columns_with_datatype = get_all_columns(target_name, source_table_name)

            ignore_columns_from_validation = [i.upper() for i in row['ignore_columns_from_validation'].split(",")] if \
                row['ignore_columns_from_validation'] != "" and row[
                    'ignore_columns_from_validation'] is not None else []
            src_columns_with_datatype = {k: v for k, v in src_columns_with_datatype.items() if
                                         k.upper() not in ignore_columns_from_validation}
            tgt_columns_with_datatype = {k: v for k, v in tgt_columns_with_datatype.items() if
                                         k.upper() not in ignore_columns_from_validation}

            result_col_status = check_column_name_and_datatype_validation(source_table_name, target_table_name,
                                                                          src_columns_with_datatype,
                                                                          tgt_columns_with_datatype, [])

            if skip_column_name_and_datatype_validation and result_col_status[-1] and result_col_status[-2]:
                print_error(
                    "Column names or datatypes between Source and Target Dataset does not match. Skipping pipeline creation")
                continue
            else:
                # Get common columns between source and target dataset
                src_columns = [i.upper() for i in src_columns_with_datatype]
                tgt_columns = [i.upper() for i in tgt_columns_with_datatype]
                common_columns = list(set(src_columns).intersection(tgt_columns))
                common_columns_with_datatype = {k: v for k, v in tgt_columns_with_datatype.items() if
                                                k.upper() in common_columns}

            if validation_type.lower() == "column_hash":
                column_checksum_validation = True
            elif validation_type.lower() == "row_hash":
                row_checksum_validation = True
            elif validation_type.lower() == "validation_summary":
                datavalidation_summary = True
            elif validation_type.lower() == "all":
                column_checksum_validation, row_checksum_validation, datavalidation_summary = True, True, True

            pipeline_id_colchksum, pipeline_id_rowhash, pipeline_id_datavalidation = None, None, None
            if column_checksum_validation:
                pipeline_id_colchksum = create_pipeline(compute_type, domain_id,
                                                        f"iwx_datavalidation_{pipeline_suffix}_column_checksum_validation",
                                                        environment_id, storage_id, compute_id)
            if row_checksum_validation:
                pipeline_id_rowhash = create_pipeline(compute_type, domain_id,
                                                      f"iwx_datavalidation_{pipeline_suffix}_row_hash_validation",
                                                      environment_id, storage_id, compute_id)
            if datavalidation_summary:
                pipeline_id_datavalidation = create_pipeline(compute_type, domain_id,
                                                             f"iwx_datavalidation_{pipeline_suffix}_datavalidation_summary",
                                                             environment_id, storage_id, compute_id)

            if column_checksum_validation and pipeline_id_colchksum is not None:
                pipeline_tgt_table_name = f"{pipeline_suffix.upper()}_COLUMN_CHECKSUM_VALIDATION_RESULTS"
                result, col_chksum_select_cols, grp_by_cols = prepare_sql_import_query_columnchksum(compute_type,
                                                                                                    domain_id,
                                                                                                    pipeline_id_colchksum,
                                                                                                    distinct_count_cols,
                                                                                                    agg_cols_for_checksum,
                                                                                                    group_by_cols_for_checksum,
                                                                                                    source_table_name,
                                                                                                    target_table_name,
                                                                                                    pipeline_tgt_schema_name,
                                                                                                    pipeline_tgt_table_name,
                                                                                                    regex_replace=regex_replace,
                                                                                                    regex_exp=regex_exp,
                                                                                                    remove_ascii_from_cols=remove_ascii_from_cols,
                                                                                                    src_columns_with_datatype=common_columns_with_datatype,
                                                                                                    data_profiling=data_profiling)
                if result:
                    trigger_pipeline_metadata_build(domain_id, pipeline_id_colchksum)

            if row_checksum_validation and pipeline_id_rowhash is not None:
                pipeline_tgt_table_name = f"{pipeline_suffix.upper()}_ROW_CHECKSUM_VALIDATION_RESULTS"
                colchksum_table = f"{pipeline_tgt_schema_name}.{pipeline_suffix.upper()}_COLUMN_CHECKSUM_VALIDATION_RESULTS"
                inner_join_cols = group_by_cols_for_checksum
                result = prepare_sql_import_query_rowhashvalidation(compute_type, domain_id, pipeline_id_rowhash,
                                                                    source_table_name,
                                                                    target_table_name,
                                                                    colchksum_table,
                                                                    pipeline_tgt_schema_name,
                                                                    pipeline_tgt_table_name, inner_join_cols,
                                                                    join_cols_row_checksum,
                                                                    src_columns_with_datatype=common_columns_with_datatype)
                if result:
                    trigger_pipeline_metadata_build(domain_id, pipeline_id_rowhash)

            if datavalidation_summary and pipeline_id_datavalidation is not None:
                prepare_sql_import_query_datavalidation_summary(domain_id, pipeline_id_datavalidation,
                                                                f"{pipeline_tgt_schema_name}.{pipeline_suffix.upper()}_COLUMN_CHECKSUM_VALIDATION_RESULTS",
                                                                f"{pipeline_tgt_schema_name}.{pipeline_suffix.upper()}_ROW_CHECKSUM_VALIDATION_RESULTS",
                                                                pipeline_tgt_schema_name,
                                                                f"{pipeline_suffix.upper()}_DATAVALIDATION_SUMMARY_RESULTS",
                                                                col_chksum_select_cols,
                                                                grp_by_cols,
                                                                target_table_name.split(".")[-1])

            if compute_type.lower() in ["bigquery", "snowflake"]:
                if pipeline_id_colchksum is not None:
                    tgt_properties_to_update = {"build_mode": "OVERWRITE",
                                                "natural_keys": natural_keys}
                    modify_active_version_pipeline(domain_id, pipeline_id_colchksum, tgt_properties_to_update)
                if pipeline_id_rowhash is not None:
                    tgt_properties_to_update = {"build_mode": "OVERWRITE",
                                                "natural_keys": natural_keys}
                    modify_active_version_pipeline(domain_id, pipeline_id_rowhash, tgt_properties_to_update)
                if pipeline_id_datavalidation is not None:
                    tgt_properties_to_update = {"build_mode": "APPEND",
                                                "natural_keys": natural_keys}
                    modify_active_version_pipeline(domain_id, pipeline_id_datavalidation, tgt_properties_to_update)

            elif compute_type.lower() == "spark":
                if pipeline_id_colchksum is not None and pipeline_id_rowhash is not None and pipeline_id_datavalidation is not None:
                    if pipeline_id_colchksum is not None:
                        tgt_properties_to_update = {"sync_type": "OVERWRITE",
                                                    "target_base_path": f"/{pipeline_tgt_schema_name}/{pipeline_suffix}_column_hash_validation",
                                                    "storage_format": "orc",
                                                    "natural_key": natural_keys}
                        modify_active_version_pipeline(domain_id, pipeline_id_colchksum, tgt_properties_to_update)
                    if pipeline_id_rowhash is not None:
                        tgt_properties_to_update = {"sync_type": "OVERWRITE",
                                                    "target_base_path": f"/{pipeline_tgt_schema_name}/{pipeline_suffix}_row_hash_validation",
                                                    "storage_format": "orc",
                                                    "natural_key": natural_keys}
                        modify_active_version_pipeline(domain_id, pipeline_id_rowhash, tgt_properties_to_update)
                    if pipeline_id_datavalidation is not None:
                        tgt_properties_to_update = {"sync_type": "APPEND",
                                                    "target_base_path": f"/{pipeline_tgt_schema_name}/{pipeline_suffix}_datavalidation_summary",
                                                    "storage_format": "orc",
                                                    "natural_key": natural_keys}
                        modify_active_version_pipeline(domain_id, pipeline_id_datavalidation, tgt_properties_to_update)
            else:
                pass
        except Exception as e:
            print_error("Script failed for " + str(row) + str(e))
        finally:
            q.task_done()


@click.command()
@click.option('--validation_type', default='all',
              type=click.Choice(['column_hash', 'row_hash', 'validation_summary', 'all']),
              help='Pass the name of validation. Pass all to create all 3 pipelines per table')
@click.option('--source_table_name', type=str, help='Pass the fully qualified name of Source Table')
@click.option('--target_table_name', type=str, help='Pass the fully qualified name of Target Table')
@click.option('--source_name', type=str,
              help='Pass the name of the source in Infoworks in which source tables are present')
@click.option('--target_name', type=str,
              help='Pass the name of the source in Infoworks in which target tables are present')
@click.option('--compute_type', type=click.Choice(['spark', 'bigquery', 'snowflake']),
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
@click.option('--regex_replace', default="false", type=click.Choice(["true", "false"]),
              help='true/false. Variable to enable/disable removing of ascii characters from string columns')
@click.option('--data_profiling', default="true", type=click.Choice(["true", "false"]),
              help='true/false. If set to true, data profiling columns will be enabled in the column checksum pipeline')
@click.option('--natural_keys', type=str,
              help='Comma separated list of columns to be configured as natural keys')
@click.option('--pipeline_target_schema_name', type=str,
              help='Pass the target schema name in which pipeline targets are to be created')
@click.option('--domain_name', type=str, help='Pass the domain name in which pipeline has to be created')
@click.option('--environment_name', type=str,
              help='Pass the environment name in which pipeline has to be created')
@click.option('--storage_name', type=str,
              help='Pass the storage name the pipeline has to use')
@click.option('--compute_name', type=str, help='Pass the compute name the pipeline has to use')
@click.option('--read_params_from_parameter_file', type=str, help='Pass the fully qualified path of the parameter file')
@click.option('--pipeline_suffix', type=str, default="iwx_datavalidation",
              help='Pass the suffix name with which the pipeline names has to be created')
@click.option('--ignore_columns_from_validation', type=str,
              help='Pass the comma seperated list of columns to ignore during validation')
@click.option('--skip_column_name_and_datatype_validation', default="true", type=click.Choice(["true", "false"]),
              help='true/false. If set to true, column name and datatype validation is skipped and pipelines will be created no matter what')
def main(pipeline_suffix, validation_type, source_table_name, target_table_name, source_name, target_name, compute_type,
         ignore_columns_from_validation, skip_column_name_and_datatype_validation,
         group_by_cols_for_checksum,
         agg_cols_for_checksum, distinct_count_cols, join_cols_row_checksum, regex_exp, remove_ascii_from_cols,
         regex_replace, data_profiling, natural_keys, pipeline_target_schema_name, domain_name, environment_name,
         storage_name, compute_name, read_params_from_parameter_file):
    if read_params_from_parameter_file is None:
        # Check if necessary inputs are passed
        if all(item is not None for item in
               [source_table_name, target_table_name, source_name, target_name, compute_type,
                pipeline_target_schema_name]):
            print_success("All the necessary inputs are passed")
            if validation_type == "column_hash" and group_by_cols_for_checksum is None:
                print_error("Please pass group_by_cols_for_checksum input")
                sys.exit(0)
            if validation_type == "row_hash" and (group_by_cols_for_checksum is None or join_cols_row_checksum is None):
                print_error("Please pass both group_by_cols_for_checksum and join_cols_row_checksum inputs")
                sys.exit(0)
        else:
            print_error(
                "Missing inputs. Please pass all of these mandatory inputs: source_table_name,target_table_name,source_name,target_name,compute_type,group_by_cols_for_checksum,pipeline_target_schema_name inputs")
            sys.exit(0)

    global refresh_token
    refresh_token = config.get('environment_details', 'refresh_token')
    refresh_delegation_token()
    if domain_name is None:
        # Read from config.ini
        domain_id = config.get('environment_details', 'domain_id')
    else:
        # Get the domain id
        domain_id = get_domain_id_from_name(domain_name)

    if environment_name is None:
        # Read from config.ini
        environment_id = config.get('environment_details', 'environment_id')
    else:
        # Get the domain id
        environment_id = get_environment_id_from_name(environment_name)

    if storage_name is None:
        # Read from config.ini
        storage_id = config.get('environment_details', 'storage_id')
    else:
        # Get the domain id
        storage_id = get_storage_id_from_name(environment_id, storage_name)

    if compute_name is None:
        # Read from config.ini
        compute_id = config.get('environment_details', 'compute_id')
    else:
        # Get the domain id
        compute_id = get_compute_id_from_name(environment_id, compute_name)

    for i in range(num_fetch_threads):
        worker = Thread(target=create_import_validation_pipelines, args=(i, job_queue,))
        worker.setDaemon(True)
        worker.start()

    if read_params_from_parameter_file is not None:
        input_file = csv.DictReader(open(read_params_from_parameter_file))
        for row in input_file:
            job_queue.put((row, domain_id, environment_id, storage_id, compute_id))
    else:
        row = {"pipeline_suffix": pipeline_suffix, "validation_type": validation_type,
               "source_table_name": source_table_name,
               "target_table_name": target_table_name, "source_name": source_name, "target_name": target_name,
               "agg_cols_for_checksum": agg_cols_for_checksum,
               "group_by_cols_for_checksum": group_by_cols_for_checksum, "distinct_count_cols": distinct_count_cols,
               "join_cols_row_checksum": join_cols_row_checksum, "regex_exp": regex_exp,
               "remove_ascii_from_cols": remove_ascii_from_cols,
               "regex_replace": regex_replace, "data_profiling": data_profiling, "natural_keys": natural_keys,
               "pipeline_target_schema_name": pipeline_target_schema_name, "compute_type": compute_type,
               "ignore_columns_from_validation": ignore_columns_from_validation,
               "skip_column_name_and_datatype_validation": skip_column_name_and_datatype_validation}
        job_queue.put((row, domain_id, environment_id, storage_id, compute_id))

    print('*** Main thread waiting ***')
    job_queue.join()
    print('*** Done ***')


def get_query_params_string_from_dict(params=None):
    if not params:
        return ""
    string = "?"
    string = string + "&limit=" + str(params.get('limit')) if params.get('limit') else string
    string = string + "&sort_by=" + str(params.get('sort_by')) if params.get('sort_by') else string
    string = string + "&order_by=" + str(params.get('order_by')) if params.get('order_by') else string
    string = string + "&offset=" + str(params.get('offset')) if params.get('offset') else string
    string = string + "&filter=" + urllib.parse.quote(json.dumps(params.get('filter'))) if params.get(
        'filter') else string
    string = string + "&projections=" + urllib.parse.quote(json.dumps(params.get('projections'))) if params.get(
        'projections') else string
    return string


def get_domain_id_from_name(domain_name):
    """
    Function to get domain id using domain name
    :param domain_name: Name of the domain
    :return: Entity identifier of the domain
    """
    params = {"filter": {"name": domain_name}}
    url_to_list_domains = '{proxy_host}:{proxy_port}/v3/domains'.format(proxy_host=proxy_host,
                                                                        proxy_port=proxy_port) + get_query_params_string_from_dict(
        params=params)
    try:
        response = requests.request("GET", url_to_list_domains, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                         'Content-Type': 'application/json'},
                                    verify=False)
        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
            result = response.json().get("result", [])
            domain_id = result[0]["id"]
            return domain_id
    except Exception as e:
        print_error(f"Error in finding domain id for {domain_name}" + str(e))
        return None


def get_environment_id_from_name(environment_name):
    params = {"filter": {"name": environment_name}}
    url_to_list_env = '{proxy_host}:{proxy_port}/v3/admin/environment'.format(proxy_host=proxy_host,
                                                                              proxy_port=proxy_port) + get_query_params_string_from_dict(
        params=params)
    try:
        response = requests.request("GET", url_to_list_env, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                     'Content-Type': 'application/json'},
                                    verify=False)
        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
            result = response.json().get("result", [])
            environment_id = result[0]["id"]
            return environment_id
    except Exception as e:
        print_error(f"Error in finding environment id for {environment_name}" + str(e))
        return None


def get_environment_type_from_id(environment_id):
    url_to_list_env = f'{proxy_host}:{proxy_port}/v3/admin/environment/{environment_id}'.format(proxy_host=proxy_host,
                                                                                                proxy_port=proxy_port,
                                                                                                environment_id=environment_id)
    try:
        response = requests.request("GET", url_to_list_env, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                     'Content-Type': 'application/json'},
                                    verify=False)
        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
            result = response.json().get("result", {})
            compute_engine = result["compute_engine"]
            data_warehouse_type = result.get("data_warehouse_type", None)
            if data_warehouse_type is not None:
                return data_warehouse_type
            else:
                return compute_engine
    except Exception as e:
        print_error(f"Error in finding compute engine for for {environment_id}" + str(e))
        return None


def get_storage_id_from_name(environment_id, storage_name):
    params = {"filter": {"name": storage_name}}
    url_to_list_storage = '{proxy_host}:{proxy_port}/v3/admin/environment/{environment_id}/environment-storage'.format(
        proxy_host=proxy_host,
        proxy_port=proxy_port, environment_id=environment_id) + get_query_params_string_from_dict(
        params=params)
    try:
        response = requests.request("GET", url_to_list_storage, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                         'Content-Type': 'application/json'},
                                    verify=False)
        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
            result = response.json().get("result", [])
            storage_id = result[0]["id"]
            return storage_id
    except Exception as e:
        print_error(f"Error in finding storage id for {storage_name}" + str(e))
        return None


def get_compute_id_from_name(environment_id, compute_name):
    params = {"filter": {"name": compute_name}}
    url_to_list_interactive_compute = '{proxy_host}:{proxy_port}/v3/admin/environment/{environment_id}/environment-interactive-clusters'.format(
        proxy_host=proxy_host,
        proxy_port=proxy_port, environment_id=environment_id) + get_query_params_string_from_dict(
        params=params)
    url_to_list_compute_template = '{proxy_host}:{proxy_port}/v3/admin/environment/{environment_id}/environment-compute-template'.format(
        proxy_host=proxy_host,
        proxy_port=proxy_port, environment_id=environment_id) + get_query_params_string_from_dict(
        params=params)
    try:
        response_interactive = requests.request("GET", url_to_list_interactive_compute,
                                                headers={'Authorization': 'Bearer ' + delegation_token,
                                                         'Content-Type': 'application/json'},
                                                verify=False)
        response_compute = requests.request("GET", url_to_list_compute_template,
                                            headers={'Authorization': 'Bearer ' + delegation_token,
                                                     'Content-Type': 'application/json'},
                                            verify=False)
        if response_interactive.status_code == 200 and len(response_interactive.json().get("result", [])) > 0:
            result = response_interactive.json().get("result", [])
            compute_id = result[0]["id"]
            return compute_id
        elif response_compute.status_code == 200 and len(response_compute.json().get("result", [])) > 0:
            result = response_compute.json().get("result", [])
            compute_id = result[0]["id"]
            return compute_id
        else:
            print_error(f"Error in finding compute id for {compute_name}")
            return None
    except Exception as e:
        print_error(f"Error in finding compute id for {compute_name}" + str(e))
        return None


if __name__ == '__main__':
    main()
