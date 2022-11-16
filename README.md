# InfoworksDataValidator

Infoworks DataValidator is a tool that helps in creating pipelines to validate 2 tables by computing the column checksum, row checksum and create a final data validation summary report.
<br>
This solution follows a layered approach to validation and creates pipelines using Infoworks Rest APIs.
<br>

Infoworks Data Validator, provides seamless integration, automated, scalable and repeatable solution for data validation across different environments. 
<br>

The adapter uses the Infoworks platform to connect to a large number of data sources and automatically validates data integrity at every step of the data migration process.
<br>

Once the Data Validation pipelines are created, these processes can then be orchestrated in Infoworks to deliver the automated solution with notification.

![image description](architecture.png)
<br>

# Steps to install and use infoworks validator tool:
1) Create a virtual environment
```shell
python3 -m venv env 
source ./env/bin/activate
```
2) ``` pip install -e . ```

3) Start using the tool

**Example:** <br>
iwx-datavalidation-tool --source_table_name healthcare_dataproc.healthcare_member_source --target_table_name healthcare.helathcare_member_target --source_name Big_query_Sync --target_name Big_query_Sync --compute_type bigquery --group_by_cols_for_checksum "extract(year from REGISTRATION_DATE)" --join_cols_row_checksum member_id --pipeline_target_schema_name accenture_data_validation --pipeline_suffix healthcare_member


