import great_expectations as gx
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource, check_if_datasource_name_exists
import datetime
import pandas as pd
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError
from ruamel.yaml import YAML
from pprint import pprint

yaml = YAML()
context = gx.get_context()

datasource_name = "my_datasource"

connection_string = "mssql+pyodbc://EPUAKHAW0118\\SQLEXPRESS/AdventureWorks2012?driver=ODBC Driver 18 for SQL Server&charset=utf&autocommit=true&Trusted_Connection=yes&Encrypt=no"

schema_name = "Production"

table_name = "Product"

example_yaml = f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: {connection_string}
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""
print(example_yaml)

context.test_yaml_config(yaml_config=example_yaml)

sanitize_yaml_and_save_datasource(context, example_yaml, overwrite_existing=True)
context.list_datasources()

batch_request = {'datasource_name': 'my_datasource', 'data_connector_name': 'default_configured_data_connector_name', 'data_asset_name': 'Product', 'limit': 1000}

expectation_suite_name = "my_first_expectation_suite"

validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)
column_names = [f'"{column_name}"' for column_name in validator.columns()]
print(f"Columns: {', '.join(column_names)}.")
validator.head(n_rows=5, fetch_all=False)

exclude_column_names = [
    "rowguid",
    "ModifiedDate",
]
result = context.assistants.onboarding.run(
    batch_request=batch_request,
    exclude_column_names=exclude_column_names,
)
validator.expectation_suite = result.get_expectation_suite(
    expectation_suite_name=expectation_suite_name
)

print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name
        }
    ]
}
checkpoint = SimpleCheckpoint(
    f"{validator.active_batch_definition.data_asset_name}_{expectation_suite_name}",
    context,
    **checkpoint_config
)
checkpoint_result = checkpoint.run()

context.build_data_docs()

validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
context.open_data_docs(resource_identifier=validation_result_identifier)

my_checkpoint_name = "getting_started_checkpoint2"

yaml_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: Production.Product
      data_connector_query:
        index: -1
    expectation_suite_name: my_first_expectation_suite
"""
print(yaml_config)
#
# pprint(context.get_available_data_asset_names())
# context.list_expectation_suite_names()
# my_checkpoint = context.test_yaml_config(yaml_config=yaml_config)
# print(my_checkpoint.get_config(mode="yaml"))
# context.add_checkpoint(**yaml.load(yaml_config))
context.run_checkpoint(checkpoint_name=my_checkpoint_name)
context.open_data_docs()

