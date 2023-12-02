# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
import sqlalchemy as sa
import urllib

import sqlalchemy as sa
import urllib

from great_expectations.core.batch import RuntimeBatchRequest
from sqlalchemy.sql import text

from ruamel import yaml

import great_expectations as gx

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier
from great_expectations.exceptions import DataContextError
from great_expectations.core.expectation_configuration import ExpectationConfiguration

context = gx.get_context()

# params = urllib.parse.quote_plus("DRIVER={ODBC Driver 18 for SQL Server};SERVER=EPUAKHAW0118\\SQLEXPRESS;DATABASE=AdventureWorks2012;Trusted_Connection=yes;Encrypt=no")
# engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
#
# with engine.connect() as conn:
#     query = text("SELECT * FROM Person.Person")
#     result = conn.execute(query)
#     row = result.fetchone()
#     print(row)
#
datasource_config = {
    "name": "my_mssql_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "mssql+pyodbc://EPUAKHAW0118\\SQLEXPRESS/AdventureWorks2012?driver=ODBC Driver 18 for SQL Server&charset=utf&autocommit=true&Trusted_Connection=yes&Encrypt=no",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
               "Production.Product"
            ],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        },
    },
}

context.test_yaml_config(yaml.dump(datasource_config))
context.add_datasource(**datasource_config)

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    print(context.list_datasources())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
