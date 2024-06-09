helpdoc = '''
This Python script loads a CSV file into Kusto function/datatable so that it can be queried like a table in Kusto.
___________________________________________________________________________________________________________________

Data upload procedure:

1) The following Python packages are required --> pip install pandas azure-devops azure-kusto-data datetime
2) Save CSV file in Windows desktop folder.
3) Assign values to arguments cluster, database, and file_path.
4) Run the script.
5) Authentication is attempted with Azure CLI. If it fails, interactive login is attempted.  authenticate in browser using correct work or personal browser profile.
6) The Kusto datatable function name will be the folder {your user_id}_DataTables, and named "{CSV file_name}_autoloaded_csv_{your user_id})()".
7) To drop the datatable function in kusto, execute this command --> ".drop function {function_name}".

* Function and Column names in Kusto datatable function will be cleaned to remove special characters and spaces.
* You will need to refresh the cluster/database in the Kusto IDE to see the new datatable function.
'''

# Initialize these data fields -->
cluster: str = "https://kvc4wpkaphjsswdr6rz58b.southcentralus.kusto.windows.net"
database: str = 'Database1'
file_path: str = r'E:\OneDrive\repo\data\prefixes.csv'

#--------------------------------------------------------------------------------

import os, sys, re, time, pandas as pd
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.exceptions import KustoApiError

@dataclass
class CsvToKustoDT:
    cluster: str
    database: str
    file_path: str

    user_id: str = os.getlogin()
    kusto_folder_name: str = user_id  + '_DataTables'
    docstring: str = 'function datatable created from CSV file'   
    file_name: str = os.path.basename(file_path) 
    max_file_size_bytes: int = 64_000_000
    max_row_count: int = 100_000
    client: KustoClient = None

    try:
        client = KustoClient(KustoConnectionStringBuilder.with_az_cli_authentication(cluster))
        response = client.execute(database,"print 'verify connection'")
        print("Authenticated successfully with Azure CLI.")
    except Exception as e:
        print("Authentication with Azure CLI did not succeed. Azure CLI may not be installed and authorized.")
        try:
            print("Trying interactive login; authenticate in browser using correct work or personal browser profile.")
            client = KustoClient(KustoConnectionStringBuilder.with_interactive_login(cluster))
            response = client.execute(database,"print 'verify connection'")
            print("Authenticated successfully with interactive login.")
        except Exception as e:
            print("Authentication with interactive login did not succeed.")
            print(f"{e}")
            sys.exit(1)

    def timing_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Function '{func.__name__}' executed in {elapsed_time:.4f} seconds")
            return result
        return wrapper

    def check_file_size(self, file_path, max_file_size_bytes: int=64_000_000, max_row_count: int=100_000) -> None:
        file_size = os.path.getsize(file_path)

        if file_size > max_file_size_bytes: raise (f"File size is {file_size} bytes. It must be less than {max_file_size_bytes}.  Consider ingesting into a table, not a datatable.", '#FF0000')

        with open(file_path, 'r') as file:
            row_count = sum(1 for row in file) - 1
            if row_count > max_row_count: raise (f"Row count is {row_count}. It must be less or equal than {max_row_count}.  Consider ingesting into a table, not a datatable.", '#FF0000')

    @staticmethod
    def clean_string(column_name) -> str:
        return re.sub(r'[^\w]', '_', column_name)

    def dataframe_to_kusto_datatable(self, df) -> str:
        df = df.fillna('')
        df['TimeAutoLoadedUTC'] = datetime.now(timezone.utc).isoformat()
        column_defs: str = ", ".join([f"{self.clean_string(col)}:string" for col in df.columns])
        rows: str = ",\n".join([f"     {', '.join([repr(v) if isinstance(v, str) else str(v) for v in row])}" for row in df.itertuples(index=False)])
        command: str = f".create-or-alter function with (folder = '{self.kusto_folder_name}', docstring = '{self.docstring}', skipvalidation = 'true') {self.clean_string(os.path.splitext(self.file_name)[0])}_autoloaded_csv_{self.user_id}() " + "{ "
        command += f"""
            datatable({column_defs})
            [
            {rows}
            ]
        """
        command += "}"
        return command

    @timing_decorator
    def main(self) -> None:
        self.check_file_size(self.file_path, self.max_file_size_bytes, self.max_row_count)
        command: str = self.dataframe_to_kusto_datatable(pd.read_csv(self.file_path))

        try: 
            response = self.client.execute_mgmt(database, command)
            print("Create datatable function executed successfully ---> ")
        except KustoApiError as e: 
            print(f"{e}\nCheck syntax of Kusto command.")
        except Exception as e: 
            print(f"{e}") 
        finally:
            print('\tCommand executed -- ' + command[:300] + '...')

        return None

if __name__ == "__main__":
    CsvToKustoDT(cluster, database, file_path).main()