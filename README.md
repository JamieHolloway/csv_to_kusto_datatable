# csv_to_kusto_datatable
load csv file into kusto datatable



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