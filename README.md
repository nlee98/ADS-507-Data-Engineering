# ADS-507-Data-Engineering

## Repository Contents:
_Jupyter Notebook_
* [Jupyter Notebook](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/ADS-507_FinalProject.ipynb) (for running the pipeline - extracting and loading raw CSV files to a MySQL server)

 _Raw CSV Files:_
* [Invoices.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/Invoices.csv)
* [OrderLeads.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/OrderLeads.csv)
* [SalesTeam.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/SalesTeam.csv) 

## Pipeline Processes:
1. The raw CSV files ([Invoices.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/Invoices.csv), [OrderLeads.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/OrderLeads.csv), and [SalesTeam.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/SalesTeam.csv)) are loaded into a Python Jupyter Notebook and converted to Pandas dataframe objects.
2. General pre-processing steps are taken.
 * Null values are assessed - in the current state, there are no missing values
 * Data types for each column are evaluated - most are imported as strings or integers
3. The following transformation are applied to the dataframes:
 * White spaces in column names with multiple words are replaced with underscores (for consistency and to prevent syntax issues).
 * To the invoices dataframe:
   * The Date and Date_of_Meal fields are converted to Datetime datatypes
   * Additional timezone information ("+00:00:00") is dropped to standardize all times to UTC timezone.
   * The hour is extracted from Date_of_Meal and mapped to a part of the day (i.e. Early Morning, Late Morning, Early Afternoon, etc.), and a new field ("Part_of_Day") is created.
  * The number of participants is derived from the Participants column and added as a new column
  * A new dataframe dataframe (customer_order) is created to link every order_id to the participating customer_id(s).
   * A last_updated column is added to represent the date at which the csv file was last imported
 * To the orders dataframe:
   * The date column is formatted to a date datatype
5. The pipeline attempts to connect to the user's local MySQL server, prompting the user to enter their  MySQL username and password.
 _Note:_ The pipeline assumes that the server is "localhost" and that the port number is 3306.
6. The database is created via a try-except command, where the script first tries to drop the database to create it again from scratch; if the database cannot be dropped because it does not already exist, the script will create the database.
7. Four tables are created for each of the dataframes (invoice, orders, saleslead, and customer_order)
 * For scalability reasons, the invoice table is partitioned by year
8. The dataframes are loaded into the MySQL tables via a for loop executing INSERT INTO statements for each row of the dataframes
9. Transformations:
 * Customer_stats View: a view of customer statistics is given (columns: customer_name, total_spent, avg_spent)
