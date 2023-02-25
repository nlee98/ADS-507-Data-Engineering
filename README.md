# ADS-507-Data-Engineering

## Repository Contents:
_Jupyter Notebook_
* [Jupyter Notebook](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/ADS-507_Final_Notebook.ipynb) (for running the pipeline - extracting and loading raw CSV files to a MySQL server)
_Executable Python Script_
* [Python Script](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/ADS-507_Final_Notebook.py) (a deployable infrastructure as code (IaC) version of the Jupyter Notebook - for importing the raw CSV Files, transforming the raw files, building a MySQL database and related tables, and loading the transformed data into the newly constructed tables)
 _Raw CSV Files:_
* [Invoices.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/Invoices.csv)
* [OrderLeads.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/OrderLeads.csv)
* [SalesTeam.CSV](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/SalesTeam.csv) 

## Pipeline Deployment:
1. Download either the [Jupyter Notebook](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/ADS-507_Final_Notebook.ipynb) or [Python Script](https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/ADS-507_Final_Notebook.py).
2. Execute downloaded file in your preferred python environment (Anaconda, VSCode, etc.).
  *_Note:_ The pipeline assumes that the server is "localhost" and that the port number is 3306. If the server name or port number differs, the script will need to be modified.
3. When prompted, enter your MySQL username (usually, "root") and your corresponding MySQL password.
4. Let the pipeline run to completion.
5. _TABLEAUUUUUUUUUUUUUUUUUUUUu_

## Pipeline Monitoring:
* Print statements, such as "print('Invoice Table Created!)," are included at critical points to confirm that the pipeline is functioning as expected.
* If a print statement, confirming the cells successful execution, is not printed, the pipeline will stop its execution.


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
* Views:
   * Average Meal Price: Average meal price by type of meal.
   * Average Participants: Average number of participants by meal type.
   * Company Metrics: For each company, the total amount and average amount of each invoice monthly are shown for each meal (and displaying their respective meal). In addition, the year-to-date amount collected and yearly total are presented.
   * Customer Purchases: Customer_Name, Part_of_Day, Company_Name, Number_of_Purchases, Total_Spent.
   * Customer Stats: total number of orders by each customer, total amount each customer spent, and the average amount each spent.
   * Difference Days: Difference in days between the date of meal and date the order was placed.
   * Percent Converted: Shows the number of orders for every company and the total converted (as a sum and proportion) and not converted to an order, as a sum.
   * Sales by Year: Number of invoices each year.
   * Sales Rep Performance: Sales_Rep, Sales_Rep_Id, Company_Name, Company_Id, Profit_by_Sales_Rep.
   * Total Sales: Total sales by type of meal price for each year.
