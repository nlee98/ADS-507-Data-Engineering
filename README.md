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
2. The following transformation are applied to the invoices dataframe:
 * White spaces in column names with multiple words are replaced with underscores (for consistency and to prevent syntax issues).
 * The Date and Date_of_Meal fields are converted to Datetime datatypes, where the additional timezone information ("+00:00:00") is dropped to standardize all times to UTC timezone.
 * The hour is extracted from Date_of_Meal and mapped to a part of the day (i.e. Early Morning, Late Morning, Early Afternoon, etc.), and a new field ("Part_of_Day") is created.
3. A new dataframe representing each unique customer is created using the Participants column from of the invoices dataframe. The row index plus one acts as a unique customer identification number.
4. Another dataframe (customer_order) is created to link every order_id to the participating customer_id(s).
5. The pipeline attempts to connect to the user's local MySQL server, prompting the user to enter their  MySQL username and password.
