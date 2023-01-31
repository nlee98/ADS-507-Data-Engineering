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
2. The pipeline attempts to connect to the user's local MySQL server, prompting the user to enter their  MySQL username and password.
