import datetime as dt
#import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re
import pymysql
from sqlalchemy import create_engine
import requests
import io
import os
import warnings
warnings.filterwarnings("ignore")

print("Wait about 5 seconds............")

# Function to Pull Raw CSV from GitHub and Convert to Pandas Dataframe Object
def github_to_pandas(raw_git_url):
    # Pull Raw CSV File from GitHub
    file_name = str(raw_git_url)
    pull_file = requests.get(file_name).content

    # Convert Raw CSV to Pandas Dataframe
    csv_df = pd.read_csv(io.StringIO(pull_file.decode('utf-8')))

    return csv_df


# Function defining hour of the day with the time of day
def time_of_day(x):
    day_hour = x.hour
    if (day_hour >= 5) and (day_hour <= 8): # 5am - 8am
        return "Early Morning"
    elif (day_hour > 8) and (day_hour <= 12): # 9am - 12pm
        return "Late Morning"
    elif (day_hour > 12) and (day_hour <= 15): # 1pm - 3pm
        return "Early Afternoon"
    elif (day_hour > 15) and (day_hour <= 19): # 4pm - 7pm
        return "Evening"
    elif (day_hour > 19) and (day_hour <= 23): # 8pm - 11pm
        return "Night"
    else: # 12am - 4am
        return "Late Night"


    # Pull CSV files from GitHub and Convert to Pandas Dataframe
invoice_df = github_to_pandas("https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/Invoices.csv")
orderleads_df = github_to_pandas("https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/OrderLeads.csv")
salesteam_df = github_to_pandas("https://raw.githubusercontent.com/nlee98/ADS-507-Data-Engineering/main/SalesTeam.csv")
print("CSV Files from GitHub Loaded into Pandas DataFrames")


# Find missing values
print("- Invoice Missing Values:\n", invoice_df.isnull().sum())
print("\n- Order Leads Missing Values:\n", orderleads_df.isnull().sum())
print("\n- Sales Team Missing Values:\n", salesteam_df.isnull().sum())
# Data types of all columns
print("- Invoice Data Types:\n", invoice_df.dtypes)
print("\n- Order Leads Data Types:\n", orderleads_df.dtypes)
print("\n- Sales Team Data Types:\n", salesteam_df.dtypes)
# Duplicated data
print("- Invoice Duplicated Values:", invoice_df.duplicated().sum())
print("- Order Leads Duplicated Values:", orderleads_df.duplicated().sum())
print("- Sales Team Duplicated Values:", salesteam_df.duplicated().sum())
    
# Replace spaces with underscores in all dataframe column names
invoice_df.columns = invoice_df.columns.str.replace(" ", "_")
orderleads_df.columns = orderleads_df.columns.str.replace(" ", "_")
salesteam_df.columns = salesteam_df.columns.str.replace(" ", "_")


#Edit CSVs
    #Edit Invoice CSV
    # Date to Date ("d-m-Y")
invoice_df["Date"] = pd.to_datetime(invoice_df["Date"], format='%d-%m-%Y')
    # Drop "+HH:MM:SS" to make all uniform to UTC timezone
invoice_df["Date_of_Meal"] = invoice_df["Date_of_Meal"].apply(lambda x: x.split("+")[0])
    # Convert Date_of_Meal to Datetime format
invoice_df["Date_of_Meal"] = pd.to_datetime(invoice_df["Date_of_Meal"],format = "%Y-%m-%d %H:%M:%S")
    # Convert Date_of_Meal to Datetime format
invoice_df["Date_of_Meal"] = pd.to_datetime(invoice_df["Date_of_Meal"],format = "%Y-%m-%d %H:%M:%S")
    # Apply time_of_day function to Date_of_Meal column
invoice_df["Part_of_Day"] = invoice_df["Date_of_Meal"].apply(time_of_day)
    # Add a field to count the number of participants
invoice_df['Number_of_Participants'] = invoice_df['Participants'].apply(lambda x: x.count("'")/2)
invoice_df['Number_of_Participants'] = invoice_df['Number_of_Participants'].astype(int)
invoice_df.head(5)


    #Create Cust_orders
    # Find all the occurrences of customer names then explode to convert values in lists to rows
cust = invoice_df['Participants'].str.findall(r"'(.*?)'").explode()
    # Join with order id 
cust_order_df = invoice_df[['Order_Id']].join(cust)
    # Factorize to encode the unique values in participants
cust_order_df['Customer_Id'] = cust_order_df['Participants'].factorize()[0] + 1
cust_order_df["Customer_Id"] = cust_order_df["Customer_Id"].astype(str)
    # Rename Participants Column
cust_order_df.columns = ["Order_Id", "Participant_Name", "Customer_Id"]
    # Add Last Updated Date
cust_order_df["Last_Updated"] = dt.date.today()
cust_order_df.head(9)
    
    
    #Edit date on Orderleads
orderleads_df["Date"] = pd.to_datetime(orderleads_df["Date"])
orderleads_df.head(3)
    
    
    #No edits to salesteam 
salesteam_df.head(3)

# Manually Login to MySQL
mysql_username = str(input("Enter MySQL Username: "))
mysql_password = str(input("Enter MySQL Password: "))

mysql_conn = pymysql.connect(
    host = "localhost",
    port = int(3306),
    user = mysql_username,
    passwd = mysql_password
)

# Create ADS-507_Supermarket MySQL Database
## Drop the database to create an updated version if it exists
try :
    mysql_conn.cursor().execute(
        """
        DROP DATABASE ADS_507_Supermarket;
        """
    )
    mysql_conn.cursor().execute(
        """
        CREATE DATABASE IF NOT EXISTS ADS_507_Supermarket;
        """
    )
# Create the database if it has not done so before
except: 
    mysql_conn.cursor().execute(
        """
        CREATE DATABASE IF NOT EXISTS ADS_507_Supermarket;
        """
    )

# Navigate to Supermarket Database
mysql_conn.select_db("ADS_507_Supermarket")

print("ADS-507 Supermarket Database Created")

create_order_table = """
    CREATE TABLE IF NOT EXISTS orders(
        Order_Id VARCHAR(100) NOT NULL,
        Company_Id VARCHAR(100) NOT NULL,
        Company_Name VARCHAR(255),
        Date DATE,
        Order_Value SMALLINT,
        Converted TINYINT UNSIGNED,
        PRIMARY KEY (Order_Id, Company_Id),
        INDEX (Order_Id),
        Index (Company_Id)
    )
;
"""

mysql_conn.cursor().execute(create_order_table);
print("Orders Table Created")

create_invoice_table = """
CREATE TABLE IF NOT EXISTS invoice (
    Order_Id VARCHAR(100) NOT NULL,
    Date DATE NOT NULL,
    Meal_Id VARCHAR(100) NOT NULL, 
    Company_Id VARCHAR(100) NOT NULL,
    Date_of_Meal DATETIME NOT NULL,
    Participants VARCHAR(255),
    Meal_Price SMALLINT,
    Type_of_Meal ENUM('Breakfast', 'Lunch', 'Dinner'),
    Part_of_Day ENUM('Early Morning', 'Late Morning', 'Early Afternoon', 'Night', 'Late Night'),
    Number_of_Participants TINYINT,
    PRIMARY KEY (Order_Id, Date),
    FOREIGN KEY (Order_Id) REFERENCES orders(Order_Id),
    FOREIGN KEY (Company_Id) REFERENCES orders(Company_Id),
    INDEX (Date),
    UNIQUE INDEX unique_order_id (Order_Id)
    )
;
"""

mysql_conn.cursor().execute(create_invoice_table);
print("Invoice Table Created")

# Foreign key added on company_id to link the salesteam to the orders table
create_salesteam_table = """
    CREATE TABLE IF NOT EXISTS salesteam(
        Sales_Rep VARCHAR(255),
        Sales_Rep_Id VARCHAR(100),
        Company_Name VARCHAR(255),
        Company_Id VARCHAR(100),
        FOREIGN KEY (Company_Id) REFERENCES orders(Company_Id),
        INDEX (Sales_Rep),
        INDEX (Company_Name)
    )
"""

mysql_conn.cursor().execute(create_salesteam_table);
print("Sales Table Created")

create_customerorder_table = """
    CREATE TABLE IF NOT EXISTS customer_order(
        Order_Id VARCHAR(100),
        Participant_Name VARCHAR(255),
        Customer_Id VARCHAR(255),
        Last_Updated DATE,
        FOREIGN KEY (Order_Id) REFERENCES orders(Order_Id),
        INDEX (Participant_Name)
    )
"""

mysql_conn.cursor().execute(create_customerorder_table);
print("Customer_Order Table Created")

# Create Engine to write to SQL table
engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
        host = "localhost", 
        db = "ADS_507_Supermarket", 
        user = mysql_username, 
        pw = mysql_password))

        # Load orders datafraome to SQL table
for i, df_row in orderleads_df.iterrows():
    row_value = """
    INSERT INTO ADS_507_Supermarket.orders VALUES (
        %s, %s, %s, %s, %s, %s)
        """
    mysql_conn.cursor().execute(row_value, tuple(df_row))

print("Successfully added data to orders table")

# Load invoice dataframe to SQL table
for i, df_row in invoice_df.iterrows():
    row_value = """
    INSERT INTO ADS_507_Supermarket.invoice VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s)
        """
    mysql_conn.cursor().execute(row_value, tuple(df_row))

print("Successfully added data to invoice table")

# Load salesteam datafraome to SQL table
for i, df_row in salesteam_df.iterrows():
    row_value = """
    INSERT INTO ADS_507_Supermarket.salesteam VALUES (
        %s, %s, %s, %s)
        """
    mysql_conn.cursor().execute(row_value, tuple(df_row))

print("Successfully added data to salesteam table")

# Load customer_order datafraome to SQL table
for i, df_row in cust_order_df.iterrows():
    row_value = """
    INSERT INTO ADS_507_Supermarket.customer_order VALUES (
        %s, %s, %s, %s)
        """
    mysql_conn.cursor().execute(row_value, tuple(df_row))

print("Successfully added data to customer_order table")

customer_stats = """
    CREATE VIEW customer_stats
        (customer_name, number_of_order, total_spent, average_spent)
        AS
        SELECT co.Participant_Name , COUNT(*), SUM(i.Meal_Price), AVG(i.Meal_Price)
        FROM customer_order AS co
            INNER JOIN invoice AS i
                ON co.Order_Id = i.Order_Id
        GROUP BY co.Participant_Name;
"""

mysql_conn.cursor().execute(customer_stats)
print("customer_stats view created")

pd.read_sql("SELECT * FROM customer_stats LIMIT 3;", mysql_conn)

company_metrics = """
    CREATE VIEW company_metrics
        (company_name, year, month, 
        meal_typ_sale, monthly_total,
        monthly_average, number_of_sales, year_to_date, yearly_total)
        AS
        SELECT o.Company_Name, YEAR(i.Date) AS Year, MONTHNAME(i.Date) AS Month,
            i.Type_of_Meal,
            SUM(i.Meal_Price) AS Monthly_Total,
            AVG(i.Meal_Price) AS Monthly_Average,
            COUNT(*) AS Number_of_Sales,
            SUM(SUM(i.Meal_Price))
                OVER (PARTITION BY o.Company_Name, Year(i.Date)
                        ORDER BY MONTH(STR_TO_DATE(month, '%M'))
                        ROWS UNBOUNDED PRECEDING) AS Year_to_Date,
            SUM(i.Meal_Price)
                OVER (PARTITION BY o.Company_Name, YEAR(i.Date)) AS Yearly_Total
        FROM orders AS o
            INNER JOIN invoice AS i
                ON o.Order_Id = i.Order_Id
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, MONTH(STR_TO_DATE(month, '%M'))
"""

mysql_conn.cursor().execute(company_metrics)
print("company_metrics view created")

pd.read_sql("SELECT * FROM company_metrics LIMIT 3;", mysql_conn)

sales_rep_performance = """
    CREATE VIEW sales_rep_performance
        (Sales_Rep, Sales_Rep_Id, Company_Name,
        Company_Id, Profit_by_Sales_Rep,
        Min_Sale, Max_Sale)
        AS
        SELECT s.Sales_Rep AS Sales_Rep, 
            s.Sales_Rep_Id AS Sales_Rep_Id,
            s.Company_Name AS Company_Name,
            s.Company_Id AS Company_Id,
            SUM(i.Meal_Price) AS Profit_by_Sales_Rep,
            MIN(i.Meal_Price) AS Min_Sale,
            MAX(i.Meal_Price) AS Max_Sale
        FROM salesteam AS s
            INNER JOIN orders AS o
                ON s.Company_Id = o.Company_Id
            INNER JOIN invoice AS i
                ON o.Order_Id = i.Order_Id
        GROUP BY s.Sales_Rep, s.Company_Name
"""

mysql_conn.cursor().execute(sales_rep_performance)
print("sales_rep_performance view created")

pd.read_sql("SELECT * FROM sales_rep_performance LIMIT 3;", mysql_conn)

customer_purchases = """
    CREATE VIEW customer_purchases
        (Customer_Name, Meal_Type, Part_of_Day, Company_Name,
        Number_of_Purchases, Total_Spent, Avg_Spent_per_Meal)
        AS
        SELECT c.Participant_Name, i.Type_of_Meal, i.Part_of_Day,
            s.Company_Name, COUNT(i.Order_Id) AS Num_Purchases,
            SUM(i.Meal_Price) AS Meal_Price, 
            AVG(i.Meal_Price) AS Avg_Spent
        FROM invoice AS i
            INNER JOIN customer_order AS c
                ON i.Order_Id = c.Order_Id
            INNER JOIN salesteam AS s
                ON s.Company_Id = i.Company_Id
        GROUP BY 1,2
"""

mysql_conn.cursor().execute(customer_purchases)
print("customer_purchases view created")

pd.read_sql("SELECT * FROM customer_purchases;", mysql_conn)

sales_by_year = """
    CREATE VIEW sales_by_year
        AS
    SELECT YEAR(Date) AS Year, COUNT(*) AS Total_Invoices 
    FROM invoice 
    GROUP BY Year;
"""
mysql_conn.cursor().execute(sales_by_year);

pd.read_sql("SELECT * FROM sales_by_year", mysql_conn)

percent_converted = """
    CREATE VIEW percent_converted
        AS
    SELECT Company_Name, 
        COUNT(Converted) AS Converted_Total,
        SUM(IF(Converted = '1', Converted, 0)) AS Converted_to_Order,
        (COUNT(Converted) - SUM(IF(Converted = '1', Converted, 0))) AS Not_Converted,
        ROUND(((SUM(Converted)/Count(*))*100), 2) AS Percent_Converted
    FROM orders 
    GROUP BY Company_Name;
"""
mysql_conn.cursor().execute(percent_converted);

test_query = """
    SELECT *
    FROM percent_converted
    ORDER BY Percent_Converted DESC
    LIMIT 5;
"""

pd.read_sql(test_query, mysql_conn)

# avg_meal_price: Average meal price
avg_meal_price = """
    CREATE VIEW avg_meal_price 
    AS
        SELECT Type_of_Meal,
            ROUND(AVG(Meal_Price),2) as Average_Meal_Price
        FROM invoice
        GROUP BY Type_of_Meal;
    """
                        
mysql_conn.cursor().execute(avg_meal_price);

test_query = """
    SELECT * 
    FROM avg_meal_price
    ORDER BY Average_Meal_Price DESC;
    """

pd.read_sql(test_query, mysql_conn)

# total_sales: Total sales by type of meal price by year
total_sales = """
    CREATE VIEW total_sales 
    AS
        SELECT YEAR(Date) AS Year, 
            Type_of_Meal, 
            SUM(Meal_Price) as Total_Sales 
        FROM invoice
        GROUP BY Year, Type_of_Meal 
        ORDER BY Year ASC, 
        FIELD(Type_of_Meal,'Breakfast','Lunch','Dinner')
    ;
    """
                        
mysql_conn.cursor().execute(total_sales);

test_query = """SELECT * FROM total_sales;"""
pd.read_sql(test_query, mysql_conn)

# avg_participants: Average amount of participants for each type of meal
avg_participants = """
    CREATE VIEW avg_participants 
    AS
        SELECT Type_of_Meal, 
            AVG(Number_of_Participants) as Average_Participants_Per_Meal
        FROM invoice
        GROUP BY Type_of_Meal"""
                        
mysql_conn.cursor().execute(avg_participants)

test_query = """SELECT * FROM avg_participants;"""
pd.read_sql(test_query, mysql_conn)

# difference_days: Difference in days between Date_of_Meal and Date
difference_days = """
    CREATE VIEW difference_days 
    AS
        SELECT Date, 
            Date_of_Meal, 
            DATEDIFF(Date, Date_of_Meal) AS Days_Between
        FROM invoice
    ;
    """
                        
mysql_conn.cursor().execute(difference_days);

test_query = """
    SELECT * 
    FROM difference_days 
    LIMIT 2;
    """

pd.read_sql(test_query, mysql_conn)

test_query = """
    show tables;
"""

pd.read_sql(test_query, mysql_conn)

quest = str(input("Pipeline Completed! Do you want to close? type yes or no: "))
if quest == 'yes':
    quit()
else:
    print('okay! ill stay open')



#pyinstaller.exe -F --clean --icon=super.ico Supermarket.py
