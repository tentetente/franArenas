# This is a sample Python script.

from pyspark.sql import SparkSession
import json
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import pandasql

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    spark = SparkSession.builder.master("local[*]").getOrCreate()


    try:
        with open('storage.googleapis.com_xcc-de-assessment_events.json', 'r') as json_file:
            data = json.load(json_file)

    #    for person in data:
    #        print(person['name'], person['age'])

    except FileNotFoundError:
        print("File not found.")
    except json.JSONDecodeError:
        print("Invalid JSON data.")


    #
    # flattened the data
    # Flatten the nested data
    flattened_data = []
    for item in data:
        flattened_item = {
            'id': item['id'],
            'type': item['type'],
            # 'user-agent': item['event']['user-agent'],
            # 'ip': item['event']['ip'],
            'customer-id': item['event']['customer-id'],
            'timestamp': item['event']['timestamp']
            # 'page': item['event']['page']
        }
        flattened_data.append(flattened_item)

    # Convert the flattened data to a DataFrame
    df2 = pd.DataFrame(flattened_data)

    #not null
    filtered_df = df2[df2['customer-id'].notna()]

    print("filtered_df")
    print(filtered_df)

    #sort timestamp to reckon time_diff later between visits
    sortedDFbyCustomer = filtered_df.sort_values(by=['customer-id', 'timestamp'], ascending=True)

    sortedDFbyCustomer['timestamp'] = pd.to_datetime(sortedDFbyCustomer['timestamp'])

    #calculate time_diff between visits
    sortedDFbyCustomer['time_diff'] = sortedDFbyCustomer.groupby('customer-id')[
        'timestamp'].diff().dt.total_seconds().fillna(0)


    ############################################################
    # Create a session column based on 'customer-id' and 'time_diff'
    sessions = []
    current_session = 0
    session_time_diff = 0
    previous_customer = 0
    cumulative_sum_dif = 0

    for index, row in sortedDFbyCustomer.iterrows():
        if row["customer-id"] != previous_customer:
            current_session += 1
            cumulative_sum_dif = 0

        if row["customer-id"] == previous_customer:

            if row['time_diff'] < 3600:
                cumulative_sum_dif += row['time_diff']

                if cumulative_sum_dif > 3600:
                    current_session += 1
                    cumulative_sum_dif = 0
                # if cumulative_sum_dif < 3600 then
            else:  # time_diff is bigger than 3600 with previous row
                current_session += 1
                cumulative_sum_dif = 0

        sessions.append(current_session)

        previous_customer = row["customer-id"]

    sortedDFbyCustomer['session'] = sessions

    ###############################################################
    ###############################################################
    print_hi('Before csv')
    csv_filename = 'data.csv'
    sortedDFbyCustomer.to_csv(csv_filename, index=False)



    # Set display options to show all rows and columns
    """
    # Connect to the PostgreSQL database
    db_params = {
        'database': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': '5432'
    }

    connection = psycopg2.connect(**db_params)

    # Create a SQLAlchemy engine
    engine = create_engine('postgresql://postgres:mysecretpassword@localhost:5432/postgres')

    # Save the DataFrame to the database
    sortedDFbyCustomer.to_sql('table_name', con=engine, if_exists='replace', index=False)

    # Close the database connection
    connection.close()
    """

    ###############################################################
    ###############################################################
    """
    STEP 2
    """
    ###############################################################
    ###############################################################

    sortedDFbyCustomer = sortedDFbyCustomer.rename(columns={'customer-id': 'customer_id'})

    print_hi('sortedDFbyCustomer.columns')
    print(sortedDFbyCustomer.columns)

    #we only calculate if there is a buy in one season
    sortedDFbyCustomer['there_is_buy'] = sortedDFbyCustomer['type'].apply(lambda x: x == 'placed_order')

    customers_and_session = sortedDFbyCustomer.groupby(['customer_id', 'session'])['there_is_buy'].any().reset_index()

    filtered_df = customers_and_session[customers_and_session['there_is_buy']]

    # Filter to keep only customers with at least one there_is_buy=True
    customers_with_buy = filtered_df['customer_id'].unique()

    customers_that_bought_at_least_once = customers_and_session[customers_and_session['customer_id'].isin(customers_with_buy)]


    print("sortedDFbyCustomer[customers_that_bought_at_least_once['customer_id']]")
    print(sortedDFbyCustomer[sortedDFbyCustomer['customer_id'].isin(customers_that_bought_at_least_once["customer_id"])])
    dataframe_step1_filtered_customers_bought_once = sortedDFbyCustomer[sortedDFbyCustomer['customer_id'].isin(customers_that_bought_at_least_once["customer_id"])]

    query = """SELECT customer_id, 
             AVG(sesions_until_buy) AS average_of_averages,
             AVG(sum_time_diff_per_counter_until_buy) AS sum_time_diff_per_counter 
             FROM (
             SELECT
             subquery.customer_id,
             subquery.counter,
             COUNT(subquery.counter) AS sesions_until_buy,
		     SUM(subquery.time_diff) AS sum_time_diff_per_counter_until_buy 
             FROM (
             SELECT
             dataframe_step1_filtered_customers_bought_once.*,
             1 + SUM(there_is_buy) OVER (PARTITION BY customer_id ORDER BY session) - there_is_buy AS counter
             FROM dataframe_step1_filtered_customers_bought_once) subquery
             GROUP BY subquery.customer_id, subquery.counter) subquery
             GROUP BY customer_id  """

    average_of_averages = pandasql.sqldf(query)

    print("average_of_averages")
    print(average_of_averages)

    ###############################################################

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/



