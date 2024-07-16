import sqlite3
import pandas as pd
import os

dish_columns = ['id', 'name', 'description', 'menus_appeared', 'times_appeared', 'first_appeared', 'last_appeared', 'lowest_price', 'highest_price']
menu_columns = ['id', 'name', 'sponsor', 'event', 'venue', 'place', 'physical_description', 'occasion', 'notes', 'call_number', 'keywords', 'language', 'date', 'location', 'location_type', 'currency', 'currency_symbol', 'status', 'page_count', 'dish_count']
menuitem_columns = ['id', 'menu_page_id', 'price', 'high_price', 'dish_id', 'created_at', 'updated_at', 'xpos', 'ypos']
menupage_columns = ['id', 'menu_id', 'page_number', 'image_id', 'full_height', 'full_width', 'uuid']

def create_db():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
    # Directory containing the CSV files
    csv_directory = 'NYPL-menus'
    
    # Iterate over each CSV file in the directory
    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            # Read the CSV file into a DataFrame
            csv_path = os.path.join(csv_directory, filename)
            df = pd.read_csv(csv_path)
            
            # Create a table name based on the CSV file name (without extension)
            table_name = os.path.splitext(filename)[0]
            
            # Write the DataFrame to the SQLite database
            df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    conn.close()

def usecase_query():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
    # Menus that contain all top 5 dishes by menus_appeared in Dish table
    query = """
    SELECT m.*
    FROM Menu m
    JOIN MenuPage mp ON m.id = mp.menu_id
    JOIN MenuItem mi ON mp.id = mi.menu_page_id
    JOIN Dish d ON mi.dish_id = d.id
    WHERE d.id IN (
        SELECT id
        FROM Dish
        ORDER BY menus_appeared DESC
        LIMIT 5
    ) AND m.status = 'complete'
    GROUP BY m.id
    HAVING COUNT(DISTINCT d.id) = 5;
    """
    result_df = pd.read_sql_query(query, conn)
    conn.close()
    return result_df

print(usecase_query())
