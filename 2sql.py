import sqlite3
import pandas as pd
import os

dish_columns = ['id', 'name', 'description', 'menus_appeared', 'times_appeared', 'first_appeared', 'last_appeared', 'lowest_price', 'highest_price']
menu_columns = ['id', 'name', 'sponsor', 'event', 'venue', 'place', 'physical_description', 'occasion', 'notes', 'call_number', 'keywords', 'language', 'date', 'location', 'location_type', 'currency', 'currency_symbol', 'status', 'page_count', 'dish_count']
menuitem_columns = ['id', 'menu_page_id', 'price', 'high_price', 'dish_id', 'created_at', 'updated_at', 'xpos', 'ypos']
menupage_columns = ['id', 'menu_id', 'page_number', 'image_id', 'full_height', 'full_width', 'uuid']

def csv2db():

    conn = sqlite3.connect('database.db')
    
    csv_directory = 'NYPL-menus'    
    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            csv_path = os.path.join(csv_directory, filename)
            df = pd.read_csv(csv_path)
            
            table_name = os.path.splitext(filename)[0]            
            df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    conn.close()

def usecase_query():

    conn = sqlite3.connect('database.db')
    
    # Menus that contain all top 5 dishes by menus_appeared in Dish table
    query = """
    SELECT DISTINCT m.*
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
    result_df.columns = menu_columns
    conn.close()
    return result_df

print(usecase_query())