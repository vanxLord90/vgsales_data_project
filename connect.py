import pandas as pd
from config import config
import psycopg2

def connect():
    """Connect to PostgreSQL database using psycopg2"""
    conn = None
    cur = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        data_to_load = pd.read_csv('//home//vishnud6//vgsales_project//platform.csv')

        # Create the table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS platform_specific_data (
                "ID" SERIAL PRIMARY KEY,
                "Platform" TEXT,
                "Total Global Sales" DOUBLE PRECISION,
                "Best Year" INTEGER,
                "Most Releases in Year" INTEGER
            )
        """)
        conn.commit()

        # Insert data into the table row by row
        for index, row in data_to_load.iterrows():
            platform = row.get('Platform', None)
            total_sales = row.get('Total Global Sales', None)
            best_year = row.get('Best Year', None)
            releases_year = row.get('Most Releases in Year', None)  # Updated column name here
            
            if platform is not None and total_sales is not None and best_year is not None and releases_year is not None:
                cur.execute("""
                    INSERT INTO platform_specific_data ("Platform", "Total Global Sales", "Best Year", "Most Releases in Year")
                    VALUES (%s, %s, %s, %s)
                """, (platform, total_sales, best_year, releases_year))
                print(f"Inserted row {index + 1} into the table")

        conn.commit()

    except psycopg2.Error as e:
        print("Error:", e)
        conn.rollback()  # Rollback changes if an error occurs

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed")

if __name__ == '__main__':
    connect()
