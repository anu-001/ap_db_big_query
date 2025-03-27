import pandas as pd
import sys
from sqlalchemy import create_engine
import argparse

def export_to_parquet(database, table, user, password, host='localhost', port=3306):
    try:
        # Create output filename based on table name
        output_file = f"{table}.parquet"
        
        # Create SQLAlchemy engine
        print(f"Connecting to MySQL database '{database}'...")
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        
        # Query the table
        print(f"Fetching data from '{table}' table...")
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, engine)
        
        # Check if we got data
        if len(df) == 0:
            print(f"Warning: No data found in the '{table}' table.")
            return
            
        # Write to Parquet
        print(f"Writing {len(df)} records to {output_file}...")
        df.to_parquet(output_file)
        
        print(f"Export complete! Data saved to {output_file}")
        print(f"Data shape: {df.shape[0]} rows × {df.shape[1]} columns")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Export MySQL table to Parquet format')
    parser.add_argument('database', help='MySQL database name')
    parser.add_argument('table', help='MySQL table name to export')
    parser.add_argument('-u', '--user', default='root', help='MySQL username (default: root)')
    parser.add_argument('-p', '--password', default='', help='MySQL password')
    parser.add_argument('--host', default='localhost', help='MySQL host (default: localhost)')
    parser.add_argument('--port', type=int, default=3306, help='MySQL port (default: 3306)')
    
    args = parser.parse_args()
    
    # If no password is provided but user is root, use the default password
    if args.password == '' and args.user == 'root':
        args.password = 'password_here'
    
    export_to_parquet(
        database=args.database,
        table=args.table,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )

if __name__ == "__main__":
    main()