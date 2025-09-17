#import nedded packages
import pandas as pd
import os
from conn_configs import user,password

'''STEP 1: LOAD THE CSV'''
def load_csv():
    #get our current dir
    cur_dir = os.getcwd()
    print(f'Current dir: {cur_dir}')

    #setting the file dir and filename
    file_dir = os.path.join(cur_dir,'ETL\\origin_file')
    filename = 'Netflix_Dataset.csv'

    #cretae the reference to read the csv
    orgn_file = os.path.join(file_dir,filename)

    #reading csv
    try:
        print(f'loading file {filename} \n')
        csv_data = pd.read_csv(orgn_file, sep=';', encoding='utf-8')
        print('File loaded')
    except Exception as e:
        print(f'Error to load file: {e}')
    
    return csv_data

'''STEP 2: PROCESS DATA'''
def process_csv():
    csv_data = load_csv()

    #preview infos
    print(csv_data.head(5))
    print('Total of rows before:', len(csv_data))

    csv_filtered = csv_data.drop_duplicates()

    print('Total of rows now:', len(csv_filtered))

    print(csv_filtered.columns)
    #fill Null values
    null_columns = ['Category', 'Title', 'Director', 'Cast',
    'Country', 'Rating', 'Duration', 'Type', 'Description']

    for column in null_columns:
        csv_filtered[column] = csv_filtered[column].fillna('Not informed')

    try:
        print('Trying to format date column')
        csv_filtered['Release_Date'] = pd.to_datetime(csv_filtered['Release_Date'],format='mixed', dayfirst=False,errors='coerce')
        #csv_filtered['Release_Date'] = csv_filtered['Release_Date'].strptime('%YY-%M-%d')
        print('Formatting done')
        return csv_filtered
    except Exception as e:
        print(f"Couldn't format date: {e}")

    #csv_filtered.to_csv('Data_filtred.csv', sep=';',encoding='utf-8', index=False)
    #print(csv_filtered['Release_Date'])

def csv_to_sql():
    #just calling the function to store the csv filtered
    df = process_csv()

    #flag to mark if the sql conn was sucessful done
    status = False 

    #creating the SQL Server conection  
    from sqlalchemy import create_engine, text
    from urllib.parse import quote_plus
    server = 'localhost\\SQLEXPRESS'   
    database = 'netflix'
    driver = 'ODBC Driver 17 for SQL Server'

    engine = create_engine(
    f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes"
    )

    #testing the connection
    try:
        print(f'Conecting to {server}')
        conn = engine.connect()
        print('Conected!')
        status = True
    except Exception as e:
        print(f"Couldn't conect: {e}")
        status = False
    

    #inserting data
    if status:
        try:
            print('Inserting data')
            df.to_sql(
                'netflix_data',
                engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            print(f'Data loaded to {server}')
        except Exception as e:
            print('Error on insert:\n{e}')

    #saving df as json and testing it
    df.to_json(
        path_or_buf=r'ETL\netflix_data.json',
        orient='table',
        date_format='iso',
        date_unit='ms',
        force_ascii=False,
        double_precision=6,
        lines=False,
        index=False
    )

    jsn_df = pd.read_json(r'ETL\netflix_data.json', orient='table')
    print(jsn_df.head(5))
    

def main():
    csv_to_sql()

if __name__ == "__main__":
    main()


