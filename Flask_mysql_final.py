from flask import Flask, jsonify, request
import mysql.connector
from config import mysql_config , output_directory
import pandas as pd
import logging
import os
import itertools

app = Flask(__name__)

def setup_logging():
    file_handler = logging.FileHandler('minorproject_final.log')
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)

setup_logging()

def connect_to_database():
    '''
    '''
    try:
        connection = mysql.connector.connect(**mysql_config)
        if connection.is_connected():
            app.logger.info("Successfully connected to MySQL database")
            return connection
    except Exception as e:
        app.logger.error(f"Error connecting to MySQL: {e}")
        return None


def check_combinations(df, columns):
    #print("Checking combinations of columns for uniqueness:")
    for r in range(2, len(columns) + 1):
        for combo in itertools.combinations(columns, r):
            combined = df[list(combo)].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
            unique_count = combined.nunique()
            total_count = df.shape[0]
            #print(f"Combination {combo} has {unique_count} unique out of {total_count} total rows.")
            if unique_count == total_count:
                #print(f"Combination {combo} can be a unique identifier.")
                return combo
    return None

def find_id(df):
    potential_id=[]
    num_rows = df.shape[0]
    unique_id_found = False
    try:
        for column in df.columns:
            unique_value_count= df[column].nunique()
            non_null_count = df[column].notnull().sum()
            if unique_value_count== num_rows and non_null_count == num_rows:
                unique_id_found= True
                potential_id.append(column)
        if not unique_id_found:
            combo=check_combinations(df, df.columns)
            if combo:
                potential_id.append(column)

        return potential_id

    except Exception as e:
        app.logger.error(f'error ocurred in find_id:str{e}')

def yearmonth_avail(data):
    try:
        if 'InvoiceDate' in data.columns:
            data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'], format='%m/%d/%y %H:%M')
            data['Year'] = data['InvoiceDate'].dt.year
            unique_years = data['Year'].unique().tolist()
            months_available = data.groupby(data['Year'])['InvoiceDate'].apply(lambda x: sorted(x.dt.month.unique()))
            months_result = {int(year): list(map(int, months)) for year, months in months_available.items()}
            return unique_years, months_result
        else:
            return [], {}
    except Exception as e:
        app.logger.error(f'Unexpected error occurred in yearmonth_avail: {str(e)}')

def groupdata(data):
    try:
        if 'InvoiceDate' in data.columns:
            data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'], errors='coerce')
            data['YearMonth'] = data['InvoiceDate'].dt.to_period('M')
            data_grouped = data.groupby('YearMonth')
            savedfiles = []
            base_output_directory = os.path.join(output_directory, "grouped_csv_files")
            os.makedirs(base_output_directory, exist_ok=True)
            for year_month, group in data_grouped:
                year = year_month.year
                year_directory = os.path.join(base_output_directory, str(year))
                os.makedirs(year_directory, exist_ok=True)
                file_name = f"dataincsv1_{year_month}.csv"
                file_path = os.path.join(year_directory, file_name)
                
                # Save the grouped data to CSV
                group.to_csv(file_path, index=False)
                savedfiles.append(file_path)
            
            return savedfiles
        else:
            return []
    except Exception as e:
        app.logger.error(f'Error occurred in groupdata: {str(e)}')
        return []


'''def groupdata(data):
    try:
        if 'InvoiceDate' in data.columns:
            data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'], errors='coerce')
            data['YearMonth'] = data['InvoiceDate'].dt.to_period('M')
            data_grouped = data.groupby('YearMonth')
            savedfiles = []
            output_directory = os.path.join(os.getcwd(), "grouped_data_files")
            os.makedirs(output_directory, exist_ok=True)
            for year_month, group in data_grouped:
                file_name = f"dataincsv1_{year_month}.csv"
                file_path = os.path.join(output_directory, file_name)
                group.to_csv(file_path, index=False)
                savedfiles.append(file_path)
            return savedfiles
        else:
            return []
    except Exception as e:
        app.logger.error(f'Error occurred in groupdata: {str(e)}')'''

@app.route('/preprocess', methods=['POST'])
def preprocess():
    app.logger.info("Preprocess Started")
    try:
        file_path = request.json.get('file_path')
        
        if not file_path:
            app.logger.error('File path not provided')
            return jsonify({"error": "File path not provided"}), 400

        if not os.path.exists(file_path):
            app.logger.error(f'File {file_path} does not exist')
            return jsonify({"error": f"File {file_path} does not exist"}), 404

        data = pd.read_csv(file_path)
        app.logger.info("File Read Successfully")

        # Compute unique values, missing values, and data types 
        unique_values_result = {col: int(data[col].nunique()) for col in data.columns}
        missing_val_result = {col: int(data[col].isnull().sum()) for col in data.columns if data[col].isnull().sum() > 0}
        data_typ_result = {col: str(data[col].dtype) for col in data.columns}

        # Drop duplicate rows
        data = data.drop_duplicates()

        #find ID
        #find_id_result= find_id(data)

        # Store unique values, missing values, and data types in a CSV file
        output_directory = os.path.join(os.getcwd(), "preprocess_output")
        os.makedirs(output_directory, exist_ok=True)
        output_file = os.path.join(output_directory, "preprocess_results.csv")
        result_df = pd.DataFrame({
            'column_name': list(data.columns),
            'unique_values': [unique_values_result.get(col, 0) for col in data.columns],
            'data_types': [data_typ_result.get(col, 'unknown') for col in data.columns],
            'missing_values': [missing_val_result.get(col, 0) for col in data.columns]
        })
        result_df.to_csv(output_file, index=False)

        #Get the available years, and months available in each year
        yearmonth_avail_year, yearmonth_avail_months = yearmonth_avail(data)

        #group the data based on the month and year and store them in csv files based on month & year available
        groupdata_result = groupdata(data)

        result = {
            'output_file': output_file,
            'unique_values': unique_values_result,
            'missing_values': missing_val_result,
            'data_types': data_typ_result,
            #'ID' : find_id_result,
            'year_months': {
                'unique_years': [int(year) for year in yearmonth_avail_year],
                'months_available': {int(year): [int(month) for month in months] for year, months in yearmonth_avail_months.items()}
            },
            'grouped_files': groupdata_result
        }
        
        return jsonify(result), 200
    
    except Exception as e:
        app.logger.error('Error in preprocess route')
        return jsonify({"error": str(e)}), 500

@app.route('/mysql_push', methods=['POST'])
def mysql_push():
    try:
        #csv_path = request.json.get('csv_path')
        
        #if not csv_path or not os.path.exists(csv_path):
            #app.logger.error('Invalid CSV file path provided')
            #return jsonify({"error": "Invalid CSV file path provided"}), 400
        outputfile_path = os.path.join(os.getcwd(), "preprocess_output")
        output_file_path = os.path.join(outputfile_path, "preprocess_results.csv")

        data = pd.read_csv(output_file_path)
        connection = connect_to_database()
        if connection:
            try:
                cur = connection.cursor()
                create_table_query = '''
                    CREATE TABLE IF NOT EXISTS column_anlys(
                        column_name VARCHAR(255) PRIMARY KEY,
                        unique_values INT,
                        data_type VARCHAR(255),
                        missing_count INT
                    )
                '''
                cur.execute(create_table_query)
                insert_query = """
                INSERT INTO column_anlys(column_name, unique_values, data_type, missing_count)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                unique_values = VALUES(unique_values),
                data_type = VALUES(data_type),
                missing_count = VALUES(missing_count)
                """
                
                cur.executemany(insert_query, data.values.tolist())
                connection.commit()
                cur.close()
                app.logger.info('Data successfully pushed to MySQL')
                return jsonify({"message": "Data successfully pushed to MySQL"}), 200

            except Exception as e:
                app.logger.error(f'Error pushing data to MySQL: {str(e)}')
                return jsonify({"error": str(e)}), 500
            finally:
                connection.close()
        else:
            return jsonify({"error": "Could not connect to the database"}), 500

    except Exception as e:
        app.logger.error('Error in mysql_push endpoint')
        return jsonify({"error": str(e)}), 500
if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=5000)
