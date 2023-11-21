import streamlit as st
import requests
import zipfile
import io
import duckdb
import os

def download_and_extract_zip(url, extract_to='.'):
    """
    Download a ZIP file and extract its contents.
    """
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        thezip.extractall(path=extract_to)
        # print("Extracted files:", thezip.namelist())  # 打印解压的文件列表

def list_files_in_directory(directory):
    """
    列出指定目录下的所有文件。
    """
    return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

def sniff_csv_with_duckdb(csv_file_path):
    """
    使用 DuckDB 来嗅探 CSV 文件的列名和部分数据。
    """
    conn = duckdb.connect(database=':memory:')
    query = f"SELECT * FROM read_csv_auto('{csv_file_path}', SAMPLE_SIZE=1048576) LIMIT 100"
    result = conn.execute(query).fetchdf()
    return result

def main():
    # 设置Streamlit应用的标题
    st.title("CSV文件探索器")

    # 1. Download and extract the ZIP file
    zip_url = "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_csv_2023-10-26.zip"
    extract_folder = 'extracted_csv'
    download_and_extract_zip(zip_url, extract_to=extract_folder)

    # 选择文件夹
    folder_path = 'extracted_csv/FoodData_Central_foundation_food_csv_2023-10-26'
    if os.path.exists(folder_path):
        files = list_files_in_directory(folder_path)
        selected_file = st.selectbox("选择一个文件", files)

        if selected_file:
            # 显示文件的列名和数据
            csv_file_path = os.path.join(folder_path, selected_file)
            df = sniff_csv_with_duckdb(csv_file_path)
            columns = df.columns.tolist()

            # 让用户选择要显示的列
            selected_columns = st.multiselect("选择列", columns, default=columns)

            # 显示数据
            if st.button("Run"):
                st.write(df[selected_columns])

if __name__ == "__main__":
    main()
