import pandas as pd
from pymongo import MongoClient

def connect_db():
    # 连接数据库
    client = MongoClient('mongodb://localhost:27017/')
    db = client['mydatabase']
    return db['recruit']

def convert_to_string(df):
    # 将所有数字字段转换为字符串
    numeric_columns = ['id', 'salary_median', 'experience_min', 'company_size_median']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df

def insert_data_from_csv(collection):
    # 从CSV文件读取数据并插入到MongoDB
    df = pd.read_csv('recruit.csv')
    print(df.head())
    
    # 列名映射
    column_mapping = {
        'id': 'id',
        '城市': 'city',
        '关键词': 'keyword',
        '职位': 'position',
        '地区': 'area',
        '学历要求': 'education_requirement',
        '公司名称': 'company_name',
        '公司类型': 'company_type',
        '薪资中值': 'salary_median',
        '最低经验要求': 'experience_min',
        '公司规模中值': 'company_size_median',
        '标签': 'tags',
        '福利待遇列表': 'benefits'
    }
    df.rename(columns=column_mapping, inplace=True)
    
    # 将所有数字字段转换为字符串
    df = convert_to_string(df)
    
    # 转换为字典并插入到MongoDB
    data_dict = df.to_dict(orient='records')
    result = collection.insert_many(data_dict)
    print(f"Inserted {len(result.inserted_ids)} documents.")

def main():
    collection = connect_db()
    insert_data_from_csv(collection)

if __name__ == "__main__":
    main()