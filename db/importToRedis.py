import pandas as pd
import redis

# 连接到Redis服务器，设置decode_responses=True
r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# 清空当前数据库
r.flushdb()

# 读取CSV文件
df = pd.read_csv('recruit.csv')

# 遍历每一行数据
for index, row in df.iterrows():
    # 使用id字段作为键
    key = f"job:{row['id']}"
    
    # 创建一个字典来存储其他字段
    data = {
        'city': row['城市'],
        'keyword': row['关键词'],
        'position': row['职位'],
        'area': row['地区'],
        'education': row['学历要求'],
        'company_name': row['公司名称'],
        'company_type': row['公司类型'],
        'salary_median': str(row['薪资中值']),  # 确保数值类型转换为字符串
        'min_experience': str(row['最低经验要求']),  # 确保数值类型转换为字符串
        'company_size_median': str(row['公司规模中值']),  # 确保数值类型转换为字符串
        'tags': row['标签'],
        'benefits': row['福利待遇列表']
    }
    
    # 将数据存储为哈希
    r.hmset(key, data)

print("Data imported successfully!")