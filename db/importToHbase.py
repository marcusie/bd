import happybase
import csv

def connect_to_hbase():
    connection = happybase.Connection('localhost')
    return connection

def create_table(connection, table_name):
    # 检查表是否已经存在
    if bytes(table_name, encoding='utf-8') in connection.tables():
        print(f"Table {table_name} already exists.")
    else:
        # 创建表
        families = {
            'info': dict(),  # 列族
        }
        connection.create_table(table_name, families)
        print(f"Created table {table_name}.")

def insert_data_into_hbase(connection, table_name, data):
    table = connection.table(table_name)
    record_count = 0  # 计数器初始化
    with table.batch() as b:
        for row in data:
            # 将数据转换为字节串
            row_key = f"{row[0]}".encode('utf-8')
            row_data = {
                'info:city': row[1].encode('utf-8'),
                'info:keyword': row[2].encode('utf-8'),
                'info:position': row[3].encode('utf-8'),
                'info:area': row[4].encode('utf-8'),
                'info:education_requirement': row[5].encode('utf-8'),
                'info:company_name': row[6].encode('utf-8'),
                'info:company_type': row[7].encode('utf-8'),
                'info:salary_median': str(row[8]).encode('utf-8'),  # 将浮点数转换为字符串再编码
                'info:experience_min': str(row[9]).encode('utf-8'),  # 将整数转换为字符串再编码
                'info:company_size_median': str(row[10]).encode('utf-8'),  # 将整数转换为字符串再编码
                'info:tags': row[11].encode('utf-8'),
                'info:benefits': row[12].encode('utf-8')
            }
            b.put(row_key, row_data)
            record_count += 1  # 每次插入后递增计数器
    print(f"Inserted {record_count} records into the table {table_name}.")

def read_csv_and_prepare_data(file_path):
    data = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # 跳过标题行
        for row in reader:
            # 假设CSV中的所有字段都是字符串类型，除了数字类型的字段需要转换
            row[8] = float(row[8])  # 薪资中值
            row[9] = int(float(row[9]))  # 最低经验要求
            row[10] = int(float(row[10]))  # 公司规模中值
            data.append(row)
    return data

if __name__ == '__main__':
    conn = connect_to_hbase()
    create_table(conn, 'recruit')
    data = read_csv_and_prepare_data('recruit.csv')
    insert_data_into_hbase(conn, 'recruit', data)
    conn.close()