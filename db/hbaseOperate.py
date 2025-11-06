import happybase
import sys

def connect_to_hbase():
    try:
        connection = happybase.Connection('localhost')
        return connection
    except Exception as e:
        print(f"Error connecting to HBase: {e}")
        sys.exit(1)

def create_table(connection, table_name):
    try:
        if bytes(table_name, encoding='utf-8') not in connection.tables():
            families = {'info': dict()}
            connection.create_table(table_name, families)
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_data(connection, table_name, row_key, data_dict):
    try:
        table = connection.table(table_name)
        with table.batch() as b:
            b.put(row_key.encode('utf-8'), {k.encode('utf-8'): str(v).encode('utf-8') for k, v in data_dict.items()})
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")

def update_data(connection, table_name, row_key, data_dict):
    try:
        table = connection.table(table_name)
        with table.batch() as b:
            for field, value in data_dict.items():
                b.put(row_key.encode('utf-8'), {field.encode('utf-8'): str(value).encode('utf-8')})
        print("Data updated successfully.")
    except Exception as e:
        print(f"Error updating data: {e}")

def delete_data(connection, table_name, row_key):
    try:
        table = connection.table(table_name)
        table.delete(row_key.encode('utf-8'))
        print("Data deleted successfully.")
    except Exception as e:
        print(f"Error deleting data: {e}")

def query_by_id(connection, table_name, row_key):
    try:
        table = connection.table(table_name)
        row = table.row(row_key.encode('utf-8'))
        if row:
            print("Row found:")
            for key, value in row.items():
                print(f"{key.decode('utf-8')}: {value.decode('utf-8')}")
        else:
            print("No row found.")
    except Exception as e:
        print(f"Error querying by ID: {e}")

def query_by_field(connection, table_name, conditions):
    try:
        table = connection.table(table_name)
        filters = [f"SingleColumnValueFilter('info', '{field}', =, 'binary:{value}')" for field, value in conditions.items()]
        filter_str = " AND ".join(filters)
        rows = table.scan(filter=filter_str)
        for key, data in rows:
            print(f"Row Key: {key.decode('utf-8')}")
            for col, val in data.items():
                print(f"  {col.decode('utf-8')}: {val.decode('utf-8')}")
    except Exception as e:
        print(f"Error querying by field: {e}")

def fuzzy_query(connection, table_name, column, pattern):
    try:
        table = connection.table(table_name)
        # 使用PrefixFilter实现模糊查询
        rows = table.scan(filter=f"PrefixFilter('{pattern}')")
        for key, data in rows:
            print(f"Row Key: {key.decode('utf-8')}, Column: {column}, Value: {data.get(column.encode('utf-8')).decode('utf-8')}")
    except Exception as e:
        print(f"Error performing fuzzy query: {e}")

def main_menu():
    print("\nHBase Operations Menu:")
    print("1. Insert Data")
    print("2. Update Data")
    print("3. Delete Data")
    print("4. Query by ID")
    print("5. Query by Field")
    print("6. Fuzzy Query")
    print("0. Exit")
    choice = input("Choose an operation (0-6): ")
    return choice

def get_conditions():
    conditions = {}
    while True:
        field = input("Enter field name (without 'info:'), or press Enter to finish: ")
        if not field:
            break
        value = input(f"Enter value to match for field '{field}': ")
        conditions[field] = value
    return conditions

def get_update_fields():
    fields = {}
    while True:
        field = input("Enter field name to update (without 'info:'), or press Enter to finish: ")
        if not field:
            break
        value = input(f"Enter new value for field '{field}': ")
        fields[f'info:{field}'] = value
    return fields

def main():
    conn = connect_to_hbase()
    create_table(conn, 'recruit')

    while True:
        choice = main_menu()
        if choice == '1':
            row_key = input("Enter Row Key: ")
            data_dict = {}
            for i in ['city', 'keyword', 'position', 'area', 'education_requirement', 'company_name', 'company_type', 'salary_median', 'experience_min', 'company_size_median', 'tags', 'benefits']:
                data_dict[f'info:{i}'] = input(f"Enter {i}: ")
            insert_data(conn, 'recruit', row_key, data_dict)
        elif choice == '2':
            row_key = input("Enter Row Key to update: ")
            fields = get_update_fields()
            update_data(conn, 'recruit', row_key, fields)
        elif choice == '3':
            row_key = input("Enter Row Key to delete: ")
            delete_data(conn, 'recruit', row_key)
        elif choice == '4':
            row_key = input("Enter Row Key to query: ")
            query_by_id(conn, 'recruit', row_key)
        elif choice == '5':
            conditions = get_conditions()
            query_by_field(conn, 'recruit', conditions)
        elif choice == '6':
            column = input("Enter column to query (with 'info:'): ")
            pattern = input("Enter prefix pattern: ")
            fuzzy_query(conn, 'recruit', column, pattern)
        elif choice == '0':
            print("Exiting program.")
            break
        else:
            print("Invalid choice, please choose again.")
    
    conn.close()

if __name__ == '__main__':
    main()