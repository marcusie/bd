import pymongo

def connect_db():
    # 连接数据库
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['mydatabase']
    return db['recruit']

def insert_data(collection):
    # 插入数据
    data = {}
    fields = [
        ('id', str),
        ('city', str),
        ('keyword', str),
        ('position', str),
        ('area', str),
        ('education_requirement', str),
        ('company_name', str),
        ('company_type', str),
        ('salary_median', str),  # 将 salary_median 也转换为字符串
        ('experience_min', str),  # 将 experience_min 也转换为字符串
        ('company_size_median', str),  # 将 company_size_median 也转换为字符串
        ('tags', str),
        ('benefits', str)
    ]
    
    for field, data_type in fields:
        value = input(f"Enter {field} (leave empty to skip): ")
        if value:
            data[field] = data_type(value)
    
    if data:
        result = collection.insert_one(data)
        print(f"Document inserted with _id: {result.inserted_id}")
    else:
        print("No data to insert.")

def update_data(collection):
    # 修改数据
    id_to_update = input("Enter the ID of the document to update: ")
    if not id_to_update:
        print("ID cannot be empty.")
        return

    updates = {}
    while True:
        field_to_update = input("Enter the field you want to update (leave empty to stop): ")
        if not field_to_update:
            break
        new_value = input(f"Enter the new value for {field_to_update}: ")
        updates[field_to_update] = str(new_value)  # 将新值转换为字符串

    if updates:
        result = collection.update_one({'id': id_to_update}, {'$set': updates})  # 使用字符串形式的 id
        if result.modified_count > 0:
            print("Document updated successfully.")
        else:
            print("No document found with that ID or no changes made.")
    else:
        print("No fields to update.")

def query_data(collection):
    # 查询数据
    query = {}
    while True:
        query_field = input("Enter the field to query by (leave empty to stop): ")
        if not query_field:
            break
        query_value = input(f"Enter the value to search for {query_field}: ")
        query[query_field] = str(query_value)  # 将查询值转换为字符串

    if query:
        results = collection.find(query)
        for doc in results:
            print(doc)
    else:
        print("No query fields provided.")

def delete_data(collection):
    # 删除数据
    id_to_delete = input("Enter the ID of the document to delete: ")
    if not id_to_delete:
        print("ID cannot be empty.")
        return

    result = collection.delete_one({'id': id_to_delete})  # 使用字符串形式的 id
    if result.deleted_count > 0:
        print("Document deleted successfully.")
    else:
        print("No document found with that ID.")

def main_menu():
    collection = connect_db()
    while True:
        print("\nMongoDB Operations Menu")
        print("1. Insert Data")
        print("2. Update Data")
        print("3. Query Data")
        print("4. Delete Data")
        print("5. Exit")
        choice = input("Please choose an option: ")

        if choice == '1':
            insert_data(collection)
        elif choice == '2':
            update_data(collection)
        elif choice == '3':
            query_data(collection)
        elif choice == '4':
            delete_data(collection)
        elif choice == '5':
            print("Exiting program.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main_menu()