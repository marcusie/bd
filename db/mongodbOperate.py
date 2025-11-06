from pymongo import MongoClient
import sys

def connect_to_mongodb():
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['mydatabase']
        collection = db['recruit']
        return collection
    except Exception as e:
        print(f"连接MongoDB时发生错误: {e}")
        sys.exit(1)

def insert_data(collection):
    city = input("请输入城市: ")
    keyword = input("请输入关键词: ")
    position = input("请输入职位: ")
    area = input("请输入地区: ")
    education_requirement = input("请输入学历要求: ")
    company_name = input("请输入公司名称: ")
    company_type = input("请输入公司类型: ")
    salary_median = input("请输入薪资中值: ")
    salary = input("请输入薪资: ")
    min_experience = int(input("请输入最低经验要求（年）: "))
    company_size_median = int(input("请输入公司规模中值: "))
    tags = input("请输入标签（逗号分隔）: ").split(',')
    benefits = input("请输入福利待遇列表（逗号分隔）: ").split(',')

    document = {
        "城市": city,
        "关键词": keyword,
        "职位": position,
        "地区": area,
        "学历要求": education_requirement,
        "公司名称": company_name,
        "公司类型": company_type,
        "薪资中值": salary_median,
        "薪资": salary,
        "最低经验要求": min_experience,
        "公司规模中值": company_size_median,
        "标签": tags,
        "福利待遇列表": benefits
    }
    result = collection.insert_one(document)
    print(f"文档已插入，ID: {result.inserted_id}")

def delete_data(collection):
    company_name = input("请输入要删除的公司名称: ")
    result = collection.delete_one({"公司名称": company_name})
    if result.deleted_count > 0:
        print(f"公司名称为 '{company_name}' 的文档已成功删除。")
    else:
        print(f"未找到公司名称为 '{company_name}' 的文档。")

def update_data(collection):
    company_name = input("请输入要更新的公司名称: ")
    new_salary_median = float(input("请输入新的薪资中值: "))
    new_benefits = input("请输入新的福利待遇列表（逗号分隔）: ").split(',')

    result = collection.update_one(
        {"公司名称": company_name},
        {"$set": {"薪资中值": new_salary_median, "福利待遇列表": new_benefits}}
    )
    if result.modified_count > 0:
        print(f"公司名称为 '{company_name}' 的文档已成功更新。")
    else:
        print(f"未找到公司名称为 '{company_name}' 的文档。")

def query_data(collection):
    company_name = input("请输入要查询的公司名称: ")
    document = collection.find_one({"公司名称": company_name})
    if document:
        print(f"找到文档: {document}")
    else:
        print(f"未找到公司名称为 '{company_name}' 的文档。")

def main_menu():
    print("\nMongoDB 操作菜单")
    print("1. 插入数据")
    print("2. 删除数据")
    print("3. 修改数据")
    print("4. 查询数据")
    print("5. 退出")
    choice = input("请输入您的选择（1-5）: ")
    return choice

def main():
    collection = connect_to_mongodb()
    
    while True:
        choice = main_menu()
        
        if choice == '1':
            insert_data(collection)
        elif choice == '2':
            delete_data(collection)
        elif choice == '3':
            update_data(collection)
        elif choice == '4':
            query_data(collection)
        elif choice == '5':
            print("退出程序。")
            break
        else:
            print("无效的选择。请输入1到5之间的数字。")

if __name__ == "__main__":
    main()
