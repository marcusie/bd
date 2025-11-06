import redis

# 连接到Redis服务器，设置decode_responses=True
r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

def insert_data():
    key = input("请输入ID: ")
    city = input("请输入城市: ")
    keyword = input("请输入关键词: ")
    position = input("请输入职位: ")
    area = input("请输入地区: ")
    education = input("请输入学历要求: ")
    company_name = input("请输入公司名称: ")
    company_type = input("请输入公司类型: ")
    salary_median = input("请输入薪资中值: ")
    min_experience = input("请输入最低经验要求: ")
    company_size_median = input("请输入公司规模中值: ")
    tags = input("请输入标签 (用逗号分隔): ")
    benefits = input("请输入福利待遇 (用逗号分隔): ")

    data = {
        'city': city,
        'keyword': keyword,
        'position': position,
        'area': area,
        'education': education,
        'company_name': company_name,
        'company_type': company_type,
        'salary_median': salary_median,
        'min_experience': min_experience,
        'company_size_median': company_size_median,
        'tags': tags,
        'benefits': benefits
    }

    r.hmset(f"job:{key}", data)
    print("数据插入成功！")

def delete_data():
    key = input("请输入要删除的ID: ")
    if r.exists(f"job:{key}"):
        r.delete(f"job:{key}")
        print("数据删除成功！")
    else:
        print("未找到该ID的数据。")

def update_data():
    key = input("请输入要修改的ID: ")
    if not r.exists(f"job:{key}"):
        print("未找到该ID的数据。")
        return

    fields = [
        ('city', "请输入新的城市: "),
        ('keyword', "请输入新的关键词: "),
        ('position', "请输入新的职位: "),
        ('area', "请输入新的地区: "),
        ('education', "请输入新的学历要求: "),
        ('company_name', "请输入新的公司名称: "),
        ('company_type', "请输入新的公司类型: "),
        ('salary_median', "请输入新的薪资中值: "),
        ('min_experience', "请输入新的最低经验要求: "),
        ('company_size_median', "请输入新的公司规模中值: "),
        ('tags', "请输入新的标签 (用逗号分隔): "),
        ('benefits', "请输入新的福利待遇 (用逗号分隔): ")
    ]

    updates = {}
    for field, prompt in fields:
        value = input(prompt)
        if value.strip():  # 如果输入不为空
            updates[field] = value

    if updates:
        r.hmset(f"job:{key}", updates)
        print("数据修改成功！")
    else:
        print("没有进行任何修改。")

def query_data():
    print("\n请选择查询方式：")
    print("1. 按ID查询")
    print("2. 按其他字段查询")
    choice = input("请输入选项 (1-2): ")

    if choice == '1':
        key = input("请输入要查询的ID: ")
        if not r.exists(f"job:{key}"):
            print("未找到该ID的数据。")
            return

        data = r.hgetall(f"job:{key}")
        for k, v in data.items():
            print(f"{k}: {v}")

    elif choice == '2':
        fields = [
            ('city', "查询城市 (留空表示不查询): "),
            ('keyword', "查询关键词 (留空表示不查询): "),
            ('position', "查询职位 (留空表示不查询): "),
            ('area', "查询地区 (留空表示不查询): "),
            ('education', "查询学历要求 (留空表示不查询): "),
            ('company_name', "查询公司名称 (留空表示不查询): "),
            ('company_type', "查询公司类型 (留空表示不查询): "),
            ('salary_median', "查询薪资中值 (留空表示不查询): "),
            ('min_experience', "查询最低经验要求 (留空表示不查询): "),
            ('company_size_median', "查询公司规模中值 (留空表示不查询): "),
            ('tags', "查询标签 (留空表示不查询): "),
            ('benefits', "查询福利待遇 (留空表示不查询): ")
        ]

        queries = {}
        for field, prompt in fields:
            value = input(prompt)
            if value.strip():  # 如果输入不为空
                queries[field] = value

        if queries:
            keys = [key for key in r.scan_iter(match="job:*")]
            found = False
            for key in keys:
                data = r.hgetall(key)
                match = all(data.get(field) == value for field, value in queries.items())
                if match:
                    found = True
                    print(f"\n匹配的记录: {key}")
                    for k, v in data.items():
                        print(f"{k}: {v}")
            if not found:
                print("未找到匹配的数据。")
        else:
            print("没有指定任何查询条件。")

    else:
        print("无效的选项，请重新输入。")

def main_menu():
    while True:
        print("\n请选择操作：")
        print("1. 插入数据")
        print("2. 删除数据")
        print("3. 修改数据")
        print("4. 查询数据")
        print("5. 退出")
        
        choice = input("请输入选项 (1-5): ")
        
        if choice == '1':
            insert_data()
        elif choice == '2':
            delete_data()
        elif choice == '3':
            update_data()
        elif choice == '4':
            query_data()
        elif choice == '5':
            print("退出程序。")
            break
        else:
            print("无效的选项，请重新输入。")

if __name__ == "__main__":
    main_menu()