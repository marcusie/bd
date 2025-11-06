from py2neo import Graph, Node, Relationship, NodeMatcher

# 连接到Neo4j数据库
graph = Graph("bolt://localhost:7687", auth=("neo4j", "512436lkl"))

# 创建NodeMatcher对象
node_matcher = NodeMatcher(graph)

def insert_single_node():
    node_label = input("请输入节点标签 (City/Company/Position): ")
    node_property = input("请输入节点属性 (name/title): ")
    node_value = input("请输入节点属性值: ")

    node = Node(node_label, **{node_property: node_value})
    graph.merge(node, node_label, node_property)
    print(f"节点 {node_label} ({node_property}: {node_value}) 插入完成！")

def insert_single_relationship():
    from_node_label = input("请输入起始节点标签 (City/Company/Position): ")
    from_node_property = input("请输入起始节点属性 (name/title): ")
    from_node_value = input("请输入起始节点属性值: ")
    to_node_label = input("请输入目标节点标签 (City/Company/Position): ")
    to_node_property = input("请输入目标节点属性 (name/title): ")
    to_node_value = input("请输入目标节点属性值: ")
    relationship_type = input("请输入关系类型 (HAS_COMPANY/HAS_POSITION): ")

    from_node = graph.nodes.match(from_node_label, **{from_node_property: from_node_value}).first()
    to_node = graph.nodes.match(to_node_label, **{to_node_property: to_node_value}).first()
    
    if from_node and to_node:
        relationship = Relationship(from_node, relationship_type, to_node)
        graph.create(relationship)
        print(f"关系 {from_node_label} ({from_node_property}: {from_node_value}) -> {to_node_label} ({to_node_property}: {to_node_value}) 插入完成！")
    else:
        print("无法找到指定的节点，关系插入失败！")

def insert_complete_data():
    city_name = input("请输入城市名称: ")
    company_name = input("请输入公司名称: ")
    position_title = input("请输入职位名称: ")

    # 创建或获取City节点
    city_node = Node('City', name=city_name)
    graph.merge(city_node, 'City', 'name')

    # 创建或获取Company节点
    company_node = Node('Company', name=company_name)
    graph.merge(company_node, 'Company', 'name')

    # 创建或获取Position节点
    position_node = Node('Position', title=position_title)
    graph.merge(position_node, 'Position', 'title')

    # 创建关系 City -> Company
    relationship_city_company = Relationship(city_node, 'HAS_COMPANY', company_node)
    graph.create(relationship_city_company)

    # 创建关系 Company -> Position
    relationship_company_position = Relationship(company_node, 'HAS_POSITION', position_node)
    graph.create(relationship_company_position)

    print("数据插入完成！")

def update_node():
    node_label = input("请输入节点标签 (City/Company/Position): ")
    node_property = input("请输入节点属性 (name/title): ")
    old_value = input("请输入旧的属性值: ")
    new_value = input("请输入新的属性值: ")

    # 检查新值是否已经存在
    check_query = f"MATCH (n:{node_label} {{{node_property}: $new_value}}) RETURN count(n)"
    result = graph.run(check_query, new_value=new_value).evaluate()

    if result > 0:
        print(f"节点 {node_label} ({node_property}: {new_value}) 已经存在，无法更新！")
        return

    query = f"MATCH (n:{node_label} {{{node_property}: $old_value}}) SET n.{node_property} = $new_value"
    graph.run(query, old_value=old_value, new_value=new_value)
    print(f"节点 {node_label} ({node_property}: {old_value}) 更新为 {new_value} 完成！")

def update_relationship():
    from_node_label = input("请输入起始节点标签 (City/Company/Position): ")
    from_node_property = input("请输入起始节点属性 (name/title): ")
    from_node_value = input("请输入起始节点属性值: ")
    to_node_label = input("请输入目标节点标签 (City/Company/Position): ")
    to_node_property = input("请输入目标节点属性 (name/title): ")
    to_node_value = input("请输入目标节点属性值: ")
    old_relationship_type = input("请输入旧的关系类型 (HAS_COMPANY/HAS_POSITION): ")
    new_relationship_type = input("请输入新的关系类型 (HAS_COMPANY/HAS_POSITION): ")

    # 检查起始节点是否存在
    check_from_node_query = f"MATCH (n:{from_node_label} {{{from_node_property}: $from_node_value}}) RETURN count(n)"
    from_node_exists = graph.run(check_from_node_query, from_node_value=from_node_value).evaluate() > 0

    if not from_node_exists:
        print(f"起始节点 {from_node_label} ({from_node_property}: {from_node_value}) 不存在，无法更新关系！")
        return

    # 检查目标节点是否存在
    check_to_node_query = f"MATCH (n:{to_node_label} {{{to_node_property}: $to_node_value}}) RETURN count(n)"
    to_node_exists = graph.run(check_to_node_query, to_node_value=to_node_value).evaluate() > 0

    if not to_node_exists:
        print(f"目标节点 {to_node_label} ({to_node_property}: {to_node_value}) 不存在，无法更新关系！")
        return

    query = f"""
    MATCH (from_node:{from_node_label} {{{from_node_property}: $from_node_value}})-[r:{old_relationship_type}]->(to_node:{to_node_label} {{{to_node_property}: $to_node_value}})
    DELETE r
    CREATE (from_node)-[:{new_relationship_type}]->(to_node)
    """
    graph.run(query, from_node_value=from_node_value, to_node_value=to_node_value)
    print(f"关系 {from_node_label} ({from_node_property}: {from_node_value}) -> {to_node_label} ({to_node_property}: {to_node_value}) 从 {old_relationship_type} 更新为 {new_relationship_type} 完成！")

def query_company_city():
    company_name = input("请输入公司名称: ")
    result = graph.run(
        "MATCH (city:City)-[:HAS_COMPANY]->(company:Company {name: $company_name}) "
        "RETURN city.name",
        company_name=company_name
    ).data()
    if result:
        print(f"公司 {company_name} 所在城市: {result[0]['city.name']}")
    else:
        print(f"未找到公司 {company_name}")

def query_city_company_count():
    city_name = input("请输入城市名称: ")
    result = graph.run(
        "MATCH (city:City {name: $city_name})-[:HAS_COMPANY]->(company:Company) "
        "RETURN count(DISTINCT company)",
        city_name=city_name
    ).data()
    if result:
        print(f"城市 {city_name} 有 {result[0]['count(DISTINCT company)']} 家公司")
    else:
        print(f"未找到城市 {city_name}")

def query_company_positions():
    company_name = input("请输入公司名称: ")
    result = graph.run(
        "MATCH (company:Company {name: $company_name})-[:HAS_POSITION]->(position:Position) "
        "RETURN position.title",
        company_name=company_name
    ).data()
    if result:
        positions = [record['position.title'] for record in result]
        print(f"公司 {company_name} 的职位: {positions}")
    else:
        print(f"未找到公司 {company_name}")

def query_single_node():
    node_label = input("请输入节点标签 (City/Company/Position): ")
    node_property = input("请输入节点属性 (name/title): ")
    node_value = input("请输入节点属性值: ")

    # 查询节点
    node = node_matcher.match(node_label, **{node_property: node_value}).first()

    if node:
        print(f"找到节点 {node_label} ({node_property}: {node_value})")
        print(node)
    else:
        print(f"未找到节点 {node_label} ({node_property}: {node_value})")

def query_relationship():
    from_node_label = input("请输入起始节点标签 (City/Company/Position): ")
    from_node_property = input("请输入起始节点属性 (name/title): ")
    from_node_value = input("请输入起始节点属性值: ")
    to_node_label = input("请输入目标节点标签 (City/Company/Position): ")
    to_node_property = input("请输入目标节点属性 (name/title): ")
    to_node_value = input("请输入目标节点属性值: ")

    result = graph.run(
        f"MATCH (from_node:{from_node_label} {{ {from_node_property}: $from_node_value }})-[r]->(to_node:{to_node_label} {{ {to_node_property}: $to_node_value }}) "
        "RETURN type(r) AS relationship_type, r",
        from_node_value=from_node_value,
        to_node_value=to_node_value
    ).data()

    if result:
        for record in result:
            relationship_type = record['relationship_type']
            properties = record['r']
            print(f"关系类型: {relationship_type}, 属性: {properties}")
    else:
        print(f"未找到 {from_node_label} ({from_node_property}: {from_node_value}) 和 {to_node_label} ({to_node_property}: {to_node_value}) 之间的关系")
def delete_node():
    node_label = input("请输入节点标签 (City/Company/Position): ")
    node_property = input("请输入节点属性 (name/title): ")
    node_value = input("请输入节点属性值: ")

    query = f"MATCH (n:{node_label} {{{node_property}: $node_value}}) DETACH DELETE n"
    graph.run(query, node_value=node_value)
    print(f"节点 {node_label} ({node_property}: {node_value}) 删除完成！")

def delete_relationship():
    from_node_label = input("请输入起始节点标签 (City/Company/Position): ")
    from_node_property = input("请输入起始节点属性 (name/title): ")
    from_node_value = input("请输入起始节点属性值: ")
    to_node_label = input("请输入目标节点标签 (City/Company/Position): ")
    to_node_property = input("请输入目标节点属性 (name/title): ")
    to_node_value = input("请输入目标节点属性值: ")
    relationship_type = input("请输入关系类型 (HAS_COMPANY/HAS_POSITION): ")

    query = f"""
    MATCH (from_node:{from_node_label} {{{from_node_property}: $from_node_value}})-[r:{relationship_type}]->(to_node:{to_node_label} {{{to_node_property}: $to_node_value}})
    DELETE r
    """
    graph.run(query, from_node_value=from_node_value, to_node_value=to_node_value)
    print(f"关系 {from_node_label} ({from_node_property}: {from_node_value}) -> {to_node_label} ({to_node_property}: {to_node_value}) 删除完成！")

def insert_menu():
    while True:
        print("\n请选择插入操作：")
        print("1. 插入单个节点")
        print("2. 插入单个关系")
        print("3. 插入完整数据（城市、公司、职位）")
        print("0. 返回上一级菜单")
        
        choice = input("请输入选项: ")
        
        if choice == '1':
            insert_single_node()
        elif choice == '2':
            insert_single_relationship()
        elif choice == '3':
            insert_complete_data()
        elif choice == '0':
            break
        else:
            print("无效的选项，请重新输入！")

def update_menu():
    while True:
        print("\n请选择更新操作：")
        print("1. 更新节点")
        print("2. 更新关系")
        print("0. 返回上一级菜单")
        
        choice = input("请输入选项: ")
        
        if choice == '1':
            update_node()
        elif choice == '2':
            update_relationship()
        elif choice == '0':
            break
        else:
            print("无效的选项，请重新输入！")

def query_menu():
    while True:
        print("\n请选择查询操作：")
        print("1. 查询公司所在城市")
        print("2. 查询某个城市有多少公司")
        print("3. 查询公司有哪些职位")
        print("4. 查询单个节点")
        print("5. 查询关系")
        print("0. 返回上一级菜单")
        
        choice = input("请输入选项: ")
        
        if choice == '1':
            query_company_city()
        elif choice == '2':
            query_city_company_count()
        elif choice == '3':
            query_company_positions()
        elif choice == '4':
            query_single_node()
        elif choice == '5':
            query_relationship()
        elif choice == '0':
            break
        else:
            print("无效的选项，请重新输入！")

def delete_menu():
    while True:
        print("\n请选择删除操作：")
        print("1. 删除节点")
        print("2. 删除关系")
        print("0. 返回上一级菜单")
        
        choice = input("请输入选项: ")
        
        if choice == '1':
            delete_node()
        elif choice == '2':
            delete_relationship()
        elif choice == '0':
            break
        else:
            print("无效的选项，请重新输入！")

def main_menu():
    while True:
        print("\n请选择操作：")
        print("1. 插入数据")
        print("2. 更新数据")
        print("3. 查询数据")
        print("4. 删除数据")
        print("0. 退出")
        
        choice = input("请输入选项: ")
        
        if choice == '1':
            insert_menu()
        elif choice == '2':
            update_menu()
        elif choice == '3':
            query_menu()
        elif choice == '4':
            delete_menu()
        elif choice == '0':
            break
        else:
            print("无效的选项，请重新输入！")

if __name__ == "__main__":
    main_menu()