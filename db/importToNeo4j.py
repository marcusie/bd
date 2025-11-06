import pandas as pd
from py2neo import Graph, Node, Relationship

# 连接到Neo4j数据库
graph = Graph("bolt://localhost:7687", auth=("neo4j", "512436lkl"))

# 读取CSV文件
csv_file_path = 'recruit.csv'
df = pd.read_csv(csv_file_path)

# 创建唯一约束以确保节点的唯一性
graph.run("CREATE CONSTRAINT ON (city:City) ASSERT city.name IS UNIQUE")
graph.run("CREATE CONSTRAINT ON (company:Company) ASSERT company.name IS UNIQUE")
graph.run("CREATE CONSTRAINT ON (position:Position) ASSERT position.title IS UNIQUE")

# 处理每一行数据
for index, row in df.iterrows():
    city_name = row['城市']
    company_name = row['公司名称']
    position_title = row['职位']

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

print("数据导入完成！")