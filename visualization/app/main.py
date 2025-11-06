from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
import os
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# 新增：允许来自所有来源的跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源
    allow_credentials=True,
    allow_methods=["*"],    # 允许所有方法
    allow_headers=["*"],   # 允许所有头部
)

def read_file_data(file_path: str):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            lines = file.readlines()
            return [line.strip() for line in lines]
    return []

@app.get("/EducationWordCount")
def read_education_word_count():
    file_path = "mr/EducationWordCount/part-r-00000"
    data = read_file_data(file_path)
    # 解析: 第一列是学历, 第二列是数量
    data = [{"education": line.split("\t")[0], "count": int(line.split("\t")[1])} for line in data]
    return JSONResponse(content=data)

@app.get("/JobCityTop")
def read_job_company_size():
    file_path = "mr/JobCityTop/part-r-00000"
    data = read_file_data(file_path)
    # 解析: 第一列是岗位, 第二列是城市, 第三列是数量
    data = [{"job": line.split("\t")[0], "city": line.split("\t")[1], "count": int(line.split("\t")[2])} for line in data]
    return JSONResponse(content=data)

@app.get("/JobCitySalaryTop")
def read_job_education():
    file_path = "mr/JobCitySalaryTop/part-r-00000"
    data = read_file_data(file_path)
    # 解析: 第一列是岗位, 第二列是城市, 第三列是薪资
    data = [{"job": line.split("\t")[0], "city": line.split("\t")[1], "salary": float(line.split("\t")[2])} for line in data]
    return JSONResponse(content=data)

@app.get("/CityHighSalaryJobsCount")
def read_job_experience():
    file_path = "mr/CityHighSalaryJobsCount/part-r-00000"
    data = read_file_data(file_path)
    # 解析: 第一列是城市, 第二列是数量
    data = [{"city": line.split("\t")[0], "count": int(line.split("\t")[1])} for line in data]
    return JSONResponse(content=data)

@app.get("/JobEducationExperienceSalaryAvg")
def read_job_salary():
    file_path = "mr/JobEducationExperienceSalaryAvg/part-r-00000"
    data = read_file_data(file_path)
    # 解析: 第一列是岗位, 第二列是学历, 第三列是经验, 第四列是平均薪资
    data = [{"job": line.split("\t")[0], "education": line.split("\t")[1], "experience": line.split("\t")[2], "salary": float(line.split("\t")[3])} for line in data]
    return JSONResponse(content=data)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)