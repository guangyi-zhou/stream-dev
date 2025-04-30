import requests
import pandas as pd
from bs4 import BeautifulSoup
headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"}

for start_num in range(0, 250, 25):
    response = requests.get(f"https://movie.douban.com/top250?start={start_num}", headers=headers)
    # print(response.status_code)
    # 打印源码
    content = response.text
    # 解析htm源码
    soup = BeautifulSoup(content, "html.parser")
    # print(soup)
    # 解析出第一个元素
    all_title = soup.findAll("span", attrs={"class": "title"})
    for price in all_title:
        title_string = price.string
        if "/" not in title_string:
            print(title_string)

