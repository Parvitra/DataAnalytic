from bs4 import BeautifulSoup
import re
import lxml
from selenium import webdriver
import pymysql

url_start = "https://www.thestar.com.my/business/marketwatch/stocks/?qcounter="
driver = webdriver.Firefox()

def name_cat(url):

    urls = []
    driver.get(url)
    src = driver.execute_script("return document.documentElement.outerHTML")
    soup = BeautifulSoup(src,"lxml")
    companies_urls_list = soup.find("div", {"class": "btn-group btn-group-sm"}).find_all("a", {"class": "btn btn-default"})
    for link in companies_urls_list[25:26]:
        urls.extend(companies_urls(link.get("href")))


    print(urls)

    return urls

def companies_urls(url):

    url_list = []
    driver.get(url)
    src = driver.execute_script("return document.documentElement.outerHTML")
    soup = BeautifulSoup(src, "lxml")
    company_url_list = soup.find("table", {"class": "market-trans"}).find_all("a", href=re.compile("(.)*$"))
    for link in company_url_list:
        href = "https://www.thestar.com.my" + link.get("href")
        print(href)
        url_list.append(href)
    return url_list


def data_list(urls):
    data_list = []
    for url in urls:
        for _ in url:
            company_name, stock_code, raw_data = company_data(_)
            data_list.append((company_name, stock_code, raw_data))
    print(data_list)

    return data_list

def company_data(urls):
    raw_data = []
    driver.get(urls)
    src = driver.execute_script("return document.documentElement.outerHTML")
    soup = BeautifulSoup(src, "lxml")

    try:
        data_list = soup.find("table", {"class": "market-trans bot-15"}).find("tbody").findAll("td")
        for _ in data_list:
            raw_data.append(_.get_text())
        company_name = soup.find("h1", {"class": "stock-profile f16"}).get_text()
        com_stock_code = soup.find("ul", {"class": "stock-code"}).find_all("li", {"class": "f14"})
        stock_code = com_stock_code[1].contents[1].strip(" :")
        print(company_name, stock_code, raw_data)
    except ValueError:
        company_name = soup.find("h1", {"class": "stock-profile f16"}).get_text()
        com_stock_code = soup.find("ul", {"class": "stock-code"}).find_all("li", {"class": "f14"})
        stock_code = com_stock_code[1].contents[1].strip(" :")
        print(company_name, stock_code, raw_data)
    return company_name, stock_code, raw_data

#if __name__ == "__main__":
#
#     company_url_list = name_cat(url_start)
#     actual_data = data_list(company_url_list)
#     with open("Dataset.csv",mode="a") as csv_file:
#         for _ in actual_data:
#             writer = csv.writer(csv_file)
#             writer.writerow(_)

def data(stock_list):
    data_ls = []
    for url in stock_list:
        data_list1, company_name, stock_code = company_data(url)
        data_ls.append((stock_code, company_name, data_list1))
    return data_ls

def stockcrawl (datalist):

        mydb = pymysql.connect(host="localhost", user="root", password="password", database="mysql")

        cur = mydb.cursor()

        try:
            cur.execute("Use StockCrawl")
            sql = "INSERT INTO StockData (CompanyName, CompanyStockCode, OpenPrice, HighPrice, LowPrice, LastPrice, Chg, " \
            "ChgPercentage, Volume, BuyVolume,SellVolume) " \
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            for ind, cmp in enumerate(datalist):
                try:
                    cur.execute(sql,(cmp[2], cmp[1], cmp[0][0], cmp[0][1], cmp[0][2], cmp[0][3],cmp[0][4], cmp[0][5], cmp[0][6], cmp[0][7],cmp[0][8]))
                except ValueError:
                    continue
                cur.connection.commit()
        finally:
            cur.close()
            mydb.close()
            print("MySQL connection is closed")


          # for item in datalist:
          #     for cmp in item:
          #         try:
          #            cur.execute(sql,
          #                   (cmp[2], cmp[1], cmp[0][0], cmp[0][1], cmp[0][2], cmp[0][3],cmp[0][4], cmp[0][5], cmp[0][6], cmp[0][7],cmp[0][8]))

        # insert_tuple = (CompanyName, StockCode, OpenPrice, HighPrice, LastPrice, Chg, ChgPercent, Volume, BuyVolume,
        #                 SellVolume)
        # result = mycursor.executemany(sql, insert_tuple)
        #
        # mydb.commit()
        # print("Record inserted successfully into database")
        #
        #        except ValueError:
        #              continue
        #           cur.connection.commit()
        #
        # finally:
        # #closing database connection.
        #     cur.close()
        #     mydb.close()
        #     print("MySQL connection is closed")

#if __name__ == "__main__":

url_list = name_cat(url_start)
stock_data = data(url_list)
print(stock_data)
stockcrawl(stock_data)
