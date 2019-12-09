from bs4 import BeautifulSoup
import requests
import pymysql.cursors

def get_url(links):

    source = requests.get(links[0]).text
    soup = BeautifulSoup(source, "lxml")
    newslinks = soup.find_all("div", {"class": "views-field views-field-title"})
    Company_Name = links[1]

    for _ in newslinks:
        News_Header = _.find("a").get_text()
        link = _.find("a").attrs["href"]
        get_data(Company_Name,News_Header,link)


def get_data(Company_Name,News_Header,link):
    News_Info = ""
    News_Link = "https://www.theedgemarkets.com" + link
    print(News_Link)
    try:
         source = requests.get(News_Link).text
         soup = BeautifulSoup(source, "lxml")
         news = soup.find("div",{"property": "content:encoded"}).find_all("p")
         for _ in news:
             News_Info += _.getText()
         Stock_Date = soup.find("span", {"class": "post-created"}).getText()
         store(Stock_Date,Company_Name, News_Header,News_Info,News_Link)
    except:
        print("Something went Wrong in get_data function")


def store(StockDate,CompanyName,NewsHeader,NewsInfo,NewsLink):


    conn = pymysql.connect(host="localhost", user="parvi", passwd="parvitra", db="mysql")
    cur = conn.cursor()
    try:
        cur.execute("USE StockNews")
        sql_query = "INSERT INTO NewsData (StockDate,CompanyName,NewsHeader,NewsInfo,NewsLink)" \
                    " VALUES (%s,%s,%s,%s,%s)"
        try:
            cur.execute(sql_query,(StockDate,CompanyName,NewsHeader,NewsInfo,NewsLink))

        except ValueError:
                print("The is no Data to Store")
        cur.connection.commit()

    finally:
        cur.close()
        conn.close()


Company_News_Link = []
company_names = ["HLBANK","MAHSING","MAGNUM","KPJ","PARKSON","POS","YTL","PADINI","AMWAY","TASEK"]


for news in company_names:
    for i in range (0,3):
        Company_News_Link.append(["https://www.theedgemarkets.com/search-results?page={0}&keywords={1}".format(i,news), news])
for news in Company_News_Link:
    get_url(news)