import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# URL = "https://www.alkosto.com/audifonos-redmi-inalambricos-bluetooth-in-ear-buds-essential-negro/p/6934177799792"

# print(page.text)
# print(a)



def get_price_from_div_price_alkosto(text):
    # print('*',text.split()[0][1:].replace('.',''))

    return text.split()[0][1:].replace('.','')
def get_price_product_from(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    a = soup.find(class_="product-main-info")
    price = a.find(class_="price-alkosto").text
    price = get_price_from_div_price_alkosto(price)
    # print(price)
    return price

def get_prices(df_urls):
    new_data = []
    for index, row in df_urls.iterrows():
        # print(row['url'])
        new_data.append({"url":row['url'],"price":get_price_product_from(row['url']),"created_on":datetime.now().isoformat()})
    new_data = pd.DataFrame.from_dict(new_data)
    return new_data

def get_data():

    data = pd.read_csv('./scripts/urls_alkosto.csv')
    # print(data)
    new_data = get_prices(data)
    print(new_data)

    old_data = pd.read_csv('./scripts/out_alkosto.csv')
    old_data = pd.concat([old_data,new_data])
    old_data.to_csv('./scripts/out_alkosto.csv',index=False)


    # for url in urls:

    #     get_price_product_from(url)

if __name__ == "__main__":
    get_data()