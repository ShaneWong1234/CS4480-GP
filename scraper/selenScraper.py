import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException 
from gazpacho import get, Soup, soup
import  pandas as pd
import time
import ssl
import os

baseURL = 'https://www.reuters.com'
url = 'https://www.reuters.com/lifestyle/wealth/'

options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')
options.add_argument('--ignore-ssl-errors=yes')

driver = webdriver.Chrome(chrome_options=options)
driver.get(url)
stoptime = time.time() + 60*3

while True:
    try:
        time.sleep(3)
        more = driver.find_element_by_xpath('.//*[@id="main-content"]/div[4]/div/div[1]/div/div[2]/button')
        more.click()
        if time.time() > stoptime:
            break
    except NoSuchElementException:
        break

pageForScrape = driver.page_source
driver.close()

soup1 = Soup(pageForScrape)
soup2 = Soup(pageForScrape)
soup3 = Soup(pageForScrape)
BigHeadings = soup1.find('h3', {'class': 'MediaStoryCard'}, partial=True)
SmallHeadings = soup2.find('h6', {'class': 'MediaStoryCard'}, partial=True)
links = soup3.find('a', {'class': 'MediaStoryCard'}, partial=True)

list_of_Headings = []
list_of_links = []
list_of_page_Content = []
list_of_categories = []

list_of_Headings.append(BigHeadings.text)
for i in SmallHeadings:
    content = i.text
    list_of_Headings.append(content)

for i in links:
    attributes = i.attrs
    href = attributes['href']
    list_of_links.append(href)
    categories = href.split('/')
    list_of_categories.append(categories[2])



for i in list_of_links:
    time.sleep(3)
    url2 = baseURL + i
    driver = webdriver.Chrome(chrome_options=options)
    driver.get(url2)
    pageForScrape = driver.page_source
    driver.close()

    soup4 = Soup(pageForScrape)
    pageContent = soup4.find('p', {'class': 'Body__large'}, partial=True)
    body = ''
    for j in pageContent:
        body = body + ' ' + j.text
        
    list_of_page_Content.append(body)

dict = {'Categories': list_of_categories, 'Heading': list_of_Headings, 'Content': list_of_page_Content}

df = pd.DataFrame.from_dict(dict, orient='index')
df = df.transpose()
path=r'C:\Users\Administrator\Desktop\study\4480\prj'
df.to_csv(os.path.join(path, r'dataset6.csv'))
