import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

header_url = "http://www.finviz.com/screener.ashx?v=111&r=1"

data_url = "http://www.finviz.com/screener.ashx?v=111&r="

url_start = 1
url_end = 7141
sym_per_page = 20

pages = range(url_start,url_end,20)

def scrape_page(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    return soup

soup = scrape_page(header_url)

#header = soup.find_all("tr",{"align" :"center"})

# This gets the header items
# information columns will store: Ticker, Company, Sector, Industry and Country
info_columns = []

# Data columns will store: Ticker, Market Cap, P/E, Price, Change and Volume
data_columns = []

#find total number of stocks
total_stocks = int(str(soup.find_all("td",{"class" : "count-text"})[0].contents[1]).split(' ')[0])

index = range(0,total_stocks)



#info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[0].text)
info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[1].text)
info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[2].text)
info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[3].text)
info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[4].text)
info_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[5].text)

#print info_columns


#data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[1].text)
data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[6].text)
#data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[7].text)
data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[8].text)
data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[9].text)
data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[10].text)

#print data_columns

# first row returns the No. This can become a temporary index in a dataframe
#Ignore the No.
#print soup.find_all("td",{"align":"right","class":"body-table-nw"})[0].contents[0]

# create dataframes
df_info = pd.DataFrame(index = index, columns = info_columns)
df_data = pd.DataFrame(index = index, columns = data_columns)

sym_info_count = range(0,100,5)
sym_data_count = range(0,115,6)

for page in pages[0:3]:
    fetch_url = data_url + str(page)
    print fetch_url

    #fetch_url = data_url + str(21)
    soup = scrape_page(fetch_url)

    snum = 0

    for i in sym_info_count:
        try:
            info_index = int(soup.find_all("td",{"align":"right","class":"body-table-nw"})[snum].contents[0])-1

            #print 'num:'+str(snum)
            #print 'info_index:'+str(info_index)
            df_info[info_columns[0]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i].contents[0].contents[0]
            df_info[info_columns[1]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+1].contents[0]
            df_info[info_columns[2]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+2].contents[0]
            df_info[info_columns[3]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+3].contents[0]
            df_info[info_columns[4]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+4].contents[0]
        except:
            print 'Issue with Info count for loop'
            pass
        snum +=6


    for j in sym_data_count:
        try:
            data_index = int(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j].contents[0])-1
            #print 'j:'+str(j)
            #print 'data_index:'+str(data_index)
            #print data_index
            if str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0]).endswith("B"):
                df_data[data_columns[0]].ix[data_index] = float(str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0]).replace('B',''))*1000
            elif soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0] == '-':
                df_data[data_columns[0]].ix[data_index] = 0
            else:
                df_data[data_columns[0]].ix[data_index] = str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0]).replace('M','')
            #df_data[data_columns[1]].ix[data_index] = soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+2].contents[0]
            df_data[data_columns[1]].ix[data_index] = soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+3].contents[0].contents[0]
            df_data[data_columns[2]].ix[data_index] = float(str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+4].contents[0].contents[0]).replace('%',''))
            df_data[data_columns[3]].ix[data_index] = long(str(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+5].contents[0]).replace(',',''))
        except:
            pass

    wait_seconds = random.randint(5,30)
    time.sleep(wait_seconds)
    print 'waiting for:' + str(wait_seconds)







df_info.to_csv('df_info.csv')
df_data.to_csv('df_data.csv')




'''
# Alternate way of getting header
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[1].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[3].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[5].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[7].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[9].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[11].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[13].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[15].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[17].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[19].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[21].contents[0]


print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "right"})[0].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "left"})[0].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "left"})[1].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "left"})[2].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "left"})[3].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "left"})[4].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "right"})[1].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "right"})[2].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "right"})[3].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "right"})[4].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"align" : "right"})[5].text


print soup.find_all("tr",{"align" : "center"})[0].contents[1].contents[0]
print soup.find_all("tr",{"align" : "center"})[0].contents[3].contents[0].contents[0]
print soup.find_all("tr",{"align" : "center"})[0].contents[5].contents[0]
print soup.find_all("tr",{"align" : "center"})[0].contents[7].contents[0]



# Alternate way of getting details

print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[1].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[2].contents[0].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[3].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[4].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[5].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[6].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[7].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[8].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[9].contents[0].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[10].contents[0].contents[0]
print soup.contents[2].contents[3].contents[29].contents[1].contents[1].contents[1].contents[23].contents[11].contents[0]

# Alternate way of getting details
print soup.find_all("td",{"align":"right"})[3].contents[0]
print soup.find_all("td",{"align":"left"})[8].contents[0].contents[0]
print soup.find_all("td",{"align":"left"})[9].contents[0]
print soup.find_all("td",{"align":"left"})[10].contents[0]
print soup.find_all("td",{"align":"left"})[11].contents[0]
print soup.find_all("td",{"align":"left"})[12].contents[0]
print soup.find_all("td",{"align":"right"})[4].contents[0]
print soup.find_all("td",{"align":"right"})[5].contents[0]
print soup.find_all("td",{"align":"right"})[6].contents[0].contents[0]
print soup.find_all("td",{"align":"right"})[7].contents[0].contents[0]
print soup.find_all("td",{"align":"right"})[8].contents[0]
'''
