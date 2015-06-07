import requests
from bs4 import BeautifulSoup
import pandas as pd

header_url = "http://www.finviz.com/screener.ashx?v=111&r=1"

data_url = "http://www.finviz.com/screener.ashx?v=111&r="

url_start = 1
url_end = 7101
sym_per_page = 20


r = requests.get(header_url)

soup = BeautifulSoup(r.content)

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
data_columns.append(soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[7].text)
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

snum = 0

#info_index = []
#data_index = []

for i in sym_info_count:
    info_index = int(soup.find_all("td",{"align":"right","class":"body-table-nw"})[snum].contents[0])
    df_info[info_columns[1]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i].contents[0].contents[0]
    df_info[info_columns[1]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+1].contents[0]
    df_info[info_columns[2]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+2].contents[0]
    df_info[info_columns[3]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+3].contents[0]
    df_info[info_columns[4]].ix[info_index] = soup.find_all("td",{"align":"left","class":"body-table-nw"})[i+4].contents[0]
    snum +=6


for j in sym_data_count:
    data_index = int(soup.find_all("td",{"align":"right","class":"body-table-nw"})[j].contents[0])
    df_data[data_columns[0]].ix[data_index] = soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+1].contents[0]
    df_data[data_columns[1]].ix[data_index] = soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+2].contents[0]
    df_data[data_columns[2]].ix[data_index] = soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+3].contents[0].contents[0]
    df_data[data_columns[3]].ix[data_index] = soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+4].contents[0].contents[0]
    df_data[data_columns[4]].ix[data_index] = soup.find_all("td",{"align":"right","class":"body-table-nw"})[j+5].contents[0]

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
