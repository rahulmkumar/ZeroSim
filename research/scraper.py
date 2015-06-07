import requests
from bs4 import BeautifulSoup

url = "http://www.finviz.com/screener.ashx?v=111&r=1"
url_start = 1
url_end = 7101


r = requests.get(url)

soup = BeautifulSoup(r.content)

#header = soup.find_all("tr",{"align" :"center"})

# This gets the header items
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[0].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[1].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[2].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[3].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[4].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[5].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[6].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[7].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[8].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[9].text
print soup.find_all("tr",{"align" : "center"})[0].find_all("td",{"style" : "cursor:pointer;"})[10].text

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


print soup.find_all("td",{"align":"right","class":"body-table-nw"})[0].contents[0]
print soup.find_all("td",{"align":"left","class":"body-table-nw"})[0].contents[0].contents[0]
print soup.find_all("td",{"align":"left","class":"body-table-nw"})[1].contents[0]
print soup.find_all("td",{"align":"left","class":"body-table-nw"})[2].contents[0]
print soup.find_all("td",{"align":"left","class":"body-table-nw"})[3].contents[0]
print soup.find_all("td",{"align":"left","class":"body-table-nw"})[4].contents[0]
print soup.find_all("td",{"align":"right","class":"body-table-nw"})[1].contents[0]
print soup.find_all("td",{"align":"right","class":"body-table-nw"})[2].contents[0]
print soup.find_all("td",{"align":"right","class":"body-table-nw"})[3].contents[0].contents[0]
print soup.find_all("td",{"align":"right","class":"body-table-nw"})[4].contents[0].contents[0]
print soup.find_all("td",{"align":"right","class":"body-table-nw"})[5].contents[0]


for link in links:
    if "http" in link.get("href"): # use try/except
        print "<a href='%s'>%s</a>" %(link.get("href"),link.text)

g_data = soup.find_all("div",{"class": "info"})

for item in g_data:
    print item.text
    print item.contents[0].text
    print item.contents[0].find_all("a",{"class": "business-name"})

