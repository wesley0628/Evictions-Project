from tqdm import tqdm
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

errorCount = 0
errorList = []
nameList = []
addressList = []
aldermanicList = []
skipList = []
parcelList = []

def webScrape(parcel):
    x = requests.get('https://www.cityofmadison.com/assessor/property/propertydata.cfm?ParcelN={}'.format(parcel))
    soup = BeautifulSoup(x.content, 'html.parser')
    find_el = soup.find_all('div', class_="clearfix")
    companyEnd = find_el[1].text.find('\r\n\t\t\t\r\n')
    company = find_el[1].text[37:companyEnd]
    company = company.strip()
    companyAddressStart = find_el[1].text.find('\n\r\n\t\t\t')

    address = find_el[1].text[companyAddressStart+5:-10]
    address = address.strip()
    
    aldermanic = find_el[4].text[42:44]
    aldermanic = aldermanic.strip()
    
    return company, address, aldermanic

if __name__ == '__main__':
    table = pd.read_csv("cleardata.csv")
    
    for i, j in zip(tqdm (range (len(table['Parcel'])), desc="Loading...", ascii=False, ncols=80), table['Parcel']):
        try:
            name, address, aldermanic = webScrape('0' + str(j))
#             print(len(address))
            if len(address) >= 50:
                skipList.append(j)
                continue
            nameList.append(name)
            addressList.append(address)
            aldermanicList.append(aldermanic)
            parcelList.append(j)
            time.sleep(0.1)
        except:
            errorCount += 1
            errorList.append(j)
#     print(nameSeries)
    data = {"Parcel": pd.Series(parcelList),
            "Name": pd.Series(nameList),
            "Address": pd.Series(addressList),
            "Aldermanic District": pd.Series(aldermanicList)}
    
    df = pd.concat(data, axis = 1)
    df.to_csv('table.csv', index = False)
    if errorCount != 0:
        pd.Series(errorList).to_csv('errorList.csv', index = False)
    pd.Series(skipList).to_csv('skipList.csv', index = False)
    print('Skip counts:', len(skipList))
    print('Error counts:', errorCount)
    