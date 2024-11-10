import pandas as pd
import sqlite3
import re
import os
import sys  

def get(url : str):
    data = pd.read_csv(url)
    data  = pd.DataFrame(data)

    return data


def save( savingPath : str, sqliteFileName : str, sqliteTableName : str, data : pd.DataFrame):      
    conn = sqlite3.connect(savingPath+sqliteFileName)
           
    data.to_sql(sqliteTableName, conn, if_exists='replace', index=False)

def main():
    url1="https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"
    data1=get(url1)
    save("../data/", "crimedata.sqlite", "crimes", data1)
    
    url2="https://data.wa.gov/api/views/wf4i-evff/rows.csv?accessType=DOWNLOAD"
    data2=get(url2)
    save("../data/", "drugdata.sqlite", "drug", data2)
    


if __name__ == "__main__":
    main()
            
        
            