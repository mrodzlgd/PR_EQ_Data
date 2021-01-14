#Importing libraries
import requests
from requests.exceptions import HTTPError
import json
import pandas as pd
import geopandas as gpd
import re
from shapely.geometry import Polygon
import sqlalchemy
from sqlalchemy import create_engine, exc
from sqlalchemy.exc import SQLAlchemyError
import pymysql
import getpass
import sys
import datetime
import time
import selenium
from selenium import webdriver
from selenium.webdriver.support.ui import Select


def get_usgs_data(url):
  #verify if request is good
    try:
        response = requests.get(url)
        # If the response was successful, no Exception will be raised and data will be extracted
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        print('Successful Request!')    
        print(url)
        #extract data
        req = requests.get(url)
        eq_data = req.json()
        #get record count
        rec_cnt = eq_data['metadata']['count']
        print('Record count = ', rec_cnt)
        if rec_cnt == 0:
            sys.exit('No new data to update available...')
        else:
            #save data we care about into a dataframe:
            eq_data = eq_data['features']
            eq_list = []  #initializing list of dictionaries
            for x in (list(range(0, rec_cnt))):            
                #getting event properties in a dictionary format
                eq_dict = {'id': eq_data[x]['id'] ,
                           'time': eq_data[x]['properties']['time'],
                           'updated': eq_data[x]['properties']['updated'],
                           'title': eq_data[x]['properties']['title'],
                           'mag': eq_data[x]['properties']['mag'], 
                           'magType': eq_data[x]['properties']['magType'],            
                           'lon': eq_data[x]['geometry']['coordinates'][0],
                           'lat': eq_data[x]['geometry']['coordinates'][1],
                           'depth': eq_data[x]['geometry']['coordinates'][2],
                           'sources': eq_data[x]['properties']['sources'],
                           'url': eq_data[x]['properties']['url'],
                           'region':None}
                eq_list.append(eq_dict)   
                #save data into dataframe
            df = pd.DataFrame(eq_list)
        return df

#helper function to scrap data from PRSN
def scrap_prsn(syear,smonth,sday,shour,smins,eyear,emonth,eday,ehour,emins):

    #define webdriver
    driver = webdriver.Firefox(executable_path = 'C:/Users/Melissa/Downloads/geckodriver-v0.27.0-win64/geckodriver.exe')
    #url to navigate
    driver.get('http://www.prsn.uprm.edu/Spanish/catalogue/index.php')

    #select options for report:

    #select start year
    select = Select(driver.find_element_by_id('sYear'))
    select.select_by_visible_text(syear)
    #select start month
    select = Select(driver.find_element_by_id('sMonth'))
    select.select_by_value(smonth)
    #select start day
    select = Select(driver.find_element_by_id('sDay'))
    select.select_by_visible_text(sday)
    #select start hour
    select = Select(driver.find_element_by_id('sHour'))
    select.select_by_visible_text(shour)
    #select start min
    select = Select(driver.find_element_by_id('sMin'))
    select.select_by_visible_text(smins)


    #select start year
    select = Select(driver.find_element_by_id('eYear'))
    select.select_by_visible_text(eyear)
    #select start month
    select = Select(driver.find_element_by_id('eMonth'))
    select.select_by_value(emonth)
    #select start day
    select = Select(driver.find_element_by_id('eDay'))
    select.select_by_visible_text(eday)
    #select start hour
    select = Select(driver.find_element_by_id('eHour'))
    select.select_by_visible_text(ehour)
    #select start min
    select = Select(driver.find_element_by_id('eMin'))
    select.select_by_visible_text(emins)

    # #set the coordinates

    # #input min latitude
    inputElement = driver.find_element_by_id("latMin")
    inputElement.clear()
    inputElement.send_keys('17.00')

    # #input max latitude
    inputElement = driver.find_element_by_id("latMax")
    inputElement.clear()
    inputElement.send_keys('20.00')

    # #input min longitude
    inputElement = driver.find_element_by_id("lonMin")
    inputElement.clear()
    inputElement.send_keys('-69.00')

    # #input max longitude
    inputElement = driver.find_element_by_id("lonMax")
    inputElement.clear()
    inputElement.send_keys('-63.50')

    # click submit button
    submit_button = driver.find_elements_by_xpath('//*[@id="submit"]')[0]
    submit_button.click()

    # Get number of rows for a web table in Selenium

    num_rows = len (driver.find_elements_by_xpath("/html/body/div[1]/table/tbody/tr[3]/td/table/tbody/tr/td/div/table[1]/tbody/tr"))
    print('Number of records:',num_rows-1)
    # Get number of columns for a web table in Selenium

    num_cols = len (driver.find_elements_by_xpath("/html/body/div[1]/table/tbody/tr[3]/td/table/tbody/tr/td/div/table[1]/tbody/tr[2]/td"))


    before_XPath_1 = "/html/body/div[1]/table/tbody/tr[3]/td/table/tbody/tr/td/div/table[1]/tbody/tr[1]/td["
    before_XPath_2 = "/html/body/div[1]/table/tbody/tr[3]/td/table/tbody/tr/td/div/table[1]/tbody/tr["
    after_XPath = "]"
    after_XPath_1 = "/td["

    data = []
    all_data = []

    for r in range(2, (num_rows + 1)):
        data=[]
        for t_col in range(1, (num_cols + 1)):
            FinalXPath = before_XPath_2 + str(r) + after_XPath + after_XPath_1 + str(t_col) + after_XPath
            cell_text = driver.find_element_by_xpath(FinalXPath).text
            data.append(cell_text)
        all_data.append(data)

    cols = []
    for t_col in range(1, (num_cols + 1)):
        FinalXPath = before_XPath_1 + str(t_col) + after_XPath
        cell_text = driver.find_element_by_xpath(FinalXPath).text
        cols.append(cell_text)
    df = pd.DataFrame(all_data, columns = cols )
    driver.close()
    return df

    
#The following function was created to clean and transform the extracted data:
def clean_data(df, source):
    #clean data for usgs source
    if source == 'USGS':
        #update time from unix timestamps to datetime
        df['date_time'] = pd.to_datetime(pd.to_datetime(df.time, unit='ms',origin='unix'
                                          ).apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S")))
        df['updated_datetime'] = pd.to_datetime(pd.to_datetime(df.updated, unit='ms',origin='unix'
                                                 ).apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S")))
        #magtype normalizing
        df.magType = df.magType.str.lower()
        df.mag = pd.to_numeric(df.mag)
        #keep events > 0
        df = df.loc[df.mag>0]
        #removing unnecesary columns
        df.drop(columns = ['time','updated'], inplace = True)
    #clean data for prsn source
    elif source =='PRSN':
        #rename columns for consistency
        df.rename(columns = {'UTC Time': 'date_time', 'Mag':'mag','Lat':'lat','Long':'lon', 'Depth':'depth', 'Network':'sources'}, inplace = True)
        #create and modify columns
        df['magType'] = df.mag.apply(lambda x: x.split()[1])
        df['mag'] =pd.to_numeric(df.mag.apply(lambda x: x.split()[0]))
        df['id']= df['date_time'].apply(lambda x: re.sub(r'[^\w\s\']', '', str(x)).replace(' ',''))
        #keep columns we care about
        df = df[['id','date_time', 'lat', 'lon', 'depth', 'mag', 'magType','sources']].copy()
        #update datatypes
        df.lat = pd.to_numeric(df.lat)
        df.lon = pd.to_numeric(df.lon)
        df.depth = pd.to_numeric(df.depth)
        df.date_time = pd.to_datetime(df.date_time)
        #keep events > 0
        df = df.loc[df.mag>0]
        #remove duplicate events
        df.drop_duplicates(inplace=True,  ignore_index = True)    
    #changing to a GeoDataFrame to create geometry series
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon,df.lat))
    return gdf    

# Function for database connection
def connect_to_db(db,user):
    try:
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user=user,
                           pw= getpass.getpass('Enter database password:'),
                           db=db))
        conn = engine.connect()
        print("Successfull connection to MySQL database")
    except exc.DBAPIError as err:
        print("Connection error to database")
        raise
    return engine.connect()

#Helper function to load data into the MySQL table, can be used for both USGS and PRSN data 

def load_to_db(df, conn, source):
    #parameters if loading PR Seismic Network data
    if source=='PRSN':
        stage_table='prsn_data_stage'
        final_table='prsn_data'
        delete_sql= '''DELETE FROM ''' + final_table + ''' WHERE id IN ( SELECT * FROM
                    (SELECT s.id FROM ''' + stage_table + ''' s
                    INNER JOIN ''' + final_table + ''' d ON s.id=d.id
                    ) AS t); '''
        update_sql= '''INSERT INTO ''' + final_table + ''' ( id, date_time, mag, magType, sources,  
                  depth, lat, lon, region) SELECT id, date_time, mag, magType, sources,  
                  depth, lat, lon, region FROM ''' + stage_table + ''' where id NOT IN (SELECT id FROM ''' + final_table + ''');'''
    #parameters if loading USGS data
    elif source=='USGS':
        stage_table='eq_stage'
        final_table='eq_data'
        delete_sql='''DELETE FROM ''' + final_table + ''' WHERE id IN ( SELECT * FROM
                    (SELECT s.id FROM ''' + stage_table + ''' s
                    INNER JOIN ''' + final_table + ''' d ON s.id=d.id
                    WHERE s.updated_datetime<>d.updated_datetime
                    ) AS t); '''
        update_sql= '''INSERT INTO ''' + final_table + ''' ( id, title, mag, magType, sources, url, date_time, updated_datetime, 
                  depth, lat, lon, region) SELECT id, title, mag, magType, sources, url, date_time, updated_datetime, 
                  depth, lat, lon, region FROM ''' + stage_table + ''' where id NOT IN (SELECT id FROM ''' + final_table + ''');'''
   

    #creating table to stage eq_data
    df.to_sql(stage_table, con = conn, if_exists = 'replace', chunksize = 1000)
    #get counts before changes
    s_count = conn.execute('''SELECT COUNT(id) FROM '''+ final_table).fetchone()[0]
    #remove records from eq_data table that exists in eq_stage table
    conn.execute (delete_sql)
    r_count = conn.execute('''SELECT COUNT(id) FROM ''' + final_table + ''';''').fetchone()[0]
    #load new and updated records into eq_data from eq_stage data
    conn.execute(update_sql)
    e_count = conn.execute('''SELECT COUNT(id) FROM ''' + final_table + ''';''').fetchone()[0]
    delta = e_count-s_count
    if delta == 0:
        print('No records to update')
    else:
        print('Records before load:', s_count)
        print('Updated records:', delta)
        print('Final record count:', e_count)
        
#Helper function that will retrieve from MySQL the region polygon data to be used to identify the region for each event
def get_regions(conn):
    data = conn.execute(''' select name, geometry from pr_regions;''').fetchall()
    #Clean geometry series to transform dataframe to a geoDataFrame
    r = pd.DataFrame(data,columns=['Name','geometry'] )
    r.geometry = r.geometry.apply(lambda x: list(eval(x.replace('POLYGON((', '('
                                                   ).replace(' ))', ')'
                                                            ).replace( ' , ', '),('
                                                                     ).replace(' ',','))))
    r.geometry = r.geometry.apply(lambda x: Polygon(x))
    regions = gpd.GeoDataFrame(r, crs="EPSG:4326")
    return regions

#Helper function that will perform the PIP analysis and will assign a region for each event
def get_intersections (gdf,conn):
    regions = get_regions(conn)
    r_list = list(regions.Name)
    #create empty dataframe
    df = pd.DataFrame().reindex_like(gdf).dropna()
    for r in r_list:
        #get geometry for specific region
        pol = (regions.loc[regions.Name==r])
        pol.reset_index(drop = True, inplace = True)
        #identify those records from gdf that are intersecting with the region polygon
        pip_mask = gdf.within(pol.loc[0, 'geometry'])
        #filter gdf to keep only the intersecting records
        pip_data = gdf.loc[pip_mask].copy()
        #create a new column and assign the region name as the value
        pip_data['region']= r
        #append region data to empty dataframe
        df = df.append(pip_data)
    print('Validating region assignment...')
    if df.loc[df.id.duplicated() == True].shape[0] > 0:
        print("There are id's with more than one region")
    elif gdf.loc[~gdf.id.isin(df.id)].shape[0] > 0:
        print("There are id's without an assigned region")
    else:
        print("No issues with region assignment!")
    df.reset_index(inplace=True, drop=True)
    df = df.drop(columns='geometry')
    return df

#Helper function to use to load new and reviewed events for USGS

def load_delta_data(conn, source):
    if source == 'USGS':
        print("\n\nDelta load for USGS data...\n")
        minlat = '17'
        maxlat = '20'
        minlon = '-69'
        maxlon = '-63.5'
        ud = conn.execute('''Select max(updated_datetime) from eq_data''').fetchone()[0]
        ud = ud - datetime.timedelta(7)
        ud = ud.strftime('%Y-%m-%dT%H:%M:%S')
        #define url for request with updatedafter
        url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minlatitude='+ minlat +'&maxlatitude='+ maxlat + '&minlongitude=' + minlon + '&maxlongitude=' + maxlon + '&updatedafter=' + ud
        df = get_usgs_data(url)
    
    elif source == 'PRSN':
        print("\n\nDelta load for PRSN data...\n")
        #set parameters for start date
        last_date = str(conn.execute('''Select max(p.date_time) from prsn_data p;''').fetchone()[0])
        syear = str(last_date[:4])
        smonth = str(int(last_date[5:7]))
        sday = str(int(last_date[8:10]))
        shour = str(int(last_date[11:13]))
        smins = str(int(last_date[14:16]))
        print('Start date = ', last_date)

        #set parameters for end datetime...today at 23:59
        end_date = str(datetime.datetime.fromtimestamp(time.time()))
        eyear = str(end_date[:4])
        emonth = str(int(end_date[5:7]))
        eday = str(int(end_date[8:10]))
        ehour = '23'
        emins = '59'
        print('End date = ', end_date)
        df=scrap_prsn(syear,smonth,sday,shour,smins,eyear,emonth,eday,ehour,emins)
    #clean data & change to gdf
    eq_df = clean_data(df, source)
    #run intersection
    print('Assigning regions...')
    gdf = get_intersections (eq_df,conn)
    #load data into MySQL
    print('Loading data into MySQL...')
    load_to_db(gdf, conn, source)