import pandas as pd

def extract_eq_data(conn):
    r_type = input('Enter report type ("y" for yearly or "m" for monthly):')
    if r_type == 'm':        
        month = input('Enter Month number: ')
        year = input ('Enter Full Year: ')
        date = year+'-'+month
        #extract data from mysql and save it in a dataframe, setting date_time as index for a time series analysis
        sql = ''' SELECT mag, date_time, lat, lon, region
                  FROM v_all_eq_data
                  WHERE mag >= 1 AND DATE_FORMAT(date_time, "%Y-%m")="'''+date+'''"
                  ORDER BY date_time ASC;'''
    elif r_type == 'y':
        year = input ('Enter Full Year: ')
        date = year
        #extract data from mysql and save it in a dataframe, setting date_time as index for a time series analysis
        sql = ''' SELECT mag, date_time, lat, lon, region
                  FROM v_all_eq_data
                  WHERE mag >= 1 AND DATE_FORMAT(date_time, "%Y")="'''+date+'''"
                  ORDER BY date_time ASC;'''
    else:
        print('invalid report option, try again')
   ##add try except logic
      
    df = pd.read_sql_query(sql, conn, index_col='date_time', parse_dates= 'date_time' )
    #Changing date index to local timezone
    #localize current time zone
    df.index = df.index.tz_localize('America/Puerto_Rico')
    #removing timezone from date time
    df.index = df.index.tz_localize(None)

    return df

# https://stackoverflow.com/questions/8924173/how-do-i-print-bold-text-in-python

class font:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'