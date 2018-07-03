# -*- coding: utf-8 -*-
"""
Pushes data to mysql from xls and csv files
Working for pvspot, pnpc, parmalat and weather sa

@author: TeaganO
"""
#----------------------------------- imports ----------------------------------
#for looping through files in directory
import os
import zipfile

#for working with the files
import pandas

#for interacting with mysql
from sqlalchemy import create_engine

#for printing progress
import sys


#------------------------------------------------------------------------------
#------------------- data_frame manipulation functions ------------------------

#------------------------- pvspot function ------------------------------------

def manip_pvspot(item,data_frame):
    print("\nFound pvspot...")
    name_frame=data_frame.loc[data_frame['pvSpot REPORT'] == 'Name:']       #find the site name row in the df - make the row into its own df
    name=name_frame.iat[0,1]                                                #pull the site name from this df
    
    #read in all the sheets of the file - returns a dictionary
    data_frame_dict = pandas.read_excel(item,sheet_name=None)
    if len(data_frame_dict.keys()) == 2:     #if there are only two sheets, set the desired sheet as the Data sheet
        sheet_to_pull='Data'
    elif len(data_frame_dict.keys()) == 3:   #if there are three, set it as the Data daily sheet
        sheet_to_pull='Data daily'
        
    #read in the desired sheet
    data_frame = pandas.read_excel(item,  sheet_name=sheet_to_pull, index_col=0,na_values=['-',' - '])   #reset data_frame to the second (data) sheet of the excel file, with the first column as the index
    data_frame.dropna(thresh=2,inplace=True)                                         #drop the empty rows of the df - to drop text at bottom
    
    #drop columns that are not necessary
    for col in data_frame.iteritems():
        if not ("e_low" in str(col[0]).lower() or "e_best" in str(col[0]).lower() or "e_high" in str(col[0]).lower()):
            data_frame.drop(columns=col[0],inplace=True)
    
    data_frame['site']=name                                                 #create a column containing the site name
    data_frame.index=pandas.to_datetime(data_frame.index,dayfirst=True)     #typecast the index as datetime
    return data_frame
    


#--------------------------- weather sa functions -----------------------------

#for correctly indexing the data frame
name_col=None
date_col=None
#find the column numbers of the name and date columns
def find_name_date_cols(data_frame):
    #find column number containing name column 
    for col in data_frame.iteritems():
        if 'name' in str(col[0]).lower(): 
            global name_col
            name_col=data_frame.columns.get_loc(col[0])    
    
    #find column number containing date column 
    for col in data_frame.iteritems():
        if str(col[0]).lower() == 'date' or str(col[0]).lower() == 'datet': 
            global date_col
            date_col=data_frame.columns.get_loc(col[0])

#actual df manipulation
def manip_weather_sa(item, data_frame):
    print("\nFound weather_sa...")
    
    #if the file had blank leading rows - set headings correctly
    if 'unnamed' in str(data_frame.columns[0]).lower() or 'weatherline' in str(data_frame.columns[0]).lower():
        data_frame.rename(columns=data_frame.iloc[0],inplace=True)      #rename the columns according to the first row
        find_name_date_cols(data_frame)         #call column finder
        data_frame.set_index(keys=[data_frame.columns[name_col],data_frame.columns[date_col]],inplace=True) #set the found name and date columns as the df indexes
        data_frame.drop(data_frame.index[0],inplace=True)               #drop the first row of the df (contains original headings)
 
    #otherwise if the file did not have blank leading rows, re-import it, setting the headings correctly
    else:
        find_name_date_cols(data_frame)     #call column finder
        #re-read the excel file to set the index as the stasname and date columns
        data_frame = pandas.read_excel(item,index_col=[name_col,date_col],na_values=['-',' - '])
        data_frame.dropna(axis='index',inplace=True,thresh=2)           #remove empty rows

    #the rest of this function operates on the data frame that has resulted from one of the above if statements

    #if necessary, replace the name index with 'stasname' (eg Station_Name --> stasname)
    if 'name' in str(data_frame.index.names[0]).lower():
        data_frame.index.names=['stasname',data_frame.index.names[1]]   #replace first one but keep second one
        
    #drop columns that are not necessary
    for col in data_frame.iteritems():
        if not ("temp" in str(col[0]).lower() or "hum" in str(col[0]).lower()):
            data_frame.drop(columns=col[0],inplace=True)
        elif "temp" in str(col[0]).lower():
            data_frame.rename(columns={col[0]:'temp'},inplace=True)
        elif "hum" in str(col[0]).lower():
            data_frame.rename(columns={col[0]:'humidity'},inplace=True)

    return data_frame #return manipulated df


#----------------- pnp and parmalat function -----------------
    
def manip_pnpc_parmalat(item,data_frame):
    #re-read csv file to ensure the column headers are set as all three levels from the raw data, and to set '-' as NaN    
    data_frame = pandas.read_csv(item,delimiter=';',index_col=0,header=[0,1,2],na_values=['-',' - '])
    data_frame.index=pandas.to_datetime(data_frame.index,dayfirst=True)           #typecast the index as datetime
    data_frame=data_frame.stack([0,1,2])                            #turn frame into many columns, few rows - with column headings as multiindex
    data_frame.index.names=['datetime','site','device','variable']  #rename indexes 
    data_frame.rename('value',inplace=True)                         #rename actual data as value
    return data_frame



#------------------------------------------------------------------------------
#------------ loop through directory and upload data to db --------------------

#folder to take files from
dirname='C:/push_to_db/'
os.chdir(dirname) #change to this folder


#loop through directory and unzip any zip files, then remove them
for item in os.listdir(dirname):
    if item.endswith('.zip'):                   #check for ".zip" extension
        file_name = os.path.abspath(item)       #get full path of file
        zip_ref = zipfile.ZipFile(file_name)    #create zipfile object
        zip_ref.extractall(dirname)             #extract file to dir
        zip_ref.close()                         #close file
        os.remove(file_name)                    #delete zipped file
        
        
#open mysql connection - has to happen outside of the file for loop otherwise it would open and close multiple times
engine = create_engine('mysql+pymysql://root:ep101@localhost/ei_db', echo=False)
connection = engine.connect()
        
#loop through remaining files in directory and send them to their relevant database table
for item in os.listdir(dirname):
    
    found=False     #flag to indicate whether or not a file has been found - data will only be sent if a file is found
    
    #here the file will be read in as a data frame simply to determine which table it must go into
    if item.endswith('.xls') or item.endswith('.xlsx'):     #read excel files
        data_frame = pandas.read_excel(item,na_values=['-',' - '])
    elif item.endswith('.csv'):        #read csv files - files MUST NOT have been text-to-columned
        data_frame = pandas.read_csv(item, delimiter=';',na_values=['-',' - '])
        
    #drop any empty rows (if they have less than 2 populated cells)
    data_frame.dropna(axis='index',inplace=True,thresh=2)
        
    #now check which table the data must be sent to
    #currently: pvspot, parmalat, weather sa, pnpc
    try:
        #check if 'pvspot' is present in first cell of df
        if 'pvspot' in data_frame.columns[0].lower():
            found=True                                  #indicate that a file has been found
            data_frame=manip_pvspot(item,data_frame)    #manipulate specific to pvspot data
            table='pvspot'             #set db table to pvspot - next step will push data to db
           
        #check if the first row contains the weather sa headings
        elif 'temp' in str(data_frame.iloc[0]).lower() and 'hum' in str(data_frame.iloc[0]).lower():
            found=True                                      #indicate that a file has been found
            data_frame=manip_weather_sa(item,data_frame)    #manipulate specific to weather sa data
            table='weather_sa'                              #set db table to weather_sa
        
        #OR check if the columns contain the weather sa headings
        elif 'temp' in str(data_frame.columns).lower() and 'hum' in str(data_frame.columns).lower():
            found=True                                      #indicate that a file has been found
            data_frame=manip_weather_sa(item,data_frame)    #manipulate specific to weather sa data
            table='weather_sa'                              #set db table to weather_sa

        
        #check if 'parmalat' is present in the second column of the df
        elif 'parmalat' in data_frame.columns[1].lower():
            found=True                                      #indicate that a file has been found
            print("\nFound parmalat...")
            data_frame=manip_pnpc_parmalat(item,data_frame) #manipulate specific to parmalat data
            table='parmalat'                                #set db table to parmalat
            
    
        #check if 'pnp' is present in the second column of the df
        elif 'pnp' in data_frame.columns[1].lower():
            found=True                                      #indicate that a file has been found
            print("\nFound pnpc...")
            data_frame=manip_pnpc_parmalat(item,data_frame) #manipulate specific to pnp data
            table='pnpc'                                    #set db table to pnpc
       
    #if file doesn't match one of the four above, alert user and then move on
    except:
        print("\nThe file \"" + str(item) + "\" is not in one of the standard formats, \nor does not yet have a database table associated with it.")
    
#------------------- end of table-specific manipulation -----------------------    
    

    #now push the data to the database one row at a time
    
    #counters
    success=0
    fail=0
    
    #only if there was at least one relevant file found
    if found==True:
        final_frame=data_frame

        #loop through the rows of the data frame and attempt to send them to the db        
        for row in range(final_frame.shape[0]):
            try:
                current_row=final_frame[row:row+1]
                current_row.to_sql(name=table, con = engine, index=True, if_exists='append') 
                success+=1
                #"progress bar"
                sys.stdout.write("\r" + str(success) + " records uploaded so far.")
            except Exception as e:
                if "Duplicate entry" in str(e):
                    #if there is a duplicate entry, say so with another progress counter 
                    fail+=1
                    sys.stdout.write("\r" + str(fail) + " duplicate records ignored.")
                else:
                    print(e)
            finally:
                pass
        
        #finish by displaying how many records were uploaded in total
        print("\n" + str(success) + ' records uploaded for ' + str(table) +'.')
        
    #delete the file from the folder
#    os.remove(item)        
            
#close database connection and notify user that the programme is finished
connection.close()
print("\nClosed and finished.")
    
