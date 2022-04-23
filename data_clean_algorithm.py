import sqlite3
import pandas as pd
import numpy as np
import re

##Query unclean data scraped from Wikipedia
conn = sqlite3.connect('films.sqlite')
cur = conn.cursor()
df=pd.read_sql_query('SELECT * FROM Film',conn)

conn.close()

df['name']=df['name'].apply(lambda x:x[1:].split('/')[1])
df['name']=df['name'].apply(lambda x:x.replace('_',' '))

import ast
## turn orginal text from scraped data to python dictionary and turned to a pd df. (decom)
df['data_a']=df['data'].apply(lambda x:ast.literal_eval(x))
decom=df['data_a'].apply(pd.Series)
decom=decom[['Directed by', 'Written by', 'Produced by', 'Starring',
       'Cinematography', 'Edited by', 'Music by', 'Release date',
       'Running time', 'Country','Language','Productioncompanies','Distributed by','Budget','Box office','Productioncompany']]

decom['Productioncompanies'] = decom['Productioncompanies'].fillna(decom.pop('Productioncompany'))


## function that forces string to have common delim for split.

def common_delim(str_):
    if str_.find(',')>-1:
        return str_
    elif len(re.split(r'\S[A-Z]',str_))>1:
        split_list=[]
        i_s=0
        for i_e in re.finditer(r'\S[A-Z]',str_):
            split_list.append(str_[i_s:i_e.start()+1])
            i_s=len(str_[:i_e.start()+1])
            i_e=i_e.start()+1
        split_list.append(str_[i_e:len(str_)])
        return split_list
    elif len(re.findall(r'[A-Z]\S+\s[A-Z]\S+\s|[A-Z]\S+\s[A-Z]\S+',str_))>1:
        multiple_names=re.findall(r'[A-Z]\S+\s[A-Z]\S+\s|[A-Z]\S+\s[A-Z]\S+',str_)
        return multiple_names
    else:
        return str_
        
## function that creates film_xx connection table and unique look up table stored both in dictionary.

        
def lookup_idtable(col,c):
    output={}
    decom[col]=decom[col].apply(lambda x:str(x))
    decom[col]=decom[col].apply(common_delim)
    decom[col]=decom[col].apply(lambda x:str(x))
    table=decom[col].str.split(',',expand=True)

    ## cleans strings from the new df
    ## creates a df of unique values 

    for i in list(range(0,len(table.columns))):    
        table[i]=table[i].apply(lambda x:re.sub('\W+',' ', str(x) ))
        table[i]=table[i].apply(lambda x:x.strip())

    b_=[]
    for i in list(range(0,len(table.columns))):
        b_.extend(list(table[table[i]!='None'].iloc[:][i]))
    
    b_lis=pd.Series(b_)
    b_lookup=pd.DataFrame(b_lis.unique())
    b_lookup.reset_index(inplace=True)
    b_lookup.columns=['id',c]
    
    output['lookup']=b_lookup
    ##intermediate df in prep for 
    inter_df=table.join(df)
    cols=list(range(0,len(table.columns)))
    cols.append('name')
    inter_df=inter_df[cols]

    ## Creates a df of every combination of director and film for the CONNECTOR table

    film_b=pd.DataFrame()
    count=0
    for index, row in inter_df.iterrows():
        df_i=pd.DataFrame(row[(row!='None')]).iloc[:-1]
        df_i['film']=row[(row!='None')].iloc[-1]
        df_i.columns=[[c,'film']]
        film_b=pd.concat([film_b,df_i])
        count=count+1
        if (count % 10000)==0:
        
            print(count)
    film_b.reset_index(inplace=True)
    film_b.drop(['index'],axis=1,inplace=True)
    film_b.columns=[c,'film']

    film_b_id= pd.merge(left=film_b, right=film_table,on='film',how='left' )
    film_b_id=pd.merge(left=film_b_id, right=b_lookup,on=c,how='left' )
    film_b_id=film_b_id[['id_x','id_y']]
    film_b_id.reset_index(inplace=True)
    film_b_id.columns=['unique_id','film_id',c+'_id']
    output['id']=film_b_id
    return output


film_table= df[['name','id']]
film_table.columns=['film','id']

dict_names=['Directors','Writers','Producers','Actors','Editors','composers','Productioncompanies','distributors']
list_a=['Directed by','Written by','Produced by','Starring','Edited by','Music by','Productioncompanies','Distributed by']
list_c=['Director','Writer','Producer','Actor','Editor','composer','Productioncompany','distributor']
## creating nested dictionary of connecting tables and unique table information 
## eg film_director_id table  and director table.
output={}
zipped_list=list(zip(dict_names,list_a,list_c))
for table,col,id_name in zipped_list:
    inter_dict=lookup_idtable(col,id_name)
    output[table]=inter_dict
    print(table)

film_table=film_table.merge(decom,left_index=True, right_index=True)
film_table=film_table[['film','id','Release date','Running time','Country','Language','Budget','Box office']]

## creating new db and inserting new unique table names ie list of all directors in table called directors
conn = sqlite3.connect('clean_film_data.sqlite')
cur = conn.cursor()
zipped_list_b=list(zip(list_c,dict_names))
for name_,names in zipped_list_b:
    cur.executescript('''
    DROP TABLE IF EXISTS '''+names+''';
    CREATE TABLE'''+' '+ names+''' (
    id     INTEGER NOT NULL PRIMARY KEY UNIQUE,
    '''+name_+'''   TEXT UNIQUE
    );
    ''')
#insert df's into SQL DB
    for index, row in output[names]['lookup'].iterrows():
        cur.execute("INSERT INTO"+" "+names+"  (id,"+name_+") VALUES (?,?)",(row))
    conn.commit()
cur.close()


## Creating xx_film_id tables. These are the connector tables for the db that deal with the many to many cardinality issue with the data
sqlite3.register_adapter(np.int64, int)
conn = sqlite3.connect('clean_film_data.sqlite')
cur = conn.cursor()
zipped_list_b=list(zip(list_c,dict_names))
for name_,names in zipped_list_b:
    cur.executescript('''
    DROP TABLE IF EXISTS '''+names+'''_film_id;
    CREATE TABLE'''+' '+ names+'''_film_id (
    unique_id     INTEGER NOT NULL PRIMARY KEY UNIQUE,
    film_id       INTEGER NOT NULL,
    '''+name_+'''_id  INTEGER NOT NULL
    );
    ''')
#insert df's into SQL DB
    for index, row in output[names]['id'].iterrows():
        
        cur.execute("INSERT INTO"+" "+names+"_film_id (unique_id,film_id,"+name_+"_id) VALUES (?,?,?)",(row))
    conn.commit()
cur.close()

## function that turns release date columns to date time objects
from datetime import datetime
from dateutil import parser

def clean_date(str_):
    str_=str(str_)
    if re.search(r'(\d{4}-\d{2}-\d{2})', str_) != None:
        date=re.search(r'(\d{4}-\d{2}-\d{2})', str_).group(1)
        try:
            date=parser.parse(date)
            date=date.strftime("%Y-%m-%d")
        except:
            pass
        return date 
    elif re.search(r'(\d{4}-\d{2})', str_) != None:
        date=re.search(r'(\d{4}-\d{2})', str_).group(1)
        try:
            date=parser.parse(date)
            date=date.strftime("%Y-%m-%d")
        except:
            date='N/A'
        return date
   
    elif str_== 'N/A':
        return str_
    else:
        try:
            date=parser.parse(date)
            date=date.strftime("%Y-%m-%d")
        except:
            date='N/A'
        return date

film_table['Release date'].fillna('N/A',inplace=True)
film_table['Release date']=film_table['Release date'].apply(clean_date)

## function that cleans running time columns

def time_clean(time):
    try:
        time=str(time)
        time=time.lower()
    except:
        pass
    try:
        time=time.split()
    except:
        pass
    minute_vari=['minutes','mins','min','mins.']
    hour_vari=['h','hour','hours','hours.']
    if len(time)>1:
    
        if (len(time)==2) and (time[1] in minute_vari):
            return time[0]
        elif (time[1] in hour_vari) and (len(time)==4):
            try:
                time=int(time[0])*60 + int(time[2])
                return time
            except:
                time=='N/A'
                return time
        elif (len(time)>2) and (time[1] in minute_vari) and(any(time for item2 in hour_vari)!=True):
            return time[0]
        else:
            return 'N/A'

film_table['Running time'].fillna('N/A',inplace=True)
film_table['Running time']=film_table['Running time'].apply(time_clean)


def currency_clean(str_):
    str_=str(str_)
    str_=str_.lower()
    str_=str_.strip()
    if len(str_.split('['))>1:
        str_=str_.split('[')[0]
        
    
    if ',' in str_:
        str_=str_.replace(',','')
    
    if (re.search(r'\d\.\d\smillion',str_)!= None):
        str_=str_.replace(' ','')
        str_=str_.replace('.','')
        str_=str_.replace('million','00000')
    
    if (re.search(r'\d\smillion',str_)!= None) or(re.search(r'\d\d\smillion',str_)!= None) or(re.search(r'\d\d\d\smillion',str_)!= None) :
        str_=str_.replace(' ','')
        str_=str_.replace('.','')
        str_=str_.replace('million','000000')
    
    if len(str_.split('('))>1:
        str_=str_.split('(')[0]
    
    
    if re.search('[^0-9\$\.]',str_)!=None:
        return 'N/A'
    try:
        str_=re.findall(r'\$.+',str_)
        return str_[0]
    except:
        return str_

film_table['Country'].fillna('N/A',inplace=True)
film_table['Language'].fillna('N/A',inplace=True)
film_table['Budget'].fillna('N/A',inplace=True)
film_table['Box office'].fillna('N/A',inplace=True)
film_table['Budget']=film_table['Budget'].apply(currency_clean)
film_table['Box office']=film_table['Box office'].apply(currency_clean)

for col in list(film_table.columns[2:]):
    film_table[col]=film_table[col].apply(lambda x:str(x))


conn = sqlite3.connect('clean_film_data.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS film_table;
CREATE TABLE film_table (
film   TEXT NOT NULL,
id     INTEGER NOT NULL PRIMARY KEY UNIQUE,
release_date TEXT,
running_time_mins TEXT,
country TEXT,
language TEXT,
budget TEXT,
box_office TEXT


    );
    ''')
#insert df's into SQL DB
for index, row in film_table.iterrows():
    cur.execute("INSERT INTO film_table (film, id, release_date, running_time_mins, country, language,budget, box_office) VALUES (?,?,?,?,?,?,?,?)",(row))
conn.commit()
cur.close()