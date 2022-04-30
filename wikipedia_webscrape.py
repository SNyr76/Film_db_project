from bs4 import BeautifulSoup
import time
import sqlite3
import requests

conn = sqlite3.connect('films.sqlite')
cur = conn.cursor()

##
##
## list of the headpages that lists all films  
##
##

headpages=['List_of_films:_numbers','List_of_films:_A','List_of_films:_B','List_of_films:_C','List_of_films:_D','List_of_films:_E','List_of_films:_F','List_of_films:_G',
 'List_of_films:_H','List_of_films:_I','List_of_films:_J–K','List_of_films:_L','List_of_films:_M',
 'List_of_films:_N–O','List_of_films:_P','List_of_films:_Q–R','List_of_films:_S','List_of_films:_T','List_of_films:_U–W',
 'List_of_films:_X–Z']
titles=[]
title_link=[]
for i in headpages:
    ##Access title pages
    i=str(i)
    r = requests.get("https://en.wikipedia.org/wiki/"+i)
    content=r.content 
    soup = BeautifulSoup(content.decode('utf-8','ignore'))
    
    list_anchors=soup.findAll('a')
    ## gets titles for every film
    for tag in list_anchors:
        if tag.get('title')==None:
            continue
        else:
            film=tag.get('title')
            film_link=tag.get('href')
            titles.append(film)
            title_link.append(film_link)
    
    print (i,"done")
    time.sleep(1)
##
##
## storing all titles to be scraped
##
##
title_link=set(title_link)
title_link_a=list(title_link)

## When programme restarts makes a list of films that haven't been scraped
cur.execute('''SELECT Name From Film''')
result = cur.fetchall()
final_result = [i[0] for i in result]
remaining=list(set(title_link_a)-set(final_result))
##
##
## Looks for info box class in webpage and takes data, stores in dictionary.Stores that dictionary in films.sql db
##
##
count=0
n=0
for link in remaining:
    link=str(link)
    try:
        r = requests.get("https://en.wikipedia.org"+link)
    except:
        print('rejection')
        time.sleep(5)
        continue
    content=r.content 
    soup = BeautifulSoup(content.decode('utf-8','ignore'))

    info=soup.find(class_="infobox vevent")
    try:
        title_h=info.findAll('th',class_='infobox-label')
    except:
        continue
##grabbing information from page under the info box class and compositing a dictionary##
    key=[]
    for tag in title_h:
        txt=tag.get_text()
        txt=str(txt)
        if len(str(txt).split('>'))>1:
            txt=txt.split('"')[2]
        txt=txt.replace('[','')
        txt=txt.replace(']','')
        txt=txt.replace('\'','')
        txt=txt.replace('</div>','')
        txt=txt.replace('<br/>','')
        txt=txt.replace('>','')
        key.append(txt)
    


    title_c=info.findAll('td',class_='infobox-data')
    val=[]
    for content in title_c:
        if content.find(class_='plainlist')!=None:
            txt_b=content.get_text()
            txt_b=str(txt_b)
            if '\xa0' in txt_b:
                txt_b=txt_b.replace('\xa0','')            
            txt_b=txt_b.split('\n')
            txt_b=list(filter(lambda x:len(x)>1,txt_b))
        
            val.append(txt_b)
        else:
            txt_b=content.get_text()
            if len(txt_b.split('['))==2:
                txt_b=txt_b[:-3]
            
            val.append(txt_b)
        
    zipped=zip(key,val)
    movie_data=dict(zipped)
    data=str(movie_data)

    cur.execute('''INSERT OR IGNORE INTO Film (name,data) 
                    VALUES ( ?, ?)''', ( link,data ) )
    conn.commit()
    count=count+1
    if (count % 10)==0:
        n=n+1
        time.sleep(5)
        print(n)

conn.close()
