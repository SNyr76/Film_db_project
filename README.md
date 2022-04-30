# Film_db_project
 **Project outline:**
 To webscrape every film on wikipedia, store its data on local db and visualize data through power bi. answer questions like biggest year in terms of box office success,
 biggest director in terms of box office success, biggest actor in terms of box office success
 **Main findings:**
 
  -- Fantasy/ Marvel films/ Childrens movies are the biggest in yeild. Walt Disney studios/ Marvel Studios/ Columbia Pictures productions are the biggest studios producing these films
  
  --Steven Spielberg is the biggest director in box office success ($11bn) in box office revenue
  --Samuel L Jackson is the biggest Actor in box office success ($18bn) in box office revenue
  --2012 was the biggest year in box office revenue.
  
Franchises are here to stay. With large franchises being the most consistent in box office revenue see Film_analysis.pbix it is unlikely that the film industry will change course in production of these movies. 


 
 
file explainations:

wikipedia_webscrape.py- This the initial file that scrapes the web data and creates a sqllite db (films.sqlite) to store that scraped data.

data_clean_algorithm.py- This cleans the data ready for loading into the db and loads the data to db (clean_film_data.sqlite) 

Film_analysis.pbix- Power BI file with dashboard
