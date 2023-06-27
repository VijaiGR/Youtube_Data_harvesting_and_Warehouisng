# Youtube_Data_harvesting_and_Warehousing

 ## Project Descriptions

 To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
 - Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
 - Option to store the data in a MongoDB database as a data lake.
 - Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
 - Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
 - Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

## Features
* ##### YouTube Data Fetching: Utilizes YouTube's Data API v3 to fetch channel details, video details, and comments.
* ##### MongoDB Atlas Integration: Stores the fetched data in MongoDB Atlas for reliable and scalable data storage.
* ##### Postgres Migration: Allows users to migrate channel data from MongoDB Atlas to their local Postgres database.
* ##### Data Analysis with Pandas: Performs custom queries and provides basic analysis of the fetched data using Pandas.
* ##### Interactive User Interface: Offers a user-friendly interface using Streamlit for easy data selection and display.

  



 
