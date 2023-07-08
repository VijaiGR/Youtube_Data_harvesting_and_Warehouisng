from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from bson import ObjectId
import streamlit as st
import pymongo
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pymongo import MongoClient
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text



st.set_page_config(layout='wide')
base="dark"
primaryColor="purple"
st.title(':purple[Youtube Data Harvesting and Warehousing]')
channel_id = st.text_input('**channel_id**')

tab1, tab2= st.tabs(["Data Extraction", "Data Migration" ])

with tab1:
    col1, col2, col3 = st.columns(3, gap='small')
    Get_data = col1.button('**Retrieve data**')
    store_data = col2.button('**Store Data in MongoDB Atlas**')
    retrieve_data = col3.button('**Retrieve Data from MongoDB Atlas**')
    st.markdown("<br><br>", unsafe_allow_html=True)

    # Define Session state to Get data button
    if "Get_state" not in st.session_state:
        st.session_state.Get_state = False
    if Get_data or st.session_state.Get_state:
        st.session_state.Get_state = True

        # Access youtube API
        api_service_name = 'youtube'
        api_version = 'v3'
        api_key = 'AIzaSyCVhhW-B7VrtiDyX0RyrJEDahEkqarc3S8'
        youtube = build(api_service_name, api_version, developerKey=api_key)

        # Retrieve channel data using YouTube API
        if Get_data:
            # Initialize YouTube Data API client
            youtube = build('youtube', 'v3', developerKey=api_key)

            # API request to get channel data
            request = youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            )
            response = request.execute()

            # Extract relevant data
            channel_data = {
                'Channel Name': response['items'][0]['snippet']['title'],
                'Subscribers': response['items'][0]['statistics']['subscriberCount'],
                'Total Videos': response['items'][0]['statistics']['videoCount'],
                'Thumbnail_img': response['items'][0]['snippet']['thumbnails']['default']['url']
            }

            col1, col2, col3, col4 = st.columns(4)
            col1.image(channel_data['Thumbnail_img'], caption=channel_data['Channel Name'])
            col2.markdown("**Channel Name:** " + channel_data['Channel Name'])
            col3.markdown("**Subscribers:** " + channel_data['Subscribers'])
            col4.markdown("**Total Videos:** " + channel_data['Total Videos'])

# ----------------------------------------------------------------------------------------------------- #

        # Fetch channel statistics
        def get_channel_stats(youtube, channel_id):
            try:
                channel_request = youtube.channels().list(
                    part='snippet,statistics,contentDetails',
                    id=channel_id)
                channel_response = channel_request.execute()

                if 'items' not in channel_response or len(channel_response['items']) == 0:
                    st.write(f"Invalid channel id: {channel_id}")
                    st.error("Enter **channel_id**")
                    return None

                return channel_response

            except HttpError as e:
                st.error('Server error (or) Check your internet connection (or) Please Try again after a few minutes',
                         icon='🚨')
                st.write('An error occurred: %s' % e)
                return None


        # Function call to Get Channel data from a single channel ID
        channel_data = get_channel_stats(youtube, channel_id)

        # Process channel data
        # Extract required information from the channel_data
        channel_name = channel_data['items'][0]['snippet']['title']
        channel_video_count = channel_data['items'][0]['statistics']['videoCount']
        channel_subscriber_count = channel_data['items'][0]['statistics']['subscriberCount']
        channel_view_count = channel_data['items'][0]['statistics']['viewCount']
        channel_description = channel_data['items'][0]['snippet']['description']
        channel_playlist_id = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        # Format channel_data into dictionary
        channel = {
            "Channel_Details": {
                "Channel_Name": channel_name,
                "Channel_Id": channel_id,
                "Video_Count": channel_video_count,
                "Subscriber_Count": channel_subscriber_count,
                "Channel_Views": channel_view_count,
                "Channel_Description": channel_description,
                "Playlist_Id": channel_playlist_id
            }
        }


        # Define a function to retrieve video IDs from channel playlist
        def get_video_ids(youtube, channel_playlist_id):
            video_id = []
            next_page_token = None
            while True:
                # Get playlist items
                request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=channel_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token)
                response = request.execute()

                # Get video IDs
                for item in response['items']:
                    video_id.append(item['contentDetails']['videoId'])

                # Check if there are more pages
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

            return video_id


        # Function call to Get  video_ids using channel playlist Id
        video_ids = get_video_ids(youtube, channel_playlist_id)


        # Define a function to retrieve video data
        def get_video_data(youtube, video_ids):
            video_data = []
            for video_id in video_ids:
                try:
                    # Get video details
                    request = youtube.videos().list(
                        part='snippet, statistics, contentDetails',
                        id=video_id)
                    response = request.execute()

                    video = response['items'][0]

                    # Get comments if available (comment function call)
                    try:
                        video['comment_threads'] = get_video_comments(youtube, video_id, max_comments=2)
                    except:
                        video['comment_threads'] = None

                    # Duration format transformation (Duration format transformation function call)
                    duration = video.get('contentDetails', {}).get('duration', 'Not Available')
                    if duration != 'Not Available':
                        duration = parse_duration(duration)
                    video['contentDetails']['duration'] = duration

                    video_data.append(video)

                except:
                    st.write('You have exceeded your YouTube API quota. Please try again tomorrow.')

            return video_data



        def get_video_comments(youtube, video_id, max_comments):
            request = youtube.commentThreads().list(
                part='snippet',
                maxResults=max_comments,
                textFormat="plainText",
                videoId=video_id)
            response = request.execute()

            return response

        # Define a function to convert duration
        def parse_duration(duration):
            duration_str = ""
            hours = 0
            minutes = 0
            seconds = 0

            # Remove 'PT' prefix from duration
            duration = duration[2:]

            # Check if hours, minutes, and/or seconds are present in the duration string
            if "H" in duration:
                hours_index = duration.index("H")
                hours = int(duration[:hours_index])
                duration = duration[hours_index + 1:]
            if "M" in duration:
                minutes_index = duration.index("M")
                minutes = int(duration[:minutes_index])
                duration = duration[minutes_index + 1:]
            if "S" in duration:
                seconds_index = duration.index("S")
                seconds = int(duration[:seconds_index])

            # Format the duration string
            if hours > 0:
                duration_str += f"{hours}h "
            if minutes > 0:
                duration_str += f"{minutes}m "
            if seconds > 0:
                duration_str += f"{seconds}s"

            return duration_str.strip()

        # Function call to Get Videos data and comment data from video ids

        video_data = get_video_data(youtube, video_ids)

        # video details processing
        videos = {}
        for i, video in enumerate(video_data):
            video_id = video['id']
            video_name = video['snippet']['title']
            video_description = video['snippet']['description']
            tags = video['snippet'].get('tags', [])
            published_at = video['snippet']['publishedAt']
            view_count = video['statistics']['viewCount']
            like_count = video['statistics'].get('likeCount', 0)
            dislike_count = video['statistics'].get('dislikeCount', 0)
            favorite_count = video['statistics'].get('favoriteCount', 0)
            comment_count = video['statistics'].get('commentCount', 0)
            duration = video.get('contentDetails', {}).get('duration', 'Not Available')
            thumbnail = video['snippet']['thumbnails']['high']['url']
            caption_status = video.get('contentDetails', {}).get('caption', 'Not Available')
            comments = 'Unavailable'

            # Handle case where comments are enabled
            if video['comment_threads'] is not None:
                comments = {}
                for index, comment_thread in enumerate(video['comment_threads']['items']):
                    comment = comment_thread['snippet']['topLevelComment']['snippet']
                    comment_id = comment_thread['id']
                    comment_text = comment['textDisplay']
                    comment_author = comment['authorDisplayName']
                    comment_published_at = comment['publishedAt']
                    comments[f"Comment_Id_{index + 1}"] = {
                        'Comment_Id': comment_id,
                        'Comment_Text': comment_text,
                        'Comment_Author': comment_author,
                        'Comment_PublishedAt': comment_published_at
                    }

            # Format processed video data into dictionary
            videos[f"Video_Id_{i + 1}"] = {
                'Video_Id': video_id,
                'Video_Name': video_name,
                'Video_Description': video_description,
                'Tags': tags,
                'PublishedAt': published_at,
                'View_Count': view_count,
                'Like_Count': like_count,
                'Dislike_Count': dislike_count,
                'Favorite_Count': favorite_count,
                'Comment_Count': comment_count,
                'Duration': duration,
                'Thumbnail': thumbnail,
                'Caption_Status': caption_status,
                'Comments': comments
            }

        # -------------------------------------------------------------------------------------------- #

        # combine channel data and videos data to a dict
        final_output = {**channel, **videos}

        # -----------------------------------    /   MongoDB connection and store the collected data   /    ---------------------------------- #

        # create a client instance of MongoDB
        #client = pymongo.MongoClient('mongodb://localhost:27017/')
        # Connect to MongoDB Atlas
        atlas_username = 'vijaygowtham11'
        atlas_password = 'Typ3myn2m3'
        atlas_cluster = 'Cluster11'
        client = MongoClient(
            f"mongodb+srv://vijaigowtham11:{atlas_password}@cluster11.tdzyngu.mongodb.net/")
        db = client['youtube_data']
        collection = db['channel_data']
        final_output_data = {
            'Channel_Name': channel_name,
            "Channel_data": final_output
        }

        # Store data in MongoDB Atlas
        if store_data:  # st.button("Store Data in MongoDB Atlas")
            #collection.replace_one({'_id': channel_id}, final_output_data, upsert=True)
            collection.insert_one(channel)
            st.success("Data stored successfully in MongoDB Atlas!")

        # Retrieve data from MongoDB Atlas
        if retrieve_data:  # st.button("Retrieve Data from MongoDB Atlas")
            retrieved_data = collection.find_one({'Channel_Name.Channel_Id': channel_id})
            if retrieved_data:
                st.subheader("Retrieved Data:")
                st.write("Channel Name:", retrieved_data['Channel_Name']['Channel_Name'])
                st.write("Subscribers:", retrieved_data['Channel_Name']['Subscription_Count'])
                st.write("Total Videos:", len(videos))
                for video_id, video_data in retrieved_data.items():
                    if video_id != 'Channel_Name' and not isinstance(video_data, ObjectId):
                        st.write("Video Name:", video_data['Video_Name'])
                        st.write("Video Description:", video_data['Video_Description'])
                        st.write("Published At:", video_data['PublishedAt'])
                        st.write("View Count:", video_data['View_Count'])
                        st.write("Like Count:", video_data['Like_Count'])
                        st.write("Dislike Count:", video_data['Dislike_Count'])
                        st.write("Comment Count:", video_data['Comment_Count'])
                        st.write("Duration:", video_data['Duration'])
                        st.write("Thumbnail:", video_data['Thumbnail'])
            else:
                st.warning("Data not found in MongoDB Atlas!")
        client.close()


with tab2:
    st.header(':violet[Data Transformation and Querying ]')

    # Connect to the MongoDB server
    # create a client instance of MongoDB
    #client = pymongo.MongoClient('mongodb://localhost:27017/')
    # Connect to MongoDB Atlas
    atlas_username = 'vijaygowtham11'
    atlas_password = 'Typ3myn2m3'
    atlas_cluster = 'Cluster11'
    client = MongoClient(
        f"mongodb+srv://vijaigowtham11:{atlas_password}@cluster11.tdzyngu.mongodb.net/")

    db = client['youtube_data']
    collection = db['channel_data']
    file_names = []
    for file in collection.find():
        file_names.append(file['_id'])

    document_name = st.selectbox('**Select Channel name**', options=file_names, key='file_names')
    Migrate = st.button('**Migrate to Postgres**')

    # Define Session state to Migrate to Postgres  button
    if 'migrate_sql' not in st.session_state:
        st.session_state_migrate_sql = False
    if Migrate or st.session_state_migrate_sql:
        st.session_state_migrate_sql = True

        # Retrieve the document with the specified name
        result = collection.find_one({'_id': document_name})
        client.close()

        # Channel data json to df
        channel_data_tosql = {
            "Channel_Name": result['Channel_Name'],
            "Channel_Id": result['_id'],
            "Video_Count": result['Channel_data']['Channel_Details']['Video_Count'],
            "Subscriber_Count": result['Channel_data']['Channel_Details']['Subscriber_Count'],
            "Channel_Views": result['Channel_data']['Channel_Details']['Channel_Views'],
            "Channel_Description": result['Channel_data']['Channel_Details']['Channel_Description'],
            "Playlist_Id": result['Channel_data']['Channel_Details']['Playlist_Id']
        }
        channel_df = pd.DataFrame.from_dict(channel_data_tosql, orient='index').T

        # playlist data json to df
        playlist_tosql = {"Channel_Id": result['_id'],
                          "Playlist_Id": result['Channel_data']['Channel_Details']['Playlist_Id']
                          }
        playlist_df = pd.DataFrame.from_dict(playlist_tosql, orient='index').T

        # video data json to df
        video_details_list = []
        for i in range(1, len(result['Channel_data']) - 1):
            video_details_tosql = {
                'Playlist_Id': result['Channel_data']['Channel_Details']['Playlist_Id'],
                'Video_Id': result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
                'Video_Name': result['Channel_data'][f"Video_Id_{i}"]['Video_Name'],
                'Video_Description': result['Channel_data'][f"Video_Id_{i}"]['Video_Description'],
                'Published_date': result['Channel_data'][f"Video_Id_{i}"]['PublishedAt'],
                'View_Count': result['Channel_data'][f"Video_Id_{i}"]['View_Count'],
                'Like_Count': result['Channel_data'][f"Video_Id_{i}"]['Like_Count'],
                'Dislike_Count': result['Channel_data'][f"Video_Id_{i}"]['Dislike_Count'],
                'Favorite_Count': result['Channel_data'][f"Video_Id_{i}"]['Favorite_Count'],
                'Comment_Count': result['Channel_data'][f"Video_Id_{i}"]['Comment_Count'],
                'Duration': result['Channel_data'][f"Video_Id_{i}"]['Duration'],
                'Thumbnail': result['Channel_data'][f"Video_Id_{i}"]['Thumbnail'],
                'Caption_Status': result['Channel_data'][f"Video_Id_{i}"]['Caption_Status']
            }
            video_details_list.append(video_details_tosql)
        video_df = pd.DataFrame(video_details_list)

        # Comment data json to df
        Comment_details_list = []
        for i in range(1, len(result['Channel_data']) - 1):
            comments_access = result['Channel_data'][f"Video_Id_{i}"]['Comments']
            if comments_access == 'Unavailable' or (
                    'Comment_Id_1' not in comments_access or 'Comment_Id_2' not in comments_access):
                Comment_details_tosql = {
                    'Video_Id': 'Unavailable',
                    'Comment_Id': 'Unavailable',
                    'Comment_Text': 'Unavailable',
                    'Comment_Author': 'Unavailable',
                    'Comment_Published_date': 'Unavailable',
                }
                Comment_details_list.append(Comment_details_tosql)

            else:
                for j in range(1, 3):
                    Comment_details_tosql = {
                        'Video_Id': result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
                        'Comment_Id': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"][
                            'Comment_Id'],
                        'Comment_Text': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"][
                            'Comment_Text'],
                        'Comment_Author': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"][
                            'Comment_Author'],
                        'Comment_Published_date':
                            result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"][
                                'Comment_PublishedAt'],
                    }
                    Comment_details_list.append(Comment_details_tosql)
        Comments_df = pd.DataFrame(Comment_details_list)

        # Connect to SQL database
        db_name = 'youtube_db'
        db_username = 'postgres'
        db_password = 'admin'
        db_host = 'localhost'
        db_port = '5432'


        cnx = psycopg2.connect(
            host=db_host,
            user=db_username,
            password=db_password,
            database=db_name
        )
        cnx.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        mycursor = cnx.cursor()
        mycursor.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = 'youtube_db'")
        exists = mycursor.fetchone()

        # Create the database if it doesn't exist
        if not exists:
            mycursor.execute("CREATE DATABASE youtube_db")



        # Close the cursor and database connection
        mycursor.close()
        cnx.close()

        # Connect to the new created database
        engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}', echo=False)
        Session = sessionmaker(bind=engine)

        # Open a session
        session = Session()

        # Base = declarative_base()

        # Use pandas to insert the DataFrames data to the SQL Database -> table1

        # Channel data to SQL
        channel_df.to_sql('channel', session.bind, if_exists='append', index=False,
                          dtype={"Channel_Name": sqlalchemy.types.VARCHAR(length=255),
                                 "Channel_Id": sqlalchemy.types.VARCHAR(length=255),
                                 "Video_Count": sqlalchemy.types.INT,
                                 "Subscriber_Count": sqlalchemy.types.BigInteger,
                                 "Channel_Views": sqlalchemy.types.BigInteger,
                                 "Channel_Description": sqlalchemy.types.TEXT,
                                 "Playlist_Id": sqlalchemy.types.VARCHAR(length=255), })

        # Playlist data to SQL
        playlist_df.to_sql('playlist', session.bind, if_exists='append', index=False,
                           dtype={"Channel_Id": sqlalchemy.types.VARCHAR(length=255),
                                  "Playlist_Id": sqlalchemy.types.VARCHAR(length=255), })

        # Video data to SQL
        video_df.to_sql('video', session.bind, if_exists='append', index=False,
                        dtype={'Playlist_Id': sqlalchemy.types.VARCHAR(length=255),
                               'Video_Id': sqlalchemy.types.VARCHAR(length=255),
                               'Video_Name': sqlalchemy.types.VARCHAR(length=255),
                               'Video_Description': sqlalchemy.types.TEXT,
                               'Published_date': sqlalchemy.types.String(length=50),
                               'View_Count': sqlalchemy.types.BigInteger,
                               'Like_Count': sqlalchemy.types.BigInteger,
                               'Dislike_Count': sqlalchemy.types.INT,
                               'Favorite_Count': sqlalchemy.types.INT,
                               'Comment_Count': sqlalchemy.types.INT,
                               'Duration': sqlalchemy.types.VARCHAR(length=1024),
                               'Thumbnail': sqlalchemy.types.VARCHAR(length=255),
                               'Caption_Status': sqlalchemy.types.VARCHAR(length=225), })

        # Commend data to SQL
        Comments_df.to_sql('comments', session.bind, if_exists='append', index=False,
                           dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=255),
                                  'Comment_Id': sqlalchemy.types.VARCHAR(length=255),
                                  'Comment_Text': sqlalchemy.types.TEXT,
                                  'Comment_Author': sqlalchemy.types.VARCHAR(length=255),
                                  'Comment_Published_date': sqlalchemy.types.String(length=50), })

        session.commit()
        session.close()

        Check_channel = st.checkbox('**Check available channel data for analysis**')

        if Check_channel:
            # Create database connection
            engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}', echo=False)
            # Execute SQL query to retrieve channel names
            query = 'SELECT "Channel_Name" FROM channel;'
            results = pd.read_sql(query, engine)
            # Get channel names as a list
            channel_names_fromsql = list(results['Channel_Name'])
            # Create a DataFrame from the list and reset the index to start from 1
            df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
            # Reset index to start from 1 instead of 0
            df_at_sql.index += 1
            # Show dataframe
            st.dataframe(df_at_sql)

        st.subheader(':violet[Channels Analysis ]')

        # Select-box creation
        question_tosql = st.selectbox('**Select your Question**',
                                      ('1. What are the names of all the videos and their corresponding channels ?',
                                       '2. Which channels have the most number of videos, and how many videos do they have?',
                                       '3. What are the top 10 most viewed videos and their respective channels?',
                                       '4. How many comments were made on each video, and what are their corresponding video names?',
                                       '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                       '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                       '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                       '8. What are the names of all the channels that have published videos in the year 2022?',
                                       '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                       '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                                      key='collection_question')

        # Create a connection to SQL
        connect_for_question = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='admin',
        database='youtube_db'
        )

        cursor = connect_for_question.cursor()
        # Create a session maker for each question
        session_maker_1 = sessionmaker(bind=engine)
        session_maker_2 = sessionmaker(bind=engine)
        session_maker_3 = sessionmaker(bind=engine)
        session_maker_4 = sessionmaker(bind=engine)
        session_maker_5 = sessionmaker(bind=engine)
        session_maker_6 = sessionmaker(bind=engine)
        session_maker_7 = sessionmaker(bind=engine)
        session_maker_8 = sessionmaker(bind=engine)
        session_maker_9 = sessionmaker(bind=engine)
        session_maker_10 = sessionmaker(bind=engine)

        # q1
        if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
            session_1 = session_maker_1()
            query = text('SELECT DISTINCT "channel"."Channel_Name", "video"."Video_Name", "playlist"."Playlist_Id" '
                         'FROM "playlist" '
                         'JOIN "channel" ON "channel"."Channel_Id" = "playlist"."Channel_Id" '
                         'JOIN "video" ON "playlist"."Playlist_Id" = "video"."Playlist_Id";')

            result_1 = session_1.execute(query)
            df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name', 'Playlist Id']).reset_index(drop=True)
            df1.index += 1
            st.dataframe(df1)
            session_1.close()

        # Q2
        elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
            session_2 = session_maker_2()
            query = text('SELECT "Channel_Name", "Video_Count" FROM channel ORDER BY "Video_Count" DESC;')

            result_2 = session_2.execute(query)

            df2 = pd.DataFrame(result_2, columns=['Channel Name', 'Video Count']).reset_index(drop=True)
            df2.index += 1
            st.dataframe(df2)
            session_2.close()

        # Q3
        elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
            session_3 = session_maker_3()
            query = text('SELECT "channel"."Channel_Name", "video"."Video_Name", "video"."View_Count" '
                         'FROM "channel" '
                         'JOIN "playlist" ON "channel"."Channel_Id" = "playlist"."Channel_Id" '
                         'JOIN "video" ON "playlist"."Playlist_Id" = "video"."Playlist_Id" '
                         'ORDER BY "video"."View_Count" DESC '
                         'LIMIT 10;')
            result_3 = session_3.execute(query)
            df3 = pd.DataFrame(result_3, columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
            df3.index += 1
            st.dataframe(df3)
            session_3.close()

        # Q4
        elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
            session_4 = session_maker_4()
            query = text('SELECT "channel"."Channel_Name", "video"."Video_Name", "video"."Comment_Count" '
                         'FROM "channel" '
                         'JOIN "playlist" ON "channel"."Channel_Id" = "playlist"."Channel_Id" '
                         'JOIN "video" ON "playlist"."Playlist_Id" = "video"."Playlist_Id";')
            result_4 = session_4.execute(query)
            df4 = pd.DataFrame(result_4, columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
            df4.index += 1
            st.dataframe(df4)
            session_4.close()

        # Q5
        elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            session_5 = session_maker_5()
            query = text('SELECT "channel"."Channel_Name", "video"."Video_Name", "video"."Like_Count" '
                         'FROM channel '
                         'JOIN playlist ON "channel"."Channel_Id" = "playlist"."Channel_Id" '
                         'JOIN video ON "playlist"."Playlist_Id" = "video"."Playlist_Id" '
                         'ORDER BY "video"."Like_Count" DESC;')
            result_5 = session_5.execute(query)
            df5 = pd.DataFrame(result_5, columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
            df5.index += 1
            st.dataframe(df5)
            session_5.close()

        # Q6
        elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            st.write('**Note: From November 2021, YouTube removed dislike count from all videos.**')
            session_6 = session_maker_6()
            query = text(
                'SELECT "channel"."Channel_Name", "video"."Video_Name", "video"."Like_Count", "video"."Dislike_Count" '
                'FROM "channel" '
                'JOIN "playlist" ON "channel"."Channel_Id" = "playlist"."Channel_Id" '
                'JOIN "video" ON "playlist"."Playlist_Id" = "video"."Playlist_Id" '
                'ORDER BY "video"."Like_Count" DESC;')
            result_6 = session_6.execute(query)
            df6 = pd.DataFrame(result_6,
                               columns=['Channel Name', 'Video Name', 'Like count', 'Dislike count']).reset_index(
                drop=True)
            df6.index += 1
            st.dataframe(df6)
            session_6.close()

        # Q7
        elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            session_7 = session_maker_7()
            query = text('SELECT distinct "Channel_Name", "Channel_Views" FROM channel ORDER BY "Channel_Views" DESC;')
            result_7 = session_7.execute(query)
            df7 = pd.DataFrame(result_7, columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
            df7.index += 1
            st.dataframe(df7)
            session_7.close()

        # Q8
        elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
            session_8 = session_maker_8()
            query = text('SELECT "channel"."Channel_Name", "video"."Video_Name", "video"."Published_date" '
                         'FROM "channel" '
                         'JOIN "playlist" ON "channel"."Channel_Id" = "playlist"."Channel_Id" '
                         'JOIN "video" ON "playlist"."Playlist_Id" = "video"."Playlist_Id" '
                         'WHERE EXTRACT(YEAR FROM "video"."Published_date"::timestamp) = 2022;')
            result_8 = session_8.execute(query)
            df8 = pd.DataFrame(result_8, columns=['Channel Name', 'Video Name', 'Year 2022 only']).reset_index(
                drop=True)
            df8.index += 1
            st.dataframe(df8)
            session_8.close()


        # Q9
        elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            session_9 = session_maker_9()
            query = text(
                'SELECT "channel"."Channel_Name", TO_CHAR(AVG("video"."Duration"::interval), \'HH24:MI:SS\') AS "duration" '
                'FROM "channel" '
                'JOIN "playlist" ON "channel"."Channel_Id" = "playlist"."Channel_Id" '
                'JOIN "video" ON "playlist"."Playlist_Id" = "video"."Playlist_Id" '
                'GROUP BY "channel"."Channel_Name" '
                'ORDER BY "duration" DESC;'
            )
            result_9 = session_9.execute(query)
            df9 = pd.DataFrame(result_9, columns=['Channel Name', 'Average duration of videos (HH:MM:SS)']).reset_index(
                drop=True)
            df9.index += 1
            st.dataframe(df9)
            session_9.close()

        # Q10
        elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            session_10 = session_maker_10()
            query = text(
                'SELECT DISTINCT "channel"."Channel_Name", "video"."Video_Name", "video"."Comment_Count" FROM "channel" JOIN "playlist" ON "channel"."Channel_Id" = "playlist"."Channel_Id" JOIN "video" ON "playlist"."Playlist_Id" = "video"."Playlist_Id" ORDER BY "video"."Comment_Count" DESC;')
            result_10 = session_10.execute(query)
            df10 = pd.DataFrame(result_10, columns=['Channel Name', 'Video Name', 'Number of comments']).reset_index(
                drop=True)
            df10.index += 1
            st.dataframe(df10)
            session_10.close()

        # SQL DB connection close
        connect_for_question.close()
