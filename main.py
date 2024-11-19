import praw
import json
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, Table, Column, MetaData, Text, Integer, Numeric, String, TIMESTAMP
from sqlalchemy.sql import text
from dotenv import load_dotenv
import os
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config():
    load_dotenv()
    config = {
        'app_name': os.getenv('APP_NAME'),
        'username': os.getenv('USERNAME'),
        'client_id': os.getenv('CLIENT_ID'),
        'client_secret': os.getenv('CLIENT_SECRET'),
        'user_agent': os.getenv('USER_AGENT'),
        'db_host': os.getenv('DB_HOST'),
        'db_port': os.getenv('DB_PORT'),
        'db_name': os.getenv('DB_NAME'),
        'db_user': os.getenv('DB_USER'),
        'staging_table_name': 'reddit_posts_raw',
        'json_file_name': 'top_analog_posts.json',
        'posts_limit': 1000
    }
    return config


def setup_database(config):
    engine = create_engine(f'postgresql://{config["db_user"]}@{config["db_host"]}:{config["db_port"]}/{config["db_name"]}')
    metadata = MetaData()
    
    Table(
        config['staging_table_name'], metadata,
        Column('title', Text),
        Column('id', String(255), primary_key=True),
        Column('score', Integer),
        Column('upvote_ratio', Numeric),
        Column('num_comments', Integer),
        Column('url', String(255)),
        Column('author', String(255)),
        Column('created_utc', TIMESTAMP(timezone=True)),
        Column('selftext', Text),
        Column('subreddit', String(255)),
        Column('post_hint', String(255)),
    )

    with engine.connect() as connection:
        logging.info(f"Dropping table {config['staging_table_name']} if it exists...")
        connection.execute(text(f'DROP TABLE IF EXISTS {config["staging_table_name"]}'))
    
    metadata.create_all(engine)
    logging.info(f"Table {config['staging_table_name']} created successfully.")
    
    return engine


def fetch_reddit_posts(config):
    reddit = praw.Reddit(client_id=config['client_id'], 
                         client_secret=config['client_secret'], 
                         user_agent=config['user_agent'])
    
    logging.info(f"Fetching top {config['posts_limit']} posts from r/analog subreddit...")
    subreddit = reddit.subreddit('analog')
    top_posts = subreddit.top(time_filter='year', limit=config['posts_limit'])

    post_data = []
    total_posts = config['posts_limit']
    for i, post in enumerate(top_posts, start=1):
        created_utc = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
        
        data = {
            'title': post.title,
            'id': post.id,
            'score': post.score,
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'url': post.url,
            'author': post.author.name if post.author else 'N/A',
            'created_utc': created_utc.strftime('%Y-%m-%d %H:%M:%S'),
            'selftext': post.selftext,
            'subreddit': post.subreddit.display_name,
            'post_hint': post.post_hint if hasattr(post, 'post_hint') else None,
        }

        post_data.append(data)

        if i % 50 == 0 or i + 1 == total_posts:
            logging.info(f"Processed {i}/{total_posts} posts...")

    logging.info("Data fetched successfully from Reddit.")
    return post_data


def save_to_json(post_data, json_file_name):
    logging.info(f"Saving data to {json_file_name}...")
    with open(json_file_name, 'w') as json_file:
        json.dump(post_data, json_file, indent=4)
    logging.info(f"Data saved to {json_file_name}")


def insert_into_db(json_file_name, engine, table_name):
    logging.info(f"Loading data from {json_file_name} and inserting into database...")
    df = pd.read_json(json_file_name)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    logging.info(f"Data inserted into DB table {table_name}")


def main():
    config = load_config()
    engine = setup_database(config)

    try:
        # Fetch Reddit data
        post_data = fetch_reddit_posts(config)

        # Save data to JSON
        save_to_json(post_data, config['json_file_name'])

        # Insert JSON data into the database
        insert_into_db(config['json_file_name'], engine, config['staging_table_name'])

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()