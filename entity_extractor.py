import spacy
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
        'db_host': os.getenv('DB_HOST'),
        'db_port': os.getenv('DB_PORT'),
        'db_name': os.getenv('DB_NAME'),
        'db_user': os.getenv('DB_USER'),
        'transformed_table_name': 'reddit_posts_transformed',
        'spacy_model': 'my_model'
    }
    return config


def setup_database(config):
    engine = create_engine(f'postgresql://{config["db_user"]}@{config["db_host"]}:{config["db_port"]}/{config["db_name"]}')
    metadata = MetaData()

    Table(
        config['transformed_table_name'], metadata,
        Column('id', String(255), primary_key=True),
        Column('title', Text),
        Column('score', Integer),
        Column('upvote_ratio', Numeric),
        Column('num_comments', Integer),
        Column('url', String(255)),
        Column('author', String(255)),
        Column('created_utc', TIMESTAMP(timezone=True)),
        Column('selftext', Text),
        Column('post_hint', String(255)),
        Column('text', Text),
        Column('camera', String(255)),
        Column('film', String(255))
    )

    # Drop and recreate the table
    with engine.connect() as connection:
        logging.info(f"Dropping table {config['transformed_table_name']} if it exists...")
        connection.execute(text(f'DROP TABLE IF EXISTS {config["transformed_table_name"]}'))

    metadata.create_all(engine)
    logging.info(f"Table {config['transformed_table_name']} created successfully.")
    
    return engine


def load_db_data(engine):
    logging.info("Loading data from the database...")
    
    query = """
    SELECT *,
           CONCAT(title, ' ', selftext) as text
    FROM reddit_posts_raw
    WHERE post_hint = 'image';
    """

    df = pd.read_sql(query, engine)
    print(df.head())
    
    logging.info("Data loaded successfully from the database.")
    return df


def extract_entities(nlp, texts):
    cameras, films = [], []

    for t in texts:
        doc = nlp(t)
        logging.info(f"Processing text: {t}")

        camera, film = None, None

        for ent in doc.ents:
            if ent.label_ == 'CAMERA':
                camera = ent.text
            elif ent.label_ == 'FILM':
                film = ent.text
            logging.info(f" - {ent.text} ({ent.label_})")

        cameras.append(camera)
        films.append(film)

    logging.info("Entities extracted successfully.")
    return cameras, films


def insert_into_db(df, cameras, films, engine, config):
    df['camera'] = cameras
    df['film'] = films

    logging.info(f"Inserting transformed data into the {config['transformed_table_name']} table...")
    df.to_sql(config['transformed_table_name'], engine, if_exists='replace', index=False)
    logging.info("Data inserted successfully.")


def main():
    config = load_config()
    engine = setup_database(config)

    try:
        # Load spaCy model
        logging.info(f"Loading spaCy model {config['spacy_model']}...")
        nlp = spacy.load(config['spacy_model'])

        # Load Reddit post data from the database instead of JSON
        df = load_db_data(engine)

        # Extract text data for entity recognition
        texts = df['text'].tolist()

        # Extract camera and film entities
        cameras, films = extract_entities(nlp, texts)

        # Insert transformed data into the database
        insert_into_db(df, cameras, films, engine, config)

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()