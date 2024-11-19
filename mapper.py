from fuzzywuzzy import process
from sqlalchemy import create_engine
import pandas as pd
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
        'posts_table': 'reddit_posts_transformed',
        'final_table': 'reddit_posts_final',
        'camera_table': 'cameras',
        'film_table': 'films',
    }
    return config


def setup_database(config):
    logging.info("Setting up database connection...")
    engine = create_engine(f'postgresql://{config["db_user"]}@{config["db_host"]}:{config["db_port"]}/{config["db_name"]}')
    return engine


def load_known_values_with_ids(engine, table_name, id_field, concatenation_fields):
    logging.info(f"Loading known values from {table_name}...")
    query = f"SELECT {id_field}, {', '.join(concatenation_fields)} FROM {table_name}"
    df = pd.read_sql(query, engine)
    known_values = df.apply(lambda row: (row[id_field], " ".join([str(row[field]) for field in concatenation_fields])), axis=1).tolist()
    return known_values


def map_to_best_match_id(item_name, known_list, min_score=60):
    if pd.isna(item_name):
        return None
    best_match, score = process.extractOne(item_name, [(item[1], item[0]) for item in known_list])
    return best_match[1] if score >= min_score else None


def map_post_data(engine, config):
    logging.info(f"Loading posts from {config['posts_table']}...")
    df_posts = pd.read_sql(f"SELECT * FROM {config['posts_table']}", engine)

    logging.info("Loading known cameras and films...")
    known_cameras = load_known_values_with_ids(engine, config['camera_table'], 'id', ['manufacturer', 'model'])
    known_films = load_known_values_with_ids(engine, config['film_table'], 'id', ['brand', 'name'])

    logging.info("Mapping camera and film names to IDs...")
    df_posts['mapped_camera_id'] = df_posts['camera'].apply(lambda camera: map_to_best_match_id(camera, known_cameras)).astype(pd.Int64Dtype())
    df_posts['mapped_film_id'] = df_posts['film'].apply(lambda film: map_to_best_match_id(film, known_films)).astype(pd.Int64Dtype())

    logging.info("Mapping completed.")
    return df_posts


def save_mapped_data(df_posts, engine, config):
    logging.info(f"Saving mapped data to {config['final_table']}...")
    df_posts.to_sql(config['final_table'], engine, if_exists='replace', index=False)
    logging.info(f"Data saved to {config['final_table']}")


def main():
    try:
        config = load_config()
        engine = setup_database(config)

        # Perform mapping of Reddit posts
        df_mapped_posts = map_post_data(engine, config)

        # Save mapped data to the database
        save_mapped_data(df_mapped_posts, engine, config)

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()