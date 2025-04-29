"""
Movie Streaming Platform ETL Pipeline

This script implements a complete ETL (Extract, Transform, Load) pipeline for
processing movie streaming platform data from multiple sources:
- User watch logs
- Movie metadata
- User profiles

The pipeline:
1. Extracts data from CSV files
2. Performs cleaning and transformation
3. Calculates engagement metrics and analytics
4. Loads results into SQL tables

Author: Claude
Date: April 29, 2025
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('movie_streaming_etl')

# Define paths (these would normally be config parameters)
DATA_DIR = 'data'
DB_PATH = 'movie_streaming.db'

def extract_data():
    """
    Extract data from CSV files.
    
    Returns:
        tuple: (watch_logs_df, movies_df, users_df)
    """
    logger.info("Starting data extraction...")
    
    try:
        # Load watch logs
        watch_logs_df = pd.read_csv(f"{DATA_DIR}/watch_logs.csv")
        logger.info(f"Loaded {len(watch_logs_df)} watch log records")
        
        # Load movie metadata
        movies_df = pd.read_csv(f"{DATA_DIR}/movies.csv")
        logger.info(f"Loaded {len(movies_df)} movie records")
        
        # Load user data
        users_df = pd.read_csv(f"{DATA_DIR}/users.csv")
        logger.info(f"Loaded {len(users_df)} user records")
        
        return watch_logs_df, movies_df, users_df
    
    except Exception as e:
        logger.error(f"Error during data extraction: {str(e)}")
        raise
        
def clean_data(watch_logs_df, movies_df, users_df):
    """
    Clean raw data by handling nulls, data types, and formatting issues.
    
    Args:
        watch_logs_df (DataFrame): Raw watch logs
        movies_df (DataFrame): Raw movie metadata
        users_df (DataFrame): Raw user data
        
    Returns:
        tuple: Cleaned dataframes (watch_logs_df, movies_df, users_df)
    """
    logger.info("Starting data cleaning...")
    
    try:
        # Clean watch logs
        # Convert timestamp to datetime
        watch_logs_df['timestamp'] = pd.to_datetime(watch_logs_df['timestamp'])
        # Handle missing values in watch_time (set to 0 for tracking)
        watch_logs_df['watch_time'] = watch_logs_df['watch_time'].fillna(0)
        # Drop rows with missing critical fields (user_id, movie_id)
        watch_logs_df = watch_logs_df.dropna(subset=['user_id', 'movie_id'])
        
        # Clean movie metadata
        # Set unknown genres to 'Unknown'
        movies_df['genre'] = movies_df['genre'].fillna('Unknown')
        # Ensure duration is numeric
        movies_df['duration'] = pd.to_numeric(movies_df['duration'], errors='coerce')
        # Fill missing durations with genre median
        genre_median_duration = movies_df.groupby('genre')['duration'].transform('median')
        movies_df['duration'] = movies_df['duration'].fillna(genre_median_duration)
        # If still NaN, fill with overall median
        movies_df['duration'] = movies_df['duration'].fillna(movies_df['duration'].median())
        
        # Clean user data
        # Convert signup_date to datetime
        users_df['signup_date'] = pd.to_datetime(users_df['signup_date'])
        # Handle missing country values
        users_df['country'] = users_df['country'].fillna('Unknown')
        # Handle missing age_group
        users_df['age_group'] = users_df['age_group'].fillna('Unknown')
        
        logger.info("Data cleaning completed successfully")
        return watch_logs_df, movies_df, users_df
    
    except Exception as e:
        logger.error(f"Error during data cleaning: {str(e)}")
        raise

def transform_data(watch_logs_df, movies_df, users_df):
    """
    Transform and enrich the data to create analytical views.
    
    Args:
        watch_logs_df (DataFrame): Cleaned watch logs
        movies_df (DataFrame): Cleaned movie metadata
        users_df (DataFrame): Cleaned user data
        
    Returns:
        tuple: Transformed dataframes (watch_summary_df, user_engagement_df, genre_trends_df)
    """
    logger.info("Starting data transformation...")
    
    try:
        # 1. Join datasets
        # Merge watch logs with movies
        enriched_logs = pd.merge(
            watch_logs_df,
            movies_df,
            on='movie_id',
            how='left'
        )
        
        # Merge with user data
        full_data = pd.merge(
            enriched_logs,
            users_df,
            on='user_id',
            how='left'
        )
        
        # 2. Calculate watch completion percentage
        full_data['watch_completion'] = (full_data['watch_time'] / full_data['duration']).clip(0, 1)
        
        # 3. Create watch summary dataframe
        watch_summary_df = full_data[['user_id', 'movie_id', 'title', 'genre', 'watch_time', 
                                      'duration', 'watch_completion', 'device', 'timestamp']]
        
        # 4. Calculate user engagement metrics
        # Calculate total watch time per user
        user_total_watch = full_data.groupby('user_id')['watch_time'].sum().reset_index()
        
        # Calculate watch count per user
        user_watch_count = full_data.groupby('user_id').size().reset_index(name='watch_count')
        
        # Merge user metrics
        user_engagement_df = pd.merge(
            user_total_watch,
            user_watch_count,
            on='user_id',
            how='outer'
        )
        
        # Add user details
        user_engagement_df = pd.merge(
            user_engagement_df,
            users_df[['user_id', 'name', 'country', 'age_group', 'signup_date']],
            on='user_id',
            how='left'
        )
        
        # Calculate days since signup
        today = datetime.now()
        user_engagement_df['days_since_signup'] = (today - user_engagement_df['signup_date']).dt.days
        
        # Calculate average daily watch time
        user_engagement_df['avg_daily_watch'] = user_engagement_df['watch_time'] / user_engagement_df['days_since_signup']
        user_engagement_df['avg_daily_watch'] = user_engagement_df['avg_daily_watch'].fillna(0)
        
        # Categorize users by engagement level
        engagement_bins = [0, 30, 120, float('inf')]
        engagement_labels = ['Light', 'Medium', 'Heavy']
        user_engagement_df['engagement_level'] = pd.cut(
            user_engagement_df['avg_daily_watch'],
            bins=engagement_bins,
            labels=engagement_labels
        )
        
        # 5. Calculate genre trends by country
        # Count watches by genre and country
        genre_country_counts = full_data.groupby(['country', 'genre']).size().reset_index(name='watch_count')
        
        # Find top genre per country
        country_top_genre = genre_country_counts.loc[
            genre_country_counts.groupby('country')['watch_count'].idxmax()
        ]
        
        # Calculate genre popularity score (normalized by country)
        genre_country_normalized = genre_country_counts.copy()
        country_totals = genre_country_counts.groupby('country')['watch_count'].sum().reset_index()
        
        genre_country_normalized = pd.merge(
            genre_country_normalized,
            country_totals,
            on='country',
            suffixes=('', '_total')
        )
        
        genre_country_normalized['popularity_score'] = genre_country_normalized['watch_count'] / genre_country_normalized['watch_count_total']
        
        # Final genre trends dataframe
        genre_trends_df = genre_country_normalized[['country', 'genre', 'watch_count', 'popularity_score']]
        
        # 6. Calculate DAU/WAU
        # Add date column for easier grouping
        full_data['date'] = full_data['timestamp'].dt.date
        
        # Calculate daily active users
        dau = full_data.groupby('date')['user_id'].nunique().reset_index(name='daily_active_users')
        
        # Calculate week number for WAU
        full_data['week'] = full_data['timestamp'].dt.isocalendar().week
        full_data['year'] = full_data['timestamp'].dt.isocalendar().year
        
        # Calculate weekly active users
        wau = full_data.groupby(['year', 'week'])['user_id'].nunique().reset_index(name='weekly_active_users')
        
        # Add DAU/WAU to genre_trends_df as additional info
        # (In a real scenario this would likely be its own table)
        
        logger.info("Data transformation completed successfully")
        return watch_summary_df, user_engagement_df, genre_trends_df, dau, wau
    
    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}")
        raise

def load_data(watch_summary_df, user_engagement_df, genre_trends_df, dau_df, wau_df):
    """
    Load transformed data into a SQLite database.
    
    Args:
        watch_summary_df (DataFrame): Watch summary data
        user_engagement_df (DataFrame): User engagement metrics
        genre_trends_df (DataFrame): Genre popularity by country
        dau_df (DataFrame): Daily active users
        wau_df (DataFrame): Weekly active users
    """
    logger.info(f"Starting data loading to {DB_PATH}...")
    
    try:
        # Create connection to SQLite database
        conn = sqlite3.connect(DB_PATH)
        
        # Load watch summary
        watch_summary_df.to_sql('watch_summary', conn, if_exists='replace', index=False)
        logger.info(f"Loaded {len(watch_summary_df)} records to watch_summary table")
        
        # Load user engagement
        user_engagement_df.to_sql('user_engagement', conn, if_exists='replace', index=False)
        logger.info(f"Loaded {len(user_engagement_df)} records to user_engagement table")
        
        # Load genre trends
        genre_trends_df.to_sql('genre_trends', conn, if_exists='replace', index=False)
        logger.info(f"Loaded {len(genre_trends_df)} records to genre_trends table")
        
        # Load DAU/WAU
        dau_df.to_sql('daily_active_users', conn, if_exists='replace', index=False)
        logger.info(f"Loaded {len(dau_df)} records to daily_active_users table")
        
        wau_df.to_sql('weekly_active_users', conn, if_exists='replace', index=False)
        logger.info(f"Loaded {len(wau_df)} records to weekly_active_users table")
        
        # Close connection
        conn.close()
        
        logger.info("Data loading completed successfully")
    
    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}")
        raise

def generate_sample_data():
    """
    Generate sample data for testing when real data is unavailable.
    Creates CSV files in the data directory.
    """
    logger.info("Generating sample data for testing...")
    
    # Create data directory if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Generate random user IDs
    user_ids = [f"U{i:04d}" for i in range(1, 1001)]
    
    # Generate random movie IDs
    movie_ids = [f"M{i:04d}" for i in range(1, 201)]
    
    # Generate movies.csv
    genres = ['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Horror', 'Documentary', 'Animation']
    movies_data = {
        'movie_id': movie_ids,
        'title': [f"Movie Title {i}" for i in range(1, 201)],
        'genre': np.random.choice(genres, 200),
        'duration': np.random.randint(60, 180, 200),
        'release_year': np.random.randint(2000, 2025, 200)
    }
    movies_df = pd.DataFrame(movies_data)
    movies_df.to_csv(f"{DATA_DIR}/movies.csv", index=False)
    
    # Generate users.csv
    countries = ['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Japan', 'Brazil', 'India']
    age_groups = ['13-17', '18-24', '25-34', '35-44', '45-54', '55+']
    
    # Create start date for user signups (3 years ago)
    start_date = datetime.now() - timedelta(days=3*365)
    
    # Generate random signup dates
    signup_dates = [start_date + timedelta(days=np.random.randint(0, 3*365)) for _ in range(1000)]
    
    users_data = {
        'user_id': user_ids,
        'name': [f"User {i}" for i in range(1, 1001)],
        'signup_date': signup_dates,
        'country': np.random.choice(countries, 1000),
        'age_group': np.random.choice(age_groups, 1000)
    }
    users_df = pd.DataFrame(users_data)
    users_df.to_csv(f"{DATA_DIR}/users.csv", index=False)
    
    # Generate watch_logs.csv
    # Create 10,000 watch events
    devices = ['Mobile', 'Tablet', 'Smart TV', 'Desktop', 'Laptop', 'Console']
    
    # Generate timestamps within the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    timestamps = [start_date + timedelta(
        days=np.random.random() * 30,
        hours=np.random.random() * 24
    ) for _ in range(10000)]
    
    watch_logs_data = {
        'user_id': np.random.choice(user_ids, 10000),
        'movie_id': np.random.choice(movie_ids, 10000),
        'watch_time': np.random.randint(5, 180, 10000),
        'device': np.random.choice(devices, 10000),
        'timestamp': timestamps
    }
    watch_logs_df = pd.DataFrame(watch_logs_data)
    watch_logs_df.to_csv(f"{DATA_DIR}/watch_logs.csv", index=False)
    
    logger.info("Sample data generated successfully")


def run_etl_pipeline():
    """
    Execute the complete ETL pipeline.
    """
    logger.info("Starting ETL pipeline execution...")
    
    try:
        # Check if data exists, if not generate sample data
        if not (os.path.exists(f"{DATA_DIR}/watch_logs.csv") and 
                os.path.exists(f"{DATA_DIR}/movies.csv") and 
                os.path.exists(f"{DATA_DIR}/users.csv")):
            generate_sample_data()
        
        # Extract data
        watch_logs_df, movies_df, users_df = extract_data()
        
        # Clean data
        clean_logs_df, clean_movies_df, clean_users_df = clean_data(watch_logs_df, movies_df, users_df)
        
        # Transform data
        watch_summary_df, user_engagement_df, genre_trends_df, dau_df, wau_df = transform_data(
            clean_logs_df, clean_movies_df, clean_users_df
        )
        
        # Load data
        load_data(watch_summary_df, user_engagement_df, genre_trends_df, dau_df, wau_df)
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    run_etl_pipeline()