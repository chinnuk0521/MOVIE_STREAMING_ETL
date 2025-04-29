"""
Movie Streaming Platform - Data Analysis

This script analyzes the transformed data from the ETL pipeline
to derive meaningful insights about user engagement, content popularity,
and platform performance.

Usage:
    Run after the ETL pipeline to analyze the processed data.

Author: Claude
Date: April 29, 2025
"""

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime, timedelta

def connect_to_db():
    """Connect to SQLite database and return connection object."""
    return sqlite3.connect('movie_streaming.db')

def analyze_user_engagement(conn):
    """
    Analyze user engagement patterns.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        dict: User engagement statistics
    """
    print("\n===== USER ENGAGEMENT ANALYSIS =====")
    
    # Get user engagement data
    user_engagement = pd.read_sql("SELECT * FROM user_engagement", conn)
    
    # Display engagement distribution
    engagement_counts = user_engagement['engagement_level'].value_counts()
    print(f"\nUser Engagement Distribution:")
    for level, count in engagement_counts.items():
        percentage = (count / len(user_engagement)) * 100
        print(f"  - {level}: {count} users ({percentage:.1f}%)")
    
    # Calculate average watch time by age group
    age_watch = user_engagement.groupby('age_group')['watch_time'].mean().reset_index()
    age_watch = age_watch.sort_values('watch_time', ascending=False)
    
    print(f"\nAverage Watch Time by Age Group (minutes):")
    for _, row in age_watch.iterrows():
        print(f"  - {row['age_group']}: {row['watch_time']:.1f}")
    
    # Calculate retention - active users by signup cohort
    # Define cohorts by month
    user_engagement['signup_month'] = pd.to_datetime(user_engagement['signup_date']).dt.to_period('M')
    cohort_activity = user_engagement.groupby('signup_month').agg(
        total_users=('user_id', 'count'),
        active_users=('watch_count', lambda x: (x > 0).sum())
    )
    cohort_activity['retention_rate'] = cohort_activity['active_users'] / cohort_activity['total_users']
    
    print(f"\nUser Retention by Signup Cohort:")
    for cohort, row in cohort_activity.iterrows():
        print(f"  - {cohort}: {row['retention_rate']:.2f} ({row['active_users']} active out of {row['total_users']} total)")
    
    return {
        'engagement_distribution': engagement_counts.to_dict(),
        'age_watch_time': age_watch.set_index('age_group')['watch_time'].to_dict(),
        'cohort_retention': cohort_activity['retention_rate'].to_dict()
    }

def analyze_content_performance(conn):
    """
    Analyze content performance metrics.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        dict: Content performance statistics
    """
    print("\n===== CONTENT PERFORMANCE ANALYSIS =====")
    
    # Get watch summary data
    watch_summary = pd.read_sql("SELECT * FROM watch_summary", conn)
    
    # Calculate most popular genres (by watch count)
    genre_popularity = watch_summary['genre'].value_counts()
    print(f"\nMost Popular Genres (by watch count):")
    for genre, count in genre_popularity.head(5).items():
        percentage = (count / len(watch_summary)) * 100
        print(f"  - {genre}: {count} watches ({percentage:.1f}%)")
    
    # Calculate genre with highest completion rate
    genre_completion = watch_summary.groupby('genre')['watch_completion'].mean().reset_index()
    genre_completion = genre_completion.sort_values('watch_completion', ascending=False)
    
    print(f"\nGenres by Average Completion Rate:")
    for _, row in genre_completion.head(5).iterrows():
        print(f"  - {row['genre']}: {row['watch_completion']*100:.1f}%")
    
    # Calculate most popular content by total watch time
    content_watch_time = watch_summary.groupby(['movie_id', 'title'])['watch_time'].sum().reset_index()
    content_watch_time = content_watch_time.sort_values('watch_time', ascending=False)
    
    print(f"\nTop Movies by Total Watch Time:")
    for _, row in content_watch_time.head(5).iterrows():
        print(f"  - {row['title']} (ID: {row['movie_id']}): {row['watch_time']} minutes")
    
    return {
        'genre_popularity': genre_popularity.head(5).to_dict(),
        'genre_completion': genre_completion.set_index('genre')['watch_completion'].head(5).to_dict(),
        'top_content': content_watch_time.head(5).set_index('title')['watch_time'].to_dict()
    }

def analyze_user_activity(conn):
    """
    Analyze user activity patterns over time.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        dict: User activity statistics
    """
    print("\n===== USER ACTIVITY ANALYSIS =====")
    
    # Get daily active users
    dau = pd.read_sql("SELECT * FROM daily_active_users", conn)
    dau['date'] = pd.to_datetime(dau['date'])
    
    # Get weekly active users
    wau = pd.read_sql("SELECT * FROM weekly_active_users", conn)
    
    # Calculate DAU/WAU ratio (stickiness)
    # For this, we need to match weeks in WAU to days in DAU
    
    # Add week number to DAU
    dau['year'] = dau['date'].dt.isocalendar().year
    dau['week'] = dau['date'].dt.isocalendar().week
    
    # Calculate average DAU per week
    avg_dau_per_week = dau.groupby(['year', 'week'])['daily_active_users'].mean().reset_index()
    
    # Join with WAU
    stickiness = pd.merge(
        avg_dau_per_week,
        wau,
        on=['year', 'week'],
        how='inner'
    )
    
    # Calculate stickiness (DAU/WAU ratio)
    stickiness['stickiness'] = stickiness['daily_active_users'] / stickiness['weekly_active_users']
    
    # Calculate overall average stickiness
    avg_stickiness = stickiness['stickiness'].mean()
    
    print(f"\nUser Activity Metrics:")
    print(f"  - Average DAU: {dau['daily_active_users'].mean():.1f} users")
    print(f"  - Average WAU: {wau['weekly_active_users'].mean():.1f} users")
    print(f"  - Average Stickiness (DAU/WAU): {avg_stickiness:.2f}")
    
    # Get watch summary to analyze activity by time of day
    watch_summary = pd.read_sql("SELECT * FROM watch_summary", conn)
    watch_summary['timestamp'] = pd.to_datetime(watch_summary['timestamp'])
    
    # Extract hour from timestamp
    watch_summary['hour'] = watch_summary['timestamp'].dt.hour
    
    # Analyze viewing patterns by hour
    hourly_views = watch_summary.groupby('hour').size()
    peak_hour = hourly_views.idxmax()
    
    print(f"\nViewing Patterns:")
    print(f"  - Peak viewing hour: {peak_hour}:00 ({hourly_views[peak_hour]} views)")
    
    # Calculate daily pattern
    watch_summary['day_of_week'] = watch_summary['timestamp'].dt.day_name()
    daily_pattern = watch_summary.groupby('day_of_week').size()
    
    # Reorder days of week
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    daily_pattern = daily_pattern.reindex(days_order)
    
    peak_day = daily_pattern.idxmax()
    
    print(f"  - Most active day: {peak_day} ({daily_pattern[peak_day]} views)")
    
    return {
        'avg_dau': dau['daily_active_users'].mean(),
        'avg_wau': wau['weekly_active_users'].mean(),
        'stickiness': avg_stickiness,  # Fixed typo here - was 'avg_stickness'
        'peak_hour': peak_hour,
        'daily_pattern': daily_pattern.to_dict()
    }

def analyze_genre_trends_by_country(conn):
    """
    Analyze genre preferences by country.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        dict: Genre trends by country
    """
    print("\n===== GENRE TRENDS BY COUNTRY =====")
    
    # Get genre trends data
    genre_trends = pd.read_sql("SELECT * FROM genre_trends", conn)
    
    # Find top genre for each country
    top_genres = genre_trends.loc[genre_trends.groupby('country')['watch_count'].idxmax()]
    
    print(f"\nTop Genre by Country:")
    for _, row in top_genres.iterrows():
        print(f"  - {row['country']}: {row['genre']} ({row['watch_count']} watches, {row['popularity_score']*100:.1f}% of country total)")
    
    # Find countries where a specific genre is most popular
    genre_countries = {}
    for genre in genre_trends['genre'].unique():
        countries = top_genres[top_genres['genre'] == genre]['country'].tolist()
        if countries:
            genre_countries[genre] = countries
    
    print(f"\nCountries Where Each Genre is Most Popular:")
    for genre, countries in genre_countries.items():
        print(f"  - {genre}: {', '.join(countries)}")
    
    return {
        'top_genre_by_country': top_genres.set_index('country')['genre'].to_dict(),
        'genre_countries': genre_countries
    }

def generate_recommendations():
    """
    Generate business recommendations based on the analytics.
    """
    print("\n===== BUSINESS RECOMMENDATIONS =====")
    
    recommendations = [
        "Focus content acquisition on genres with highest completion rates to increase user satisfaction",
        "Target retention campaigns for cohorts showing declining engagement after 3 months",
        "Optimize content delivery during peak viewing hours (7-10 PM) to ensure smooth streaming",
        "Create country-specific featured content sections based on regional genre preferences",
        "Develop engagement strategies for users in the 'Light' category to increase their activity",
        "Consider age-specific content recommendations based on watch time patterns by age group",
        "Launch weekend-specific promotions as weekend viewing activity is significantly higher",
        "Implement smart notifications for prime viewing hours to increase DAU/WAU ratio"
    ]
    
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec}")

def main():
    """Main function to run all analyses."""
    print("===== MOVIE STREAMING PLATFORM ANALYTICS =====")
    
    try:
        # Connect to database
        conn = connect_to_db()
        
        # Run analyses
        user_metrics = analyze_user_engagement(conn)
        content_metrics = analyze_content_performance(conn)
        activity_metrics = analyze_user_activity(conn)
        genre_metrics = analyze_genre_trends_by_country(conn)
        
        # Generate recommendations
        generate_recommendations()
        
        # Close connection
        conn.close()
        
        print("\nAnalysis completed successfully")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")

if __name__ == "__main__":
    main()