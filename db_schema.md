erDiagram
    watch_summary {
        string user_id
        string movie_id
        string title
        string genre
        float watch_time
        float duration
        float watch_completion
        string device
        datetime timestamp
    }
    
    user_engagement {
        string user_id
        string name
        float watch_time
        int watch_count
        string country
        string age_group
        datetime signup_date
        int days_since_signup
        float avg_daily_watch
        string engagement_level
    }
    
    genre_trends {
        string country
        string genre
        int watch_count
        float popularity_score
    }
    
    daily_active_users {
        date date
        int daily_active_users
    }
    
    weekly_active_users {
        int year
        int week
        int weekly_active_users
    }
    
    watch_summary ||--o{ user_engagement : "user_id"
    genre_trends ||--o{ user_engagement : "country"