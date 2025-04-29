# Movie Streaming Platform Analytics

## Overview

This project provides comprehensive analytics and visualization tools for a movie streaming platform. It processes user engagement data, content performance metrics, user activity patterns, and regional genre preferences to generate actionable business insights.

## Features

- **User Engagement Analysis**: Analyze user engagement distribution, watch time by age group, and cohort retention
- **Content Performance Analysis**: Identify popular genres, completion rates, and top-performing movies
- **User Activity Patterns**: Track DAU/WAU metrics, platform "stickiness", and peak viewing times
- **Regional Trends**: Analyze genre preferences by country
- **Visualization**: Generate comprehensive charts and graphs for all metrics
- **Dashboard**: Create an HTML dashboard with all visualizations
- **Executive Reporting**: Export key metrics to Excel for stakeholder presentations
- **Business Recommendations**: Generate data-driven recommendations for platform improvement

## Requirements

- Python 3.6+
- Required Python packages:
  - pandas
  - sqlite3
  - matplotlib
  - seaborn
  - numpy
  - openpyxl (optional, for Excel export)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/movie-streaming-analytics.git
   cd movie-streaming-analytics
   ```

2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Running the Analysis

The main analysis script processes the data in the SQLite database and generates all charts and reports:

```
python data_analysis.py
```

This will:
1. Connect to the SQLite database
2. Perform all analyses
3. Generate visualizations in the `charts` directory
4. Create an HTML dashboard
5. Generate business recommendations
6. Save a summary report in JSON format
7. Export key metrics to Excel (if openpyxl is installed)

### Project Structure

```
movie_streaming_analytics/
├── data_analysis.py        # Main analysis script
├── movie_streaming.db      # SQLite database with streaming data
├── charts/                 # Generated visualization charts
│   ├── user_engagement_distribution.png
│   ├── watch_time_by_age.png
│   ├── cohort_retention.png
│   └── ...
├── movie_streaming_dashboard.html    # Generated HTML dashboard
├── analysis_report.json              # Analysis results in JSON format
├── movie_streaming_executive_summary.xlsx  # Excel report for executives
└── README.md                         # Project documentation
```

## Data Model

The analysis script works with a SQLite database containing the following tables:

1. **user_engagement**: User-level engagement metrics
   - user_id, engagement_level, watch_time, age_group, signup_date, watch_count

2. **watch_summary**: Content viewing information
   - movie_id, title, genre, watch_time, watch_completion, timestamp

3. **daily_active_users**: Daily active user metrics
   - date, daily_active_users

4. **weekly_active_users**: Weekly active user metrics
   - year, week, weekly_active_users

5. **genre_trends**: Genre popularity by country
   - country, genre, watch_count, popularity_score

## Generated Analytics

### User Engagement Analysis
- Distribution of users by engagement level (Light, Medium, Heavy)
- Average watch time by age group
- User retention rates by signup cohort

### Content Performance Analysis
- Most popular genres by watch count
- Genres by average completion rate
- Top movies by total watch time

### User Activity Analysis
- Daily and weekly active user trends
- Platform stickiness (DAU/WAU ratio)
- Viewing patterns by hour of day and day of week

### Genre Trends by Country
- Top genre by country
- Heatmap of genre popularity across countries
- Countries where each genre is most popular

## Generated Files

### Charts
The script generates multiple visualization charts saved in the `charts` directory:
- User engagement distribution
- Watch time by age group
- Cohort retention rates
- Genre popularity
- Genre completion rates
- Top movies by watch time
- DAU and WAU trends
- Stickiness trend
- Hourly and daily activity patterns
- Genre-country heatmap
- Top genre by country

### Dashboard
An HTML dashboard (`movie_streaming_dashboard.html`) is generated with all charts organized into sections:
- User Engagement
- Content Performance
- User Activity
- Genre Trends by Country
- Business Recommendations

### Reports
- `analysis_report.json`: Complete analysis results in JSON format
- `movie_streaming_executive_summary.xlsx`: Excel report with key metrics for executives

## Troubleshooting

### Common Issues

1. **Error with week formatting**: 
   - Error: `Unknown format code 'd' for object of type 'float'`
   - Solution: Convert week numbers to integers before formatting: `int(row['week'])`

2. **JSON serialization error**:
   - Error: `keys must be str, int, float, bool or None, not Period`
   - Solution: Convert pandas Period objects to strings before serializing to JSON

## Future Enhancements

1. Add predictive analytics for user churn
2. Implement A/B test analysis functionality
3. Create interactive dashboards using Plotly or Dash
4. Add content recommendation algorithms based on viewing patterns
5. Integrate real-time analytics processing

## License

[MIT License](LICENSE)

## Contributors

- [Chandu Kalluru](https://github.com/chinnuk0521)

## Acknowledgments

- Data visualization inspired by best practices from [Storytelling with Data](http://www.storytellingwithdata.com/)
- Dashboard design influenced by Netflix's internal analytics tools