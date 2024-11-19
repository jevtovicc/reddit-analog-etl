# Project Overview  

## Goal  
The aim of this project was to extract data from the Reddit API, specifically from the **r/analog** subreddit, to uncover trends and patterns in analog photography. The focus was to:  
1. Identify the most commonly used film and camera brands.  
2. Assess how their use correlates with post scores (upvotes).  

This information could offer insights into popular gear choices and help inform future purchasing decisions.  

## Data Source  
The dataset was derived from the **top posts** in the **r/analog** subreddit, with a limit of 1000 posts from the previous year. Each post includes fields such as:  
- **Post title**  
- **Score** (upvotes)  
- **Author**  
- **Time of post** (`created_utc`)  
- Additional details (e.g., `num_comments`, `URL`, etc.)  

## Objective  
- **Identify** the most popular film and camera brands based on post count.  
- **Analyze** how these brands influence post scores.  
- **Present** a statistical understanding of popular equipment to guide future purchasing decisions.  

## Process  

### Data Extraction  
- Accessed the Reddit API to pull the top posts from the **r/analog** subreddit.  
- Stored raw data in a **PostgreSQL database** in the `reddit_posts_raw` table.  

### Data Processing  
1. **Name Extraction**:  
   - Camera and film names were extracted from post titles and descriptions using a **custom-trained SpaCy model**.  
   - Results were stored in the `reddit_posts_transformed` table.  

2. **Standardization**:  
   - Extracted names were mapped to standardized values using the **fuzzywuzzy library** for similarity calculations.  
   - Final results were stored in the `reddit_posts_final` table.  

3. **Aggregations**:  
   - Calculated post counts and average scores for each camera and film brand.  

### Analysis  
- Visualized the number of posts per camera and film brand to identify the most popular ones.  
- Calculated average scores to understand how post scores vary by equipment.  

## Results  

### Top Camera Brands  
The most frequently used camera brands were:  
- **Nikon**  
- **Canon**  
- **Mamiya**  
- **Pentax**  

### Top Film Brands  
- The most frequently used film brand was **Kodak**, accounting for almost **75%** of usage.  
- This dominance is expected since **Kodak Portra** is widely regarded as the most popular and versatile film stock among both professionals and amateurs.  

### Post Scores  
- **Leica**, **Olympus**, and **Fujica** cameras tended to receive the most upvotes, despite being less frequently used compared to Nikon and Canon.  
- Posts featuring **Fujifilm** or **Fomapan** film stocks received higher average upvotes, though these stocks are less commonly used due to their limited availability (out of production or harder to find).  
