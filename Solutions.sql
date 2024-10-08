DROP TABLE IF EXISTS Amazon_Prime;
create table Amazon_Prime
(
	show_id		VARCHAR(50),
	type 	VARCHAR(50),
	title 	VARCHAR(300),
	director 	VARCHAR(500),
	"cast" 	VARCHAR(1000),
	country 	VARCHAR(100),
	date_added	 VARCHAR(20),
	release_year	INT,
	rating 		VARCHAR(50),
	duration	VARCHAR(30),
	listed_in	VARCHAR(200),
	description		VARCHAR(1500)
)

select * from amazon_prime;

-- 1. Count the Number of Movies vs TV Shows
SELECT 
    type,
    COUNT(*)
FROM amazon_prime
GROUP BY 1;

-- **Objective:** Determine the distribution of content types on Amazon prime.


-- 2. Find the Most Common Rating for Movies and TV Shows
WITH RatingCounts AS (
    SELECT 
        type,
        rating,
        COUNT(*) AS rating_count
    FROM amazon_prime
    GROUP BY type, rating
),
RankedRatings AS (
    SELECT 
        type,
        rating,
        rating_count,
        RANK() OVER (PARTITION BY type ORDER BY rating_count DESC) AS rank
    FROM RatingCounts
)
SELECT 
    type,
    rating AS most_frequent_rating
FROM RankedRatings
WHERE rank = 1;

-- **Objective:** Identify the most frequently occurring rating for each type of content.


-- 3. List All Movies Released in a Specific Year (e.g., 2020)
SELECT title , release_year
FROM amazon_prime
WHERE release_year = 2020;

-- **Objective:** Retrieve all movies released in a specific year.


-- 4. Find the Top 5 Countries with the Most Content on Amazon prime

SELECT * 
FROM
(
    SELECT 
        UNNEST(STRING_TO_ARRAY(country, ',')) AS country,
        COUNT(*) AS total_content
    FROM amazon_prime
    GROUP BY 1
) AS t1
WHERE country IS NOT NULL
ORDER BY total_content DESC
LIMIT 5;

-- **Objective:** Identify the top 5 countries with the highest number of content items.


-- 5. Select the longest movie duration

SELECT type, duration
FROM amazon_prime
WHERE CAST(SPLIT_PART(duration, ' ', 1) AS INT) = (
    SELECT MAX(CAST(SPLIT_PART(duration, ' ', 1) AS INT))
    FROM amazon_prime
    WHERE type = 'Movie'
)
AND type = 'Movie';



SELECT *
FROM amazon_prime
WHERE type = 'Movie'
ORDER BY SPLIT_PART(duration, ' ', 1)::INT DESC;


-- **Objective:** Find the movie with the longest duration.



-- 6. Find Content Added in the Last 5 Years

SELECT *
FROM amazon_prime
WHERE TO_DATE(date_added, 'DD-Mon-YY') >= CURRENT_DATE - INTERVAL '5 years';

-- **Objective:** Retrieve content added to Amazon prime in the last 5 years.


-- 7.Find All Movies/TV Shows by Director 'Rajiv Chilaka'

SELECT *
FROM (
    SELECT 
        *,
        UNNEST(STRING_TO_ARRAY(director, ',')) AS director_name
    FROM amazon_prime
) AS t
WHERE director_name = 'Rajiv Chilaka';

-- **Objective:** List all content directed by 'Rajiv Chilaka'.


-- 8. Find the most common director
SELECT director, COUNT(*) AS director_count
FROM amazon_prime
WHERE director IS NOT NULL
GROUP BY director
ORDER BY director_count DESC
LIMIT 1;

-- **Objective:** The most common director


-- 9. Find all the movie/tv show directed by 'Mark knight'

select type, title
from amazon_prime
where director = 'Mark Knight'

 -- list all content directed by 'Mark knight'


 -- 10.  List All TV Shows with More Than 5 Seasons

SELECT type, duration
FROM amazon_prime
WHERE type = 'Tv show'
  AND CAST(SPLIT_PART(duration, ' ', 1) AS INT) > 5;

SELECT *
FROM amazon_prime
WHERE type = 'Tv show'
  AND CAST(TRIM(SPLIT_PART(duration, ' ', 1)) AS INT) > 5;


SELECT type, duration
FROM amazon_prime
WHERE type = 'Tv show'
  AND CAST(SPLIT_PART(duration, ' ', 1) AS INT) > 5;


SELECT type, duration
FROM amazon_prime
WHERE type = 'Tv show'
  AND CAST(SPLIT_PART(
          REPLACE(duration, ' Season', ''), 
          ' ', 
          1) AS INT) > 5;
SELECT type, duration
FROM amazon_prime
WHERE type = 'Tv show'
  AND (
      (duration LIKE '%Season' AND CAST(REPLACE(duration, ' Season', '') AS INT) > 5)
      OR
      (duration LIKE '%Seasons' AND CAST(REPLACE(duration, ' Seasons', '') AS INT) > 5)
  );

SELECT type, duration
FROM amazon_prime
WHERE type = 'Tv show'
  AND (
      (duration LIKE '% Season' AND CAST(TRIM(REPLACE(duration, ' Season', '')) AS INT) > 5)
      OR
      (duration LIKE '% Seasons' AND CAST(TRIM(REPLACE(duration, ' Seasons', '')) AS INT) > 5)
  );


 -- **Objective:** Identify TV shows with more than 5 seasons.


 -- 11. Count the Number of Content Items in Each Genre

 SELECT 
    UNNEST(STRING_TO_ARRAY(listed_in, ',')) AS genre,
    COUNT(*) AS total_content
FROM amazon_prime
GROUP BY 1;

-- **Objective:** Count the number of content items in each genre.



-- 12. Find each year and the average numbers of content release in India on Amazon Prime.
-- Top 5 only

SELECT 
    country,
    release_year,
    COUNT(show_id) AS total_release,
    ROUND(
        COUNT(show_id)::numeric /
        (SELECT COUNT(show_id) FROM amazon_prime WHERE country = 'India')::numeric * 100, 2
    ) AS avg_release
FROM amazon_prime
WHERE country = 'India'
GROUP BY country, release_year
ORDER BY avg_release DESC
LIMIT 5;


-- **Objective:**  Calculate and rank years by the average number of content releases by India.


-- 13. List all content without director

SELECT * 
FROM amazon_prime
WHERE director IS NULL;

-- **Objective:** List content that does not have a director.


-- 14. Find the Top 10 Actors Who Have Appeared in the Highest Number of Movies Produced in India

SELECT 
    UNNEST(STRING_TO_ARRAY("cast", ',')) AS actor,
    COUNT(*)
FROM amazon_prime
WHERE country = 'India'
GROUP BY actor
ORDER BY COUNT(*) DESC
LIMIT 10;

-- **Objective:** Identify the top 10 actors with the most appearances in Indian-produced movies.


-- 15. Categorize Content Based on the Presence of 'Kill' and 'Violence' Keywords

SELECT 
    category,
    COUNT(*) AS content_count
FROM (
    SELECT 
        CASE 
            WHEN description ILIKE '%kill%' OR description ILIKE '%violence%' THEN 'Bad'
            ELSE 'Good'
        END AS category
    FROM amazon_prime
) AS categorized_content
GROUP BY category;


-- **Objective:**  Categorize content as 'Bad' if it contains 'kill' or 'violence' and 'Good' otherwise. Count the number of items in each category.
















