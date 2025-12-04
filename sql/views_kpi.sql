CREATE OR REPLACE VIEW dw.vw_sentiment_distribution AS
SELECT 
    ds.sentiment_label,
    ds.sentiment_color,
    COUNT(fr.sentiment) AS total_reviews,
    ROUND(
        (COUNT(fr.sentiment)::numeric / (SELECT COUNT(*) FROM dw.fact_reviews)) * 100,
        2
    ) AS percentage
FROM dw.fact_reviews fr
LEFT JOIN dw.dim_sentiment ds ON LOWER(fr.sentiment) = LOWER(ds.sentiment_label)
GROUP BY ds.sentiment_label, ds.sentiment_color
ORDER BY total_reviews DESC;

CREATE OR REPLACE VIEW dw.vw_rating_distribution AS
SELECT 
    dr.rating_label,
    dr.satisfaction_level,
    COUNT(fr.rating) AS total_reviews,
    ROUND(
        (COUNT(fr.rating)::numeric / (SELECT COUNT(*) FROM dw.fact_reviews)) * 100,
        2
    ) AS percentage
FROM dw.fact_reviews fr
LEFT JOIN dw.dim_rating dr ON fr.rating = dr.rating_value
GROUP BY dr.rating_label, dr.satisfaction_level, dr.rating_value
ORDER BY dr.rating_value DESC;

CREATE OR REPLACE VIEW dw.vw_day_distribution AS
SELECT
    TRIM(fr.day_name) AS day_name,
    fr.day_number,
    COUNT(*) AS total_reviews,
    MODE() WITHIN GROUP (ORDER BY dr.rating_label) AS most_common_rating
FROM dw.fact_reviews fr
LEFT JOIN dw.dim_rating dr ON fr.rating = dr.rating_value
WHERE fr.day_number BETWEEN 1 AND 7
GROUP BY TRIM(fr.day_name), fr.day_number
ORDER BY fr.day_number;


CREATE OR REPLACE VIEW dw.vw_day_rating_distribution AS
SELECT
    TRIM(fr.day_name) AS day_name,
    fr.day_number,
    SUM(CASE WHEN fr.rating = 5 THEN 1 END) AS excellent,
    SUM(CASE WHEN fr.rating = 4 THEN 1 END) AS good,
    SUM(CASE WHEN fr.rating = 3 THEN 1 END) AS average,
    SUM(CASE WHEN fr.rating = 2 THEN 1 END) AS poor,
    SUM(CASE WHEN fr.rating = 1 THEN 1 END) AS terrible,
    COUNT(*) AS total_reviews
FROM dw.fact_reviews fr
WHERE fr.day_number BETWEEN 1 AND 7
GROUP BY TRIM(fr.day_name), fr.day_number
ORDER BY fr.day_number;


CREATE OR REPLACE VIEW dw.vw_year_rating_distribution AS
SELECT
    EXTRACT(YEAR FROM fr.date) AS year,
    COALESCE(SUM(CASE WHEN fr.rating = 5 THEN 1 END), 0) AS excellent,
    COALESCE(SUM(CASE WHEN fr.rating = 4 THEN 1 END), 0) AS good,
    COALESCE(SUM(CASE WHEN fr.rating = 3 THEN 1 END), 0) AS average,
    COALESCE(SUM(CASE WHEN fr.rating = 2 THEN 1 END), 0) AS poor,
    COALESCE(SUM(CASE WHEN fr.rating = 1 THEN 1 END), 0) AS terrible,
    COUNT(*) AS total_reviews
FROM dw.fact_reviews fr
WHERE fr.date >= '2019-01-01'
GROUP BY EXTRACT(YEAR FROM fr.date)
ORDER BY year;
