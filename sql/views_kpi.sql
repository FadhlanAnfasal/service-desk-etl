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
    fr.day_name,
    fr.day_number,
    COUNT(fr.review_id) AS total_reviews,
    MODE() WITHIN GROUP (ORDER BY fr.rating_label) AS most_common_rating
FROM dw.fact_reviews fr
GROUP BY fr.day_name, fr.day_number
ORDER BY fr.day_number;

CREATE OR REPLACE VIEW dw.vw_day_rating_distribution AS
SELECT
    fr.day_name,
    fr.day_number,
    SUM(CASE WHEN fr.rating_label = 'Excellent' THEN 1 ELSE 0 END) AS excellent,
    SUM(CASE WHEN fr.rating_label = 'Good' THEN 1 ELSE 0 END) AS good,
    SUM(CASE WHEN fr.rating_label = 'Average' THEN 1 ELSE 0 END) AS average,
    SUM(CASE WHEN fr.rating_label = 'Poor' THEN 1 ELSE 0 END) AS poor,
    SUM(CASE WHEN fr.rating_label = 'Terrible' THEN 1 ELSE 0 END) AS terrible,
    COUNT(*) AS total_reviews
FROM dw.fact_reviews fr
GROUP BY fr.day_name, fr.day_number
ORDER BY fr.day_number;