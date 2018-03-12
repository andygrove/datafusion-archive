CREATE EXTERNAL TABLE uk_cities (city VARCHAR(100), lat DOUBLE, lng DOUBLE)
SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities WHERE lat < 53
SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities WHERE lat >= 53