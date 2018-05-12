CREATE EXTERNAL TABLE uk_cities (city VARCHAR(100), lat DOUBLE, lng DOUBLE) STORED AS CSV WITHOUT HEADER ROW LOCATION '/test/data/uk_cities.csv';
SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities WHERE lat < 53.0;
SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities WHERE lat >= 53.0;