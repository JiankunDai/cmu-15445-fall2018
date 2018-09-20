select city, count(station_id) cnt
from station 
group by city
order by cnt, city;