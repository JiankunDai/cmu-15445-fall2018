with t as (select count(1) as total from (select distinct start_station_id, end_station_id from trip))
select city, ROUND(
    count(city) / (t.total + .0),
    4
  ) as ratio
from (
  select distinct city, sid
  from (
      select city, end_station_id as sid
      from trip, station
      where end_station_id = station_id and end_station_id != start_station_id
    union
      select city, start_station_id as sid
      from trip, station
      where start_station_id = station_id and end_station_id != start_station_id
  )
), t
group by city
order by ratio desc, city;
