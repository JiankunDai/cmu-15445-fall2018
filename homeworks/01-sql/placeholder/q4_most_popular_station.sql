select city, station_name most_popular_staion, max(cnt) visit_cout
from (
  select city, station_name, sid, count(sid) cnt
  from (
      select city, station_name, end_station_id as sid
      from trip, station
      where
        end_station_id = station_id
        and end_station_id != start_station_id
      union all
      select city, station_name, start_station_id as sid
      from trip, station
      where
        start_station_id = station_id
        and end_station_id != start_station_id
    )
  group by city, station_name
)
group by city
order by city asc;