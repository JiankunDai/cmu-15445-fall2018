(wget -qO- https://15445.courses.cs.cmu.edu/fall2018/files/bike_sharing.tar.gz | tar xvz) &&
(sqlite3 bike_sharing.db < setup.sql)