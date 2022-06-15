
# postgres
brew install postgres
pg_ctl -D /usr/local/var/postgres start # stop to stop
# psql postgres
# CREATE ROLE wds WITH LOGIN PASSWORD 'wds';
# CREATE DATABASE wds;
# GRANT ALL PRIVILEGES ON DATABASE wds TO wds;

# gradle
sdkman install gradle
gradle bootRun

# gradle alternative if we commit ./gradlew: ./gradlew bootRun
