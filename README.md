# KillrVideo CSV-to-OpenSearch
## Data loader
This project takes a CSV file and loads it into a like-named index in OpenSearch.

### Requirements
 - Java 25
 - Maven

### Building
```
mvn clean package
```

### Environment
```
export OPENSEARCH_HOST=openSearcHostname
export OPENSEARCH_PASSWORD=openSearchPassword
export OPENSEARCH_USERNAME=openSearchUsername
```

### Running
To load the KillrVideo-data [videos.csv](https://github.com/KillrVideo/killrvideo-data/blob/master/data/csv/videos.csv) file into an OpenSearch index named "videos", first copy `videos.csv` into the project's local `data/` dir. Then run:
```
java -jar target/csv-to-opensearch-1.0-SNAPSHOT.jar videos.csv
```
