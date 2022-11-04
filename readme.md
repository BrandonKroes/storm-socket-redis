# Storm - Redis benchmarking use Apache Storm

## Requirements
- Java
- Maven

## Inner-workings
- A redis host ip, redis port, data generator ip, data generator port and topolgy name are to be submitted to storm.
- A socket spout is opened.
- A timestamp is made when the first package arrives.
- A timestamp when it gets split into the relevant fields
- A timestamp is made when the package is ready for database storage
- All timestamps get combined into a collection and stored into a DB.

## Install
- Compile using `mvn clean package`

## Executables
- Copy the `.jars` to the `executables/` directory, if you don't the new version of the program can't be run.

