# Build from source
- clone from cdap-build

- run 
  >  mvn clean package -DskipTests

- follow this guide from [cdap](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/480346208/Development+Environment+Setup)  and use JDK 8 to build
- start the ui by run 
  > nvm install 10.16.2

  > npm run prestart & npm run start
# Build from docker
- pull from docker 
  > docker pull caskdata/cdap-sandbox:6.5.0
  
- create container
  > docker run -d --name cdap-sandbox -p 11011:11011 -p 11015:11015 caskdata/cdap-sandbox:6.5.0