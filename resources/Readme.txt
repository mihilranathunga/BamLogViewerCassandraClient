Add this jars to repository/components/libs folder


bam.logClient.logviewer-1.0.jar
cassandra-jdbc-1.1.1.wso2v1.jar
commons-validator-1.4.0.jar
json-simple-1.1.jar

Add the lines below to the log4j properties file in BAM

log4j.logger.compactor = info,stdout

# Direct log messages to stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.Target=System.out
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] BAM LOGCLIENT VIEWER: %5p %c - %x %m%n