set JAVA_HOME=D:\Program Files\Java\jdk1.8.0_73
set PATH=%JAVA_HOME%\bin;%PATH%
java -version
java -Djava.net.preferIPv4Stack=true -jar TwitterTotalJob.jar s3pdata1.properties twitter
