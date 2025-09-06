set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.26.4-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%

java -version
call mvn clean install
pause
