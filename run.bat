set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.26.4-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%

set EXECUTION_JAR=target\dbunit-tools-0.0.1-SNAPSHOT.jar

set JAVA_AGENT_JAR=%~dp0..\magic-aop\target\magic-aop-0.0.1-SNAPSHOT.jar
set MAGIC_AOP_LIB_DIR=%~dp0..\magic-aop\target\libs
set MAGIC_AOP_CONFIG=magic-aop.config

for %%I in ("%EXECUTION_JAR%") do set EXECUTION_JAR=%%~fI

java -version


java -jar %EXECUTION_JAR%
pause
