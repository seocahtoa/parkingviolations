java -version
yarn classpath
javac -classpath `yarn classpath` -d . CountRecsMapper.java

javac -classpath `yarn classpath` -d . CountRecsReducer.java

javac -classpath `yarn classpath`:. -d . CountRecs.java

jar -cvf CountRecs.jar *.class