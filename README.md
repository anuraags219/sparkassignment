Aadhar & visit data Spark Maven Project
=======================================

A demonstration of a simple Spark project that uses Maven for building.  The app simply counts the
number of lines in a text file.

To build a JAR:

    mvn clean package assembly:single

To run on a cluster with Spark installed:

    spark-submit --class <fully classified class name> --master <yarn/local> \
      <path-to-jar-with-dependencies.jar> <input file>
