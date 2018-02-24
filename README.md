# finddup
If you want to build or change the code, use sbt assembly to build again and it will create the pyth-assembly-0.1.jar again.
 
So, how to run?
 
Pre-requisite -> Definitely Java 1.8
 
If the user is having Scala then Scala 2.11 and above version is good. However, just to run the compiled code, use java you don't need Scala
 
Multiple ways to run (any of the options are good), here I have used 12gb as Xmx, if the user system is really good with respect to memory, please increase the number as higher the RAM the higher the performance, also change the Capacity object in code accordingly by adding 0 (zero's) based on your system. Also, change the baseDirectory argument accordingly, ( the below is  my example).
 
1) If they have imported the code in IDE, then from IDE directly run the FindDup, pass the base directory as argument 
and change the JVM -Xmx option.
(OR)
2) From command line, if Scala available
env JAVA_OPTS="-Xmx12288m" scala target/scala-2.12/pyth-assembly-0.1.jar "/Users/hari/Desktop"
(OR)
3) From command line, env JAVA_OPTS="-Xmx12288m" java -jar target/scala-2.12/pyth-assembly-0.1.jar "/Users/hari/Desktop"
 
You will see the output with ArrayBuffer i.e List of files which are duplicates, for e.g something like this, 
ArrayBuffer(/Users/hari/Desktop/samp/hari1.txt, /Users/hari/Desktop/samp/hari1 copy.txt)
ArrayBuffer(/Users/hari/Desktop/samp/test/bad.txt, /Users/hari/Desktop/samp/test/a/bad1.txt)

There are some other options, yeah sure, may be using ExecutorService, Java streams (map-reduce concept), debating on 
spark , also can the code to be improved, sure, by going full akka using AkkaStreams and server and reducing redundant code..