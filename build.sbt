name := "finddup"

version := "0.1"


lazy val akkaVersion = "2.5.3"



resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion

)