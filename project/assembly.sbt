resolvers ++= Seq(
  "Sbt plugins"  at "https://dl.bintray.com/sbt/sbt-plugin-releases"

)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")