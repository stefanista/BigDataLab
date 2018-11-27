package graphframes-3e6cfe8fec757e76fa041b8f96f2731cae77a353.project

// You may use this file to add plugin dependencies for sbt.
resolvers += "Spark Packages repo" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.2.6")

// scalacOptions in (Compile,doc) := Seq("-groups", "-implicits")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")
