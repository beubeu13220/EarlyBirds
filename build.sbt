name := "EarlyBirdsAB"

version := "1.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.0.0"

// Dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
)


// Assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly :=  s"${name.value}-${version.value}.jar"