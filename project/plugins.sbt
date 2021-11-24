addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "spark-packages" at "https://repos.spark-packages.org/"

resolvers += "Maven Central Server" at "https://repo1.maven.org/maven2"

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
