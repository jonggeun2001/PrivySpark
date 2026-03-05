plugins {
    scala
    application
    id("com.gradleup.shadow") version "9.0.0"
}

group = "io.github.jonggeun2001"
version = "0.1.0"

repositories {
    mavenCentral()
}

val scalaVersion = "2.12.19"
val sparkVersion = "3.5.3"
val scalaTestVersion = "3.2.19"
val jvmTarget = "1.8"
val javaRelease = "8"

dependencies {
    implementation("org.scala-lang:scala-library:$scalaVersion")

    compileOnly("org.apache.spark:spark-sql_2.12:$sparkVersion")
    compileOnly("org.apache.spark:spark-core_2.12:$sparkVersion")

    implementation("com.github.scopt:scopt_2.12:4.1.0")
    implementation("org.yaml:snakeyaml:2.2")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.scalatest:scalatest_2.12:$scalaTestVersion")
    testImplementation("org.scalatestplus:junit-4-13_2.12:3.2.19.0")
    testImplementation("org.apache.spark:spark-sql_2.12:$sparkVersion")
}

application {
    mainClass.set("io.github.jonggeun2001.privyspark.PrivySparkApp")
}

tasks.withType<ScalaCompile>().configureEach {
    scalaCompileOptions.additionalParameters = listOf(
        "-deprecation",
        "-feature",
        "-unchecked",
        "-target:jvm-$jvmTarget",
        "-release:$javaRelease",
    )
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(8)
}

tasks.test {
    useJUnit()
    jvmArgs(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
    )
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "io.github.jonggeun2001.privyspark.PrivySparkApp"
    }
}

tasks.shadowJar {
    archiveClassifier.set("all")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "io.github.jonggeun2001.privyspark.PrivySparkApp"
    }
}
