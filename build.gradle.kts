import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.bundling.Zip
import org.gradle.api.tasks.testing.Test

plugins {
    scala
    application
    id("com.gradleup.shadow") version "9.0.0"
}

group = "io.github.jonggeun2001"
version = "1.4.6"

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

    implementation("org.apache.spark:spark-avro_2.12:$sparkVersion")
    implementation("com.crealytics:spark-excel_2.12:3.5.1_0.20.4")
    implementation("org.apache.commons:commons-compress:1.26.2")
    implementation("org.tukaani:xz:1.10")
    implementation("com.github.luben:zstd-jni:1.5.6-3")
    implementation("com.github.junrar:junrar:7.5.5")

    compileOnly("org.mariadb.jdbc:mariadb-java-client:3.4.1")

    implementation("com.github.scopt:scopt_2.12:4.1.0")
    implementation("org.yaml:snakeyaml:2.2")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.scalatest:scalatest_2.12:$scalaTestVersion")
    testImplementation("org.scalatestplus:junit-4-13_2.12:3.2.19.0")
    testImplementation("org.apache.spark:spark-sql_2.12:$sparkVersion")
    testImplementation("com.h2database:h2:2.2.224")
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

tasks.register<JavaExec>("generateSampleDatasets") {
    group = "verification"
    description = "Generate the sample input-case datasets under samples/input-cases"
    dependsOn(tasks.testClasses)
    val testTask = tasks.named<Test>("test")
    classpath = testTask.get().classpath
    mainClass.set("io.github.jonggeun2001.privyspark.SampleDatasetGenerator")
    workingDir = projectDir
    jvmArgs(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
    )
}

tasks.register<Zip>("packageSampleDatasets") {
    group = "distribution"
    description = "Package the sample input-case datasets as a distributable zip archive"
    archiveFileName.set("privyspark-sample-datasets.zip")
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
    from(layout.projectDirectory.dir("samples/input-cases")) {
        into("input-cases")
    }
}
