package io.github.jonggeun2001.privyspark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class ReleaseArtifactWorkflowSpec extends AnyFunSuite {
  private val workflowPath = Paths.get(".github", "workflows", "release-artifact.yml")

  test("release workflow uploads default-rules.yaml as a release asset") {
    val workflow = new String(Files.readAllBytes(workflowPath), StandardCharsets.UTF_8)

    assert(workflow.contains("config/rules/default.yaml"), "release workflow should stage the default ruleset file")
    assert(workflow.contains("default-rules.yaml"), "release workflow should publish a default-rules.yaml asset")
    assert(workflow.contains("asset_rules"), "release workflow should expose the rules asset as a workflow output")
  }

  test("shadow jar does not bundle the MariaDB JDBC driver") {
    val buildScript = new String(Files.readAllBytes(Paths.get("build.gradle.kts")), StandardCharsets.UTF_8)

    assert(
      buildScript.contains("""compileOnly("org.mariadb.jdbc:mariadb-java-client:"""),
      "MariaDB JDBC driver must stay out of runtimeClasspath so Shadow JAR does not package it"
    )
    assert(
      !buildScript.contains("""implementation("org.mariadb.jdbc:mariadb-java-client:"""),
      "MariaDB JDBC driver must not be an implementation dependency"
    )
  }

  test("submit script can pass external JDBC jars to spark-submit") {
    val submitScript = new String(Files.readAllBytes(Paths.get("bin", "privyspark-submit")), StandardCharsets.UTF_8)

    assert(submitScript.contains("PRIVYSPARK_JARS"), "submit script should expose an env hook for external driver jars")
    assert(submitScript.contains("--jars"), "submit script should pass external driver jars to spark-submit")
  }
}
