package io.github.jonggeun2001.privyspark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

@RunWith(classOf[JUnitRunner])
class ReleaseArtifactWorkflowSpec extends AnyFunSuite {
  private val workflowPath = Paths.get(".github", "workflows", "release-artifact.yml")

  test("release workflow uploads default-rules.yaml as a release asset") {
    val workflow = readText(workflowPath)

    assert(workflow.contains("config/rules/default.yaml"), "release workflow should stage the default ruleset file")
    assert(workflow.contains("default-rules.yaml"), "release workflow should publish a default-rules.yaml asset")
    assert(workflow.contains("asset_rules"), "release workflow should expose the rules asset as a workflow output")
  }

  test("release workflow publishes offline review response HTML example") {
    val workflow = readText(workflowPath)

    assert(workflow.contains("review_response_example=\"samples/offline-review/review-response-example.html\""))
    assert(workflow.contains("asset_review_response_example=\"build/distributions/privyspark-${{ steps.meta.outputs.tag }}-review-response-example.html\""))
    assert(workflow.contains("${{ steps.assets.outputs.asset_review_response_example }}"))
  }

  test("offline review response HTML example is self-contained and downloads response JSON") {
    val html = readText("samples/offline-review/review-response-example.html")

    assert(html.contains("<!doctype html>"))
    assert(html.contains("const REVIEW_DATA ="))
    assert(html.contains("\"schema_version\": 1"))
    assert(html.contains("\"scan_results_fingerprint\""))
    assert(html.contains("\"finding_key\""))
    assert(html.contains("\"finding_hash\""))
    assert(html.contains("privyspark-response.json"))
    assert(!html.contains("http://"))
    assert(!html.contains("https://"))
  }

  test("shadow jar does not bundle the MariaDB JDBC driver") {
    val buildScript = readText("build.gradle.kts")

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
    val submitScript = readText(Paths.get("bin", "privyspark-submit"))

    assert(submitScript.contains("PRIVYSPARK_JARS"), "submit script should expose an env hook for external driver jars")
    assert(submitScript.contains("--jars"), "submit script should pass external driver jars to spark-submit")
  }

  private def readText(path: String): String =
    readText(Paths.get(path))

  private def readText(path: java.nio.file.Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
}
