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
}
