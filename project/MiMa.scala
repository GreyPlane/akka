/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import scala.collection.immutable
import sbt._
import sbt.Keys._
import com.typesafe.tools.mima.plugin.MimaPlugin
import com.typesafe.tools.mima.plugin.MimaPlugin.autoImport._

object MiMa extends AutoPlugin {

  //  akka-pki artifact was added in Akka 2.6.6
  private val firstPatchOf26 = 6
  private val latestPatchOf26 = 20
  private val latestPatchOf27 = 0

  override def requires = MimaPlugin
  override def trigger = allRequirements

  val checkMimaFilterDirectories =
    taskKey[Unit]("Check that the mima directories are correct compared to latest version")

  override val projectSettings = Seq(
    mimaReportSignatureProblems := true,
    mimaPreviousArtifacts := akkaPreviousArtifacts(name.value, organization.value, scalaBinaryVersion.value),
    checkMimaFilterDirectories := checkFilterDirectories(baseDirectory.value))

  def checkFilterDirectories(moduleRoot: File): Unit = {
    val nextVersionFilterDir = moduleRoot / "src" / "main" / "mima-filters" / s"2.7.${latestPatchOf27 + 1}.backwards.excludes"
    if (nextVersionFilterDir.exists()) {
      throw new IllegalArgumentException(s"Incorrect mima filter directory exists: '$nextVersionFilterDir' " +
      s"should be with number from current release '${moduleRoot / "src" / "main" / "mima-filters" / s"2.7.$latestPatchOf27.backwards.excludes"}")
    }
  }

  def akkaPreviousArtifacts(
      projectName: String,
      organization: String,
      scalaBinaryVersion: String): Set[sbt.ModuleID] = {
    if (scalaBinaryVersion.startsWith("3")) {
      // No binary compatibility for 3.0 artifacts for now - experimental
      Set.empty
    } else {
      val versions: Seq[String] = {
        val akka26Previous = expandVersions(2, 6, firstPatchOf26 to latestPatchOf26)
        // FIXME during milestone phase we check latest milestone but when final it should be replaced with
        // val akka27Previous = expandVersions(2, 7, 0 to latestPatchOf27)
        // FIXME why can't the snapshot be used here? List("2.7.0-M1")
        val akka27Previous = Nil

        akka26Previous ++ akka27Previous
      }

      // check against all binary compatible artifacts
      versions.map { v =>
        organization %% projectName % v
      }.toSet
    }
  }

  private def expandVersions(major: Int, minor: Int, patches: immutable.Seq[Int]): immutable.Seq[String] =
    patches.map(patch => s"$major.$minor.$patch")
}
