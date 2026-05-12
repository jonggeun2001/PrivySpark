package io.github.jonggeun2001.privyspark.util

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[privyspark] final case class DriverTcpConnectionSnapshot(
  available: Boolean,
  tcpFdCount: Int,
  stateCounts: Map[String, Int],
  remotePortCounts: Map[Int, Int],
  establishedRemotePortCounts: Map[Int, Int],
  establishedRemoteEndpointCounts: Map[String, Int],
  unavailableReason: Option[String] = None
) {
  def fields: Seq[(String, Any)] = {
    if (available) {
      Seq(
        "tcp_snapshot_available" -> true,
        "tcp_fd_count" -> tcpFdCount,
        "tcp_states" -> DriverTcpConnectionSnapshot.renderStateCounts(stateCounts),
        "tcp_remote_ports_top" -> DriverTcpConnectionSnapshot.renderRemotePortCounts(remotePortCounts),
        "tcp_established_remote_ports_top" -> DriverTcpConnectionSnapshot.renderRemotePortCounts(establishedRemotePortCounts),
        "tcp_established_remote_endpoints_top" -> DriverTcpConnectionSnapshot.renderRemoteEndpointCounts(establishedRemoteEndpointCounts)
      )
    } else {
      Seq(
        "tcp_snapshot_available" -> false,
        "tcp_fd_count" -> 0,
        "tcp_snapshot_reason" -> unavailableReason.getOrElse("unavailable")
      )
    }
  }
}

private[privyspark] object DriverTcpConnectionSnapshot {
  private val ProcSelfFd = Paths.get("/proc/self/fd")
  private val ProcNetTcpFiles = Seq(Paths.get("/proc/net/tcp"), Paths.get("/proc/net/tcp6"))
  private val SocketTargetPattern = """socket:\[(\d+)\]""".r
  private val StateNames = Map(
    "01" -> "ESTABLISHED",
    "02" -> "SYN_SENT",
    "03" -> "SYN_RECV",
    "04" -> "FIN_WAIT1",
    "05" -> "FIN_WAIT2",
    "06" -> "TIME_WAIT",
    "07" -> "CLOSE",
    "08" -> "CLOSE_WAIT",
    "09" -> "LAST_ACK",
    "0A" -> "LISTEN",
    "0B" -> "CLOSING"
  )
  private val StateOrder = Seq(
    "ESTABLISHED",
    "CLOSE_WAIT",
    "SYN_SENT",
    "SYN_RECV",
    "FIN_WAIT1",
    "FIN_WAIT2",
    "TIME_WAIT",
    "LAST_ACK",
    "CLOSING",
    "CLOSE",
    "LISTEN"
  ).zipWithIndex.toMap

  def capture(): DriverTcpConnectionSnapshot =
    capture(ProcSelfFd, ProcNetTcpFiles)

  private[privyspark] def capture(
    fdDirectory: Path,
    tcpFiles: Seq[Path]
  ): DriverTcpConnectionSnapshot = {
    if (!Files.isDirectory(fdDirectory)) {
      return unavailable("proc_fd_unavailable")
    }

    try {
      val socketInodes = currentProcessSocketInodes(fdDirectory)
      val tcpLines = tcpFiles
        .filter(path => Files.exists(path))
        .flatMap(path => Files.readAllLines(path, StandardCharsets.UTF_8).asScala)

      if (tcpLines.isEmpty) {
        unavailable("proc_tcp_unavailable")
      } else {
        fromProcLines(socketInodes, tcpLines)
      }
    } catch {
      case NonFatal(e) =>
        unavailable(Option(e.getMessage).filter(_.nonEmpty).getOrElse(e.getClass.getSimpleName))
    }
  }

  private[privyspark] def parseSocketInode(fdTarget: String): Option[String] =
    fdTarget match {
      case SocketTargetPattern(inode) => Some(inode)
      case _ => None
    }

  private[privyspark] def fromProcLines(
    socketInodes: Set[String],
    procTcpLines: Seq[String]
  ): DriverTcpConnectionSnapshot = {
    val entries = procTcpLines.flatMap(parseTcpEntry(_, socketInodes))
    val establishedEntries = entries.filter(_.state == "ESTABLISHED")
    DriverTcpConnectionSnapshot(
      available = true,
      tcpFdCount = entries.size,
      stateCounts = entries.groupBy(_.state).map { case (state, values) => state -> values.size },
      remotePortCounts = entries.groupBy(_.remotePort).map { case (port, values) => port -> values.size },
      establishedRemotePortCounts = establishedEntries.groupBy(_.remotePort).map { case (port, values) => port -> values.size },
      establishedRemoteEndpointCounts = establishedEntries.groupBy(_.remoteEndpoint).map { case (endpoint, values) => endpoint -> values.size }
    )
  }

  private def currentProcessSocketInodes(fdDirectory: Path): Set[String] = {
    val stream = Files.newDirectoryStream(fdDirectory)
    try {
      stream.iterator().asScala.flatMap { fdPath =>
        try {
          parseSocketInode(Files.readSymbolicLink(fdPath).toString)
        } catch {
          case NonFatal(_) => None
        }
      }.toSet
    } finally {
      stream.close()
    }
  }

  private final case class TcpEntry(state: String, remotePort: Int, remoteEndpoint: String)

  private def parseTcpEntry(line: String, socketInodes: Set[String]): Option[TcpEntry] = {
    val parts = line.trim.split("\\s+")
    if (parts.length <= 9 || !parts(0).endsWith(":")) {
      return None
    }

    val inode = parts(9)
    if (!socketInodes.contains(inode)) {
      return None
    }

    for {
      state <- StateNames.get(parts(3).toUpperCase)
      remote <- parseRemoteEndpoint(parts(2))
    } yield TcpEntry(state, remote.port, remote.endpoint)
  }

  private final case class RemoteEndpoint(host: String, port: Int) {
    def endpoint: String = s"$host:$port"
  }

  private def parseRemoteEndpoint(remoteAddress: String): Option[RemoteEndpoint] = {
    val index = remoteAddress.lastIndexOf(':')
    if (index < 0 || index == remoteAddress.length - 1) {
      None
    } else {
      try {
        val host = parseRemoteHost(remoteAddress.substring(0, index))
        val port = Integer.parseInt(remoteAddress.substring(index + 1), 16)
        Some(RemoteEndpoint(host, port))
      } catch {
        case NonFatal(_) => None
      }
    }
  }

  private[privyspark] def parseRemoteHost(rawAddress: String): String = {
    val normalized = rawAddress.toUpperCase
    if (normalized.length == 8) {
      normalized
        .grouped(2)
        .toSeq
        .reverse
        .map(Integer.parseInt(_, 16).toString)
        .mkString(".")
    } else {
      normalized
    }
  }

  private def unavailable(reason: String): DriverTcpConnectionSnapshot =
    DriverTcpConnectionSnapshot(
      available = false,
      tcpFdCount = 0,
      stateCounts = Map.empty,
      remotePortCounts = Map.empty,
      establishedRemotePortCounts = Map.empty,
      establishedRemoteEndpointCounts = Map.empty,
      unavailableReason = Some(reason)
    )

  private[privyspark] def renderStateCounts(stateCounts: Map[String, Int]): String =
    renderCounts(
      stateCounts.toSeq.sortBy { case (state, _) => StateOrder.getOrElse(state, Int.MaxValue) }
    )

  private[privyspark] def renderRemotePortCounts(remotePortCounts: Map[Int, Int]): String =
    renderCounts(
      remotePortCounts.toSeq
        .sortBy { case (port, count) => (-count, port) }
        .take(5)
        .map { case (port, count) => port.toString -> count }
    )

  private[privyspark] def renderRemoteEndpointCounts(remoteEndpointCounts: Map[String, Int]): String =
    renderCounts(
      remoteEndpointCounts.toSeq
        .sortBy { case (endpoint, count) => (-count, endpoint) }
        .take(5)
    )

  private def renderCounts(counts: Seq[(String, Int)]): String =
    if (counts.isEmpty) {
      "none"
    } else {
      counts.map { case (name, count) => s"$name:$count" }.mkString("+")
    }
}

private[privyspark] object DriverTcpConnectionLogger {
  def debugSnapshot(event: String, fields: (String, Any)*): Unit = {
    if (DriverLogger.currentLogLevel.priority >= DriverLogLevel.Debug.priority) {
      val baseFields = Seq("thread_name" -> Thread.currentThread().getName)
      DriverLogger.debug(event, (fields ++ baseFields ++ DriverTcpConnectionSnapshot.capture().fields): _*)
    }
  }
}
