package io.github.jonggeun2001.privyspark

import io.github.jonggeun2001.privyspark.util.DriverTcpConnectionSnapshot

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DriverTcpConnectionSnapshotSpec extends AnyFunSuite {
  test("builds TCP snapshot fields from current process socket inodes") {
    val procTcpLines = Seq(
      "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
      "   0: 0100007F:C001 0200000A:268A 01 00000000:00000000 00:00000000 00000000 501 0 11111 1 0000000000000000 100 0 0 10 0",
      "   1: 0100007F:C002 0300000A:1F54 08 00000000:00000000 00:00000000 00000000 501 0 22222 1 0000000000000000 100 0 0 10 0",
      "   2: 0100007F:C003 0400000A:1F54 01 00000000:00000000 00:00000000 00000000 501 0 33333 1 0000000000000000 100 0 0 10 0"
    )

    val snapshot = DriverTcpConnectionSnapshot.fromProcLines(Set("11111", "22222"), procTcpLines)
    val fields = snapshot.fields.toMap

    assert(snapshot.available)
    assert(fields("tcp_snapshot_available") == true)
    assert(fields("tcp_fd_count") == 2)
    assert(fields("tcp_states") == "ESTABLISHED:1+CLOSE_WAIT:1")
    assert(fields("tcp_remote_ports_top") == "8020:1+9866:1")
    assert(fields("tcp_established_remote_ports_top") == "9866:1")
    assert(fields("tcp_established_remote_endpoints_top") == "10.0.0.2:9866:1")
  }

  test("parses socket fd symlink targets") {
    assert(DriverTcpConnectionSnapshot.parseSocketInode("socket:[12345]") == Some("12345"))
    assert(DriverTcpConnectionSnapshot.parseSocketInode("pipe:[12345]").isEmpty)
  }

  test("renders IPv4 proc tcp addresses in dotted decimal order") {
    assert(DriverTcpConnectionSnapshot.parseRemoteHost("0200000A") == "10.0.0.2")
    assert(DriverTcpConnectionSnapshot.parseRemoteHost("0100007F") == "127.0.0.1")
  }
}
