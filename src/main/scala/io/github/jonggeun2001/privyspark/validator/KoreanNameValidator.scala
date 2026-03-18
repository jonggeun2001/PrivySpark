package io.github.jonggeun2001.privyspark.validator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

import java.util.regex.Pattern
import scala.collection.mutable
import scala.io.Source

object KoreanNameValidator {
  val ValidatorName = "korean_name_dict"
  val RuleRegex =
    "(남궁|선우|독고|사공|제갈|황보|김|이|박|최|정|강|조|윤|장|임|한|오|서|신|권|황|안|송|류|전|홍|고|문|양|손|배|백|허|유|남|심|노|하|곽|성|차|주|우|구|민|진|나|지|엄|채|원|천|방|공|현|함|변|염|여|추|도|석|선|설|마|길|연|위|표|명|기|반|라|왕|금|옥|육|인|맹|제|모|탁|국|어|은|편|봉|피|경|사|가)[가-힣]{1,2}"

  private val CompoundSurnames = Seq("남궁", "선우", "독고", "사공", "제갈", "황보")
  private val SingleSurnames = Seq(
    "김", "이", "박", "최", "정", "강", "조", "윤", "장", "임", "한", "오", "서", "신", "권", "황", "안",
    "송", "류", "전", "홍", "고", "문", "양", "손", "배", "백", "허", "유", "남", "심", "노", "하", "곽",
    "성", "차", "주", "우", "구", "민", "진", "나", "지", "엄", "채", "원", "천", "방", "공", "현", "함",
    "변", "염", "여", "추", "도", "석", "선", "설", "마", "길", "연", "위", "표", "명", "기", "반", "라",
    "왕", "금", "옥", "육", "인", "맹", "제", "모", "탁", "국", "어", "은", "편", "봉", "피", "경", "사", "가"
  )
  private val NamePattern = Pattern.compile(RuleRegex)
  private val broadcastCache = mutable.Map.empty[String, Broadcast[Set[String]]]
  private lazy val localSyllables = loadSyllables()

  def predicate(spark: SparkSession, valueColumn: Column): Column = {
    val syllables = broadcastSyllables(spark)
    val validate = udf { value: String =>
      containsLikelyName(value, syllables.value)
    }
    validate(valueColumn)
  }

  private[privyspark] def containsLikelyName(value: String): Boolean = {
    containsLikelyName(value, localSyllables)
  }

  private def containsLikelyName(value: String, syllables: Set[String]): Boolean = {
    if (value == null) {
      return false
    }

    val normalized = value.trim
    if (normalized.isEmpty) {
      return false
    }

    val matcher = NamePattern.matcher(normalized)
    while (matcher.find()) {
      if (isLikelyNameCandidate(matcher.group(), syllables)) {
        return true
      }
    }

    false
  }

  private def isLikelyNameCandidate(candidate: String, syllables: Set[String]): Boolean = {
    extractGivenName(candidate).exists { givenName =>
      givenName.nonEmpty && givenName.length <= 2 && givenName.forall(ch => syllables.contains(ch.toString))
    }
  }

  private def extractGivenName(candidate: String): Option[String] = {
    CompoundSurnames.find(candidate.startsWith)
      .orElse(SingleSurnames.find(candidate.startsWith))
      .map(surname => candidate.substring(surname.length))
  }

  private def broadcastSyllables(spark: SparkSession): Broadcast[Set[String]] = broadcastCache.synchronized {
    val appId = spark.sparkContext.applicationId
    broadcastCache.getOrElseUpdate(appId, spark.sparkContext.broadcast(localSyllables))
  }

  private def loadSyllables(): Set[String] = {
    val stream = Option(getClass.getResourceAsStream("/korean-name-syllables.txt"))
      .getOrElse(throw new IllegalStateException("Missing resource: /korean-name-syllables.txt"))
    val source = Source.fromInputStream(stream, "UTF-8")
    try {
      source.getLines().map(_.trim).filter(line => line.nonEmpty && !line.startsWith("#")).toSet
    } finally {
      source.close()
    }
  }
}
