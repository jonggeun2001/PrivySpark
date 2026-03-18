package io.github.jonggeun2001.privyspark.validator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

import java.util.regex.Pattern
import scala.collection.mutable
import scala.io.Source

object KoreanNameValidator {
  private final case class NameDictionary(givenNames: Set[String], shortFullNames: Set[String])

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
  private val AllowedTrailingPrefixes = Seq(
    "님", "씨", "군", "양",
    "아", "야",
    "이", "가", "은", "는", "을", "를", "의", "과", "와", "도", "만",
    "에", "에서", "에게", "한테", "께", "께서", "로", "으로", "부터", "까지", "보다", "처럼",
    "이며", "이고", "이라", "이라고", "이는", "이가", "이를", "입니다"
  )
  private val NamePattern = Pattern.compile(RuleRegex)
  private val broadcastCache = mutable.Map.empty[String, Broadcast[NameDictionary]]
  private lazy val localDictionary = NameDictionary(
    givenNames = loadEntries("/korean-name-given-names.txt"),
    shortFullNames = loadEntries("/korean-name-short-full-names.txt")
  )

  def predicate(spark: SparkSession, valueColumn: Column): Column = {
    val dictionary = broadcastDictionary(spark)
    val validate = udf { value: String =>
      containsLikelyName(value, dictionary.value)
    }
    validate(valueColumn)
  }

  private[privyspark] def containsLikelyName(value: String): Boolean = {
    containsLikelyName(value, localDictionary)
  }

  private def containsLikelyName(value: String, dictionary: NameDictionary): Boolean = {
    if (value == null) {
      return false
    }

    val normalized = value.trim
    if (normalized.isEmpty) {
      return false
    }

    val matcher = NamePattern.matcher(normalized)
    while (matcher.find()) {
      if (isLikelyNameCandidate(matcher.group(), normalized, matcher.end(), dictionary)) {
        return true
      }
    }

    false
  }

  private def isLikelyNameCandidate(
    candidate: String,
    source: String,
    matchEnd: Int,
    dictionary: NameDictionary
  ): Boolean = {
    val directMatch = candidateMatches(candidate, dictionary) && hasAllowedTrailingBoundary(source, matchEnd)
    directMatch || shortenedShortNameCandidate(candidate).exists { shortened =>
      candidateMatches(shortened, dictionary) && hasAllowedTrailingBoundary(source, matchEnd - 1)
    }
  }

  private def candidateMatches(candidate: String, dictionary: NameDictionary): Boolean = {
    extractGivenName(candidate).exists { givenName =>
      givenName.length match {
        case 1 => dictionary.shortFullNames.contains(candidate)
        case 2 => dictionary.givenNames.contains(givenName)
        case _ => false
      }
    }
  }

  private def shortenedShortNameCandidate(candidate: String): Option[String] = {
    extractGivenName(candidate).flatMap { givenName =>
      if (givenName.length == 2) {
        val shortened = candidate.dropRight(1)
        extractGivenName(shortened).filter(_.length == 1).map(_ => shortened)
      } else {
        None
      }
    }
  }

  private def hasAllowedTrailingBoundary(source: String, matchEnd: Int): Boolean = {
    if (matchEnd >= source.length) {
      true
    } else {
      consumeAllowedTrailingSuffixes(source.substring(matchEnd))
    }
  }

  private def consumeAllowedTrailingSuffixes(trailing: String): Boolean = {
    if (trailing.isEmpty) {
      true
    } else {
      val next = trailing.charAt(0)
      if (!isHangul(next)) {
        true
      } else {
        AllowedTrailingPrefixes.exists { prefix =>
          trailing.startsWith(prefix) && consumeAllowedTrailingSuffixes(trailing.substring(prefix.length))
        }
      }
    }
  }

  private def isHangul(ch: Char): Boolean = {
    ch >= '\uAC00' && ch <= '\uD7A3'
  }

  private def extractGivenName(candidate: String): Option[String] = {
    CompoundSurnames.find(candidate.startsWith)
      .orElse(SingleSurnames.find(candidate.startsWith))
      .map(surname => candidate.substring(surname.length))
  }

  private def broadcastDictionary(spark: SparkSession): Broadcast[NameDictionary] = broadcastCache.synchronized {
    val appId = spark.sparkContext.applicationId
    broadcastCache.getOrElseUpdate(appId, spark.sparkContext.broadcast(localDictionary))
  }

  private def loadEntries(resourcePath: String): Set[String] = {
    val stream = Option(getClass.getResourceAsStream(resourcePath))
      .getOrElse(throw new IllegalStateException(s"Missing resource: $resourcePath"))
    val source = Source.fromInputStream(stream, "UTF-8")
    try {
      source.getLines().map(_.trim).filter(line => line.nonEmpty && !line.startsWith("#")).toSet
    } finally {
      source.close()
    }
  }
}
