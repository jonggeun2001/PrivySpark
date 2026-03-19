package io.github.jonggeun2001.privyspark.validator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

import java.util.regex.Pattern
import scala.collection.mutable
import scala.io.Source

object KoreanNameValidator {
  private final case class NameDictionary(givenNames: Set[String], shortFullNames: Set[String])
  private final case class CandidateSpan(candidate: String, start: Int, end: Int)

  val ValidatorName = "korean_name_dict"
  val RuleRegexReference = "__KOREAN_NAME_RULE_REGEX__"

  private val CompoundSurnames = Seq("남궁", "선우", "독고", "사공", "제갈", "황보")
  private val SingleSurnames = Seq(
    "김", "이", "박", "최", "정", "강", "조", "윤", "장", "임", "한", "오", "서", "신", "권", "황", "안",
    "송", "류", "전", "홍", "고", "문", "양", "손", "소", "배", "백", "허", "유", "남", "심", "노", "하", "곽",
    "성", "차", "주", "우", "구", "민", "진", "나", "지", "엄", "채", "원", "천", "방", "공", "현", "함",
    "변", "염", "여", "추", "도", "석", "선", "설", "마", "길", "연", "위", "표", "명", "기", "반", "라",
    "왕", "금", "옥", "육", "인", "맹", "제", "모", "탁", "국", "어", "은", "편", "봉", "피", "경", "사", "가"
  )
  private val SupportedSurnames = (CompoundSurnames ++ SingleSurnames).sortBy(prefix => -prefix.length)
  val RuleRegex = s"(${SupportedSurnames.mkString("|")})[가-힣]{1,2}"
  private val AllowedTrailingSuffixes = Seq(
    "님", "씨", "군", "양",
    "아", "야", "요",
    "이", "가", "은", "는", "을", "를", "의", "과", "와", "도", "만", "나", "랑",
    "에", "에서", "에게", "에게서", "한테", "한테서", "께", "께서", "로", "으로", "부터", "까지", "보다", "처럼", "하고", "이랑",
    "이나", "이며", "이고", "이라", "이라고", "인가요", "예요", "이에요", "죠", "이죠", "네요", "이네요",
    "라", "라고", "라는", "라서", "지만", "이지만", "이는", "이가", "이를", "인데", "인데요", "입니다", "이군요"
  ).sortBy(prefix => -prefix.length)
  private val SecondarySuffixes = Seq("은", "는", "도", "만")
  private val PoliteEndingSuffixes = Seq("예요", "이에요", "입니다", "인가요", "죠", "이죠", "네요", "이네요")
  private val HonorificContinuationSuffixes =
    Seq(
      "이", "가", "을", "를", "의", "께", "께서", "과", "와", "랑", "이랑", "하고",
      "에게", "에게서", "한테", "한테서", "로", "으로",
      "라고", "이라고", "라는", "라서", "지만", "이지만", "인데", "인데요", "이며", "이고", "이라"
    ) ++
      SecondarySuffixes ++
      PoliteEndingSuffixes
  private val ContinuationSuffixes: Map[String, Seq[String]] = Map(
    "님" -> HonorificContinuationSuffixes,
    "씨" -> HonorificContinuationSuffixes,
    "군" -> HonorificContinuationSuffixes,
    "양" -> HonorificContinuationSuffixes,
    "과" -> SecondarySuffixes,
    "와" -> SecondarySuffixes,
    "랑" -> SecondarySuffixes,
    "에" -> SecondarySuffixes,
    "에서" -> SecondarySuffixes,
    "에게" -> SecondarySuffixes,
    "에게서" -> SecondarySuffixes,
    "한테" -> SecondarySuffixes,
    "한테서" -> SecondarySuffixes,
    "께" -> SecondarySuffixes,
    "께서" -> SecondarySuffixes,
    "로" -> SecondarySuffixes,
    "으로" -> SecondarySuffixes,
    "부터" -> SecondarySuffixes,
    "까지" -> SecondarySuffixes,
    "보다" -> SecondarySuffixes,
    "처럼" -> SecondarySuffixes,
    "하고" -> (Seq("요") ++ SecondarySuffixes),
    "이랑" -> (Seq("요") ++ SecondarySuffixes),
    "라고" -> (Seq("요") ++ SecondarySuffixes),
    "이라고" -> (Seq("요") ++ SecondarySuffixes),
    "은" -> Seq("요"),
    "는" -> Seq("요"),
    "도" -> Seq("요"),
    "만" -> Seq("요")
  ).map { case (suffix, nextSuffixes) =>
    suffix -> nextSuffixes.sortBy(prefix => -prefix.length)
  }
  private val CandidatePattern = Pattern.compile(RuleRegex)
  private val broadcastCache = mutable.Map.empty[String, Broadcast[NameDictionary]]
  private lazy val localDictionary = NameDictionary(
    givenNames = loadEntries("/korean-name-given-names.txt"),
    shortFullNames = loadEntries("/korean-name-short-full-names.txt")
  )

  def predicate(spark: SparkSession, valueColumn: Column, ruleRegex: String): Column = {
    val rulePattern = Pattern.compile(ruleRegex)
    val dictionary = broadcastDictionary(spark)
    val validate = udf { value: String =>
      containsLikelyName(value, rulePattern, dictionary.value)
    }
    validate(valueColumn)
  }

  private[privyspark] def containsLikelyName(value: String, ruleRegex: String): Boolean = {
    containsLikelyName(value, Pattern.compile(ruleRegex), localDictionary)
  }

  private def containsLikelyName(value: String, rulePattern: Pattern, dictionary: NameDictionary): Boolean = {
    if (value == null) {
      return false
    }

    if (value.trim.isEmpty) {
      return false
    }

    val ruleMatcher = rulePattern.matcher(value)
    while (ruleMatcher.find()) {
      candidateSpans(ruleMatcher).foreach {
        case (candidate, candidateEnd) =>
          if (isLikelyNameCandidate(candidate, value, candidateEnd, dictionary)) {
            return true
          }
      }
    }

    false
  }

  private def candidateSpans(ruleMatcher: java.util.regex.Matcher): Seq[(String, Int)] = {
    val candidates = allCandidateSpans(ruleMatcher.group(), ruleMatcher.start())
    val explicitGroupCandidates = candidateSpansFromGroups(ruleMatcher, candidates)
    val selectedCandidates =
      if (explicitGroupCandidates.nonEmpty) {
        explicitGroupCandidates
      } else {
        selectCandidateSpans(candidates)
      }

    selectedCandidates.map(candidate => candidate.candidate -> candidate.end)
  }

  private def allCandidateSpans(spanText: String, spanStart: Int): Seq[CandidateSpan] = {
    val candidateMatcher = CandidatePattern.matcher(spanText)
    val candidates = mutable.ArrayBuffer.empty[CandidateSpan]
    while (candidateMatcher.find()) {
      candidates += CandidateSpan(
        candidate = candidateMatcher.group(),
        start = spanStart + candidateMatcher.start(),
        end = spanStart + candidateMatcher.end()
      )
    }
    candidates.toSeq
  }

  private def candidateSpansFromGroups(
    ruleMatcher: java.util.regex.Matcher,
    matchCandidates: Seq[CandidateSpan]
  ): Seq[CandidateSpan] = {
    if (ruleMatcher.groupCount() == 0) {
      Seq.empty
    } else {
      val matchCandidateSet = matchCandidates.toSet
      val candidateBearingGroups = (1 to ruleMatcher.groupCount()).map { groupIndex =>
        Option(ruleMatcher.group(groupIndex)).toSeq.flatMap { groupText =>
          allCandidateSpans(groupText, ruleMatcher.start(groupIndex)).filter(matchCandidateSet.contains)
        }
      }.filter(_.nonEmpty)

      if (candidateBearingGroups.length == 1) {
        candidateBearingGroups.head.distinct
      } else {
        Seq.empty
      }
    }
  }

  private def selectCandidateSpans(candidates: Seq[CandidateSpan]): Seq[CandidateSpan] = {
    if (candidates.length == 1) {
      candidates
    } else {
      Seq.empty
    }
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
    consumeAllowedTrailingSuffixes(trailing, AllowedTrailingSuffixes)
  }

  private def consumeAllowedTrailingSuffixes(trailing: String, allowedSuffixes: Seq[String]): Boolean = {
    if (trailing.isEmpty) {
      true
    } else {
      val next = trailing.charAt(0)
      if (!isHangul(next)) {
        true
      } else {
        allowedSuffixes.exists { suffix =>
          if (trailing.startsWith(suffix)) {
            val remaining = trailing.substring(suffix.length)
            hasTerminalBoundary(remaining) || allowedNextSuffixes(suffix).exists(nextSuffixes =>
              consumeAllowedTrailingSuffixes(remaining, nextSuffixes)
            )
          } else {
            false
          }
        }
      }
    }
  }

  private def allowedNextSuffixes(suffix: String): Option[Seq[String]] = {
    ContinuationSuffixes.get(suffix)
  }

  private def hasTerminalBoundary(trailing: String): Boolean = {
    trailing.isEmpty || !isHangul(trailing.charAt(0))
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
