package org.dqeproject

import com.amazon.deequ.analyzers.{ApproxCountDistinct, Completeness, Compliance, Correlation, CountDistinct, Distinctness, Maximum, Mean, Minimum, MutualInformation, PatternMatch, Sum, Uniqueness}
import scala.util.matching.Regex
/***
 *
 */
class SetupAnalysisMetricsForColumns {


  /***
   *
   * @note : List of columns on which Completeness need to Apply
   * @param columnList
   * @return
   */
  def setupCompleteness(columnList: Seq[String]): Seq[Completeness] = {
    var completenessAnalyzers = Seq.empty[Completeness]
    for (col <- columnList) {
      completenessAnalyzers = completenessAnalyzers :+ Completeness(col)
    }
    return completenessAnalyzers
  }

  /***
   * @note: List of columns on which CountDistinct need to Apply
   * @param columnList
   * @return
   */
  def setupCountDistinct(columnList: Seq[String]):Seq[CountDistinct] = {
    var countDistinctAnalyzers = Seq.empty[CountDistinct]
    for (col <- columnList) {
      countDistinctAnalyzers = countDistinctAnalyzers :+ CountDistinct(col)
    }
    return countDistinctAnalyzers
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupMaximum(columnList: Seq[String]): Seq[Maximum] = {
    var maximumAnalyzers = Seq.empty[Maximum]
    for (col <- columnList) {
      maximumAnalyzers = maximumAnalyzers :+ Maximum(col)
    }
    return maximumAnalyzers
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupMean(columnList: Seq[String]): Seq[Mean] = {
    var setupMean = Seq.empty[Mean]
    for (col <- columnList) {
      setupMean = setupMean :+ Mean(col)
    }
    return setupMean
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupMinimum(columnList: Seq[String]): Seq[Minimum] = {
    var minimumAnalyzers = Seq.empty[Minimum]
    for (col <- columnList) {
      minimumAnalyzers = minimumAnalyzers :+ Minimum(col)
    }
    return minimumAnalyzers
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupSum(columnList: Seq[String]): Seq[Sum] = {
    var sumAnalyzers = Seq.empty[Sum]
    for (col <- columnList) {
      sumAnalyzers = sumAnalyzers :+ Sum(col)
    }
    return sumAnalyzers
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupUniqueness(columnList: Seq[String]): Seq[Uniqueness] = {
    var uniquenessAnalyzers = Seq.empty[Uniqueness]
    for (col <- columnList) {
      uniquenessAnalyzers = uniquenessAnalyzers :+ Uniqueness(col)
    }
    return uniquenessAnalyzers
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupDistinctness(columnList: Seq[String]): Seq[Distinctness] = {
    var distinctnessAnalyzers = Seq.empty[Distinctness]
    for (col <- columnList) {
      distinctnessAnalyzers = distinctnessAnalyzers :+ Distinctness(col)
    }
    return distinctnessAnalyzers
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupApproxCountDistinctAnalyzer(columnList: Seq[String]): Seq[ApproxCountDistinct] = {
    var setupApproxCountDistinctAnalyzer = Seq.empty[ApproxCountDistinct]
    for (col <- columnList) {
      setupApproxCountDistinctAnalyzer = setupApproxCountDistinctAnalyzer :+ ApproxCountDistinct(col)
    }
    return setupApproxCountDistinctAnalyzer
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupComplianceAnalyzer(columnList: Seq[String]): Seq[Compliance] = {
    var setupComplianceAnalyzer = Seq.empty[Compliance]
    for (col <- columnList) {
      val complience_name = col.split("~")(0)
      println(complience_name)
      val complience_condition = col.split("~")(1)
      println(complience_condition)
      setupComplianceAnalyzer = setupComplianceAnalyzer :+ Compliance(complience_name,complience_condition)
    }
    return setupComplianceAnalyzer
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupCorrelationAnalyzer(columnList: Seq[String]): Seq[Correlation] = {
    var setupCorrelationAnalyzer = Seq.empty[Correlation]
    for (col <- columnList) {
      val correlation_col_one = col.split("~")(0)
      println(correlation_col_one)
      val correlation_col_two = col.split("~")(1)
      println(correlation_col_two)
      setupCorrelationAnalyzer = setupCorrelationAnalyzer :+ Correlation(correlation_col_one, correlation_col_two)
    }
    return setupCorrelationAnalyzer
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupMutualInformationAnalyzer(columnList: Seq[String]): Seq[MutualInformation] = {
    var setupMutualInformationAnalyzer = Seq.empty[MutualInformation]
    for (col <- columnList) {
      val mutualInformation_col_one = col.split("~")(0)
      println(mutualInformation_col_one)
      val mutualInformation_col_two = col.split("~")(1)
      println(mutualInformation_col_two)
      setupMutualInformationAnalyzer = setupMutualInformationAnalyzer :+ MutualInformation(Seq(mutualInformation_col_one,mutualInformation_col_two))
    }
    return setupMutualInformationAnalyzer
  }

  /***
   *
   * @param columnList
   * @return
   */
  def setupPatternMatchAnalyzer(columnList: Seq[String]): Seq[PatternMatch] = {
    var setupPatternMatchAnalyzer = Seq.empty[PatternMatch]
    for (col <- columnList) {
      val patternMatch_col = col.split("~")(0)
      val pattern_to_match = col.split("~")(1)
      val pattern_find = new Regex(pattern_to_match)
      println(pattern_find)
      setupPatternMatchAnalyzer = setupPatternMatchAnalyzer :+ PatternMatch(patternMatch_col, pattern = pattern_find)
    }
    return setupPatternMatchAnalyzer
  }

}
