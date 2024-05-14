package org.dqeproject

import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Compliance, Correlation, CountDistinct, Distinctness, Maximum, Mean, Minimum, MutualInformation, PatternMatch, Size, Sum, Uniqueness}

class AnalysisBuilder {

  /***
   *
   * @param buildAanalysisParams
   * @return
   */
  def buildAnalysis(buildAanalysisParams:scala.collection.Map[String,Seq[String]])={

    val setupAnalysisMetricsForColumns = new SetupAnalysisMetricsForColumns()
    val analyzers = buildAanalysisParams.keys
    var completenessAnalyzers = Seq.empty[Completeness]
    var countDistinctAnalyzers = Seq.empty[CountDistinct]
    var maximumAnalyzers = Seq.empty[Maximum]
    var minimumAnalyzers = Seq.empty[Minimum]
    var meanAnalyzers = Seq.empty[Mean]
    var uniquenessAnalyzers = Seq.empty[Uniqueness]
    var distinctnessAnalyzers = Seq.empty[Distinctness]
    var sumAnalyzers = Seq.empty[Sum]
    var approxCountDistinctAnalyzer = Seq.empty[ApproxCountDistinct]
    var complianceAnalyzer = Seq.empty[Compliance]
    var correlationAnalyzer = Seq.empty[Correlation]
    var mutualInformationAnalyzer = Seq.empty[MutualInformation]
    var patternMatchAnalyzer = Seq.empty[PatternMatch]
    for(analyzer <- analyzers){
      if(analyzer.toUpperCase() == "COMPLETENESS"){
        completenessAnalyzers = setupAnalysisMetricsForColumns.setupCompleteness(buildAanalysisParams.get(analyzer).get)
      }
      else if(analyzer.toUpperCase() == "COUNTDISTINCT"){
        countDistinctAnalyzers = setupAnalysisMetricsForColumns.setupCountDistinct(buildAanalysisParams.get(analyzer).get)
      }
      else if(analyzer.toUpperCase() == "MAXIMUM") {
        maximumAnalyzers = setupAnalysisMetricsForColumns.setupMaximum(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase() == "MINIMUM") {
        minimumAnalyzers = setupAnalysisMetricsForColumns.setupMinimum(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase() == "MEAN") {
        meanAnalyzers = setupAnalysisMetricsForColumns.setupMean(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase() == "UNIQUENESS") {
        uniquenessAnalyzers = setupAnalysisMetricsForColumns.setupUniqueness(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase() == "DISTINCTNESS") {
        distinctnessAnalyzers = setupAnalysisMetricsForColumns.setupDistinctness(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase() == "SUM") {
        sumAnalyzers = setupAnalysisMetricsForColumns.setupSum(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase() == "APPROXCOUNTDISTINCT") {
        approxCountDistinctAnalyzer = setupAnalysisMetricsForColumns.setupApproxCountDistinctAnalyzer(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase() == "COMPLIANCE") {
        complianceAnalyzer = setupAnalysisMetricsForColumns.setupComplianceAnalyzer(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase() == "CORRELATION") {
        correlationAnalyzer = setupAnalysisMetricsForColumns.setupCorrelationAnalyzer(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase == "MUTUALINFORMATION") {
        mutualInformationAnalyzer = setupAnalysisMetricsForColumns.setupMutualInformationAnalyzer(buildAanalysisParams.get(analyzer).get)
      }
      else if (analyzer.toUpperCase == "PATTERNMATCH") {
        patternMatchAnalyzer = setupAnalysisMetricsForColumns.setupPatternMatchAnalyzer(buildAanalysisParams.get(analyzer).get)
      }
    }
    Analysis().addAnalyzers(completenessAnalyzers)
      .addAnalyzers(countDistinctAnalyzers)
      .addAnalyzers(maximumAnalyzers)
      .addAnalyzers(minimumAnalyzers)
      .addAnalyzers(meanAnalyzers)
      .addAnalyzers(uniquenessAnalyzers)
      .addAnalyzers(distinctnessAnalyzers)
      .addAnalyzers(sumAnalyzers)
      .addAnalyzers(approxCountDistinctAnalyzer)
      .addAnalyzers(complianceAnalyzer)
      .addAnalyzers(correlationAnalyzer)
      .addAnalyzers(mutualInformationAnalyzer)
      .addAnalyzers(patternMatchAnalyzer)
      .addAnalyzer(Size())
  }

}
