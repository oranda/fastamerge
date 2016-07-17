/*
 * Copyright (C) 2016 James McCabe <james@oranda.com>
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
 * Assemble overlapping DNA sequence fragments in FASTA format as read from a file.
 */
object MergeFasta {

  /**
   * A set of sequence fragments, e.g.
   *
   * (ATTAGACCTG, CCTGCCGGAA, AGACCTGCCG, GCCGGAATAC)
   */
  type SequenceSet = RDD[String]

  /**
   *  A matrix showing the overlapping lengths between each sequence and all the others, e.g.
   *
   *  ATTAGACCTG: (CCTGCCGGAA, 4) (AGACCTGCCG, 7) (GCCGGAATAC, 1)
   *  CCTGCCGGAA: (ATTAGACCTG, 1) (AGACCTGCCG, 1) (GCCGGAATAC, 7)
   *  AGACCTGCCG: (ATTAGACCTG, 0) (CCTGCCGGAA, 7) (GCCGGAATAC, 4)
   *  GCCGGAATAC: (ATTAGACCTG, 0) (CCTGCCGGAA, 1) (AGACCTGCCG, 0)
   */
  type OverlapMatrix = RDD[(String, Seq[(String, Int)])]

  /**
   *  A matrix showing only the best overlap matches between sequences, and including only
   *  pairs that overlap by more than half their length, e.g.
   *
   *  ATTAGACCTG: (AGACCTGCCG, 7)
   *  CCTGCCGGAA: (GCCGGAATAC, 7)
   *  AGACCTGCCG: (CCTGCCGGAA, 7)
   */
  type OverlapMatrixReduced = RDD[(String, (String, Int))]

  /**
   * Entry point. Runs the code and prints the result to stdout.
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MergeFasta")
    val sc = new SparkContext(conf)

    val initialSeqSet = sc.parallelize(parseFastaData("fasta_data.txt"))
    val combinedSeq = mergeSequences(initialSeqSet)
    showResult(initialSeqSet, combinedSeq)
  }

  /**
   * Assemble sequence fragments into a single sequence.
   */
  def mergeSequences(seqSet: SequenceSet): String = {
    val overlapMatrix = findOverlappingPairs(seqSet)
    val startingSeq = findStartingSeq(overlapMatrix)
    followSequences(overlapMatrix.collectAsMap(), startingSeq, startingSeq)
  }

  /**
   * Given an overlap matrix with good matches only, find a sequence which is
   * a key but not in a value anywhere, meaning it must be the starting sequence.
   * If there isn't exactly one, show an error and exit.
   */
  def findStartingSeq(matrix: OverlapMatrixReduced): String = {
    val values = matrix.values.map(_._1).collect()
    val startingSequences = matrix.filter { case (key, _) => !values.contains(key) }.keys

    startingSequences.collect().toList match {
      case Nil => throw new RuntimeException("ERROR: no starting sequence")
      case startingSeq :: Nil => startingSeq
      case seqs =>  throw new RuntimeException(s"ERROR: more than one starting sequence: $seqs")
    }
  }

  /**
   * From a set of sequence fragments, form a matrix of overlap information including
   * only matches that overlap by more than half their length.
   */
  def findOverlappingPairs(seqSet: SequenceSet): OverlapMatrixReduced =
    reduceToGoodMatches(buildOverlapMatrix(seqSet))

  /**
   * From a set of sequence fragments, form a matrix of overlap information.
   */
  def buildOverlapMatrix(seqSet: SequenceSet): OverlapMatrix = {
    val seqList = seqSet.collect.toList
    val matrix = seqSet.map(seq => (seq, seqList.filterNot(_ == seq)))
    matrix.map {
      case (keySeq, seqs) => (keySeq, seqs.map(seq => (seq, matchingLength(keySeq, seq))))
    }
  }

  /**
   * Given an overlap matrix, filter out pairs that don't overlap by more than half their length.
   */
  def reduceToGoodMatches(matrix: OverlapMatrix): OverlapMatrixReduced = {
    val matrixMaximums = matrix.mapValues(seq => seq.maxBy(_._2))
    matrixMaximums.filter { case (key, (seq, length)) => length > seq.length / 2 }
  }

  /**
   *  Take two strings and if the end of the first matches the beginning of the
   *  second return the length of the match, else return 0.
   *
   *  E.g. passing in (ATTAGACCTG, CCTGCCGGAA) should return 4
   */
  def matchingLength(seq1: String, seq2: String): Int = {
    def matches(seq1: String, seq2: String, length: Int): Boolean =
      (seq1 takeRight length) == (seq2 take length)
    (seq1.length to 0 by -1).find(matches(seq1, seq2, _)).getOrElse(0)
  }

  /**
   * Recursively traverse a reduced overlap matrix (i.e. a matrix which should
   * contain just good matches that are easy to put together) and form a combined sequence.
   */
  def followSequences(
      overlapMatrixReduced: Map[String, (String, Int)],
      seq: String,
      fullSeqSoFar: String): String =
    overlapMatrixReduced.get(seq) match {
      case None => fullSeqSoFar
      case Some((nextSeq, length)) =>
        val combinedSequence = fullSeqSoFar + nextSeq.substring(length)
        followSequences(overlapMatrixReduced, nextSeq, combinedSequence)
    }

  /**
   * Read the FASTA data from a file.
   * If a bottleneck, this could be optimized to do parsing in parallel.
   */
  def parseFastaData(filepath: String): Seq[String] = {

    def parseFastaSequence(text: String): String =
      text.stripLineEnd.indexOf(System.lineSeparator) match {
        case -1 => throw new RuntimeException(s"Could not parse sequence text $text")
        case newlineIdx => text.substring(newlineIdx + 1).replaceAll(System.lineSeparator, "")
      }

    val fastaText = scala.io.Source.fromFile(filepath).mkString
    val sequenceTexts: Seq[String] = fastaText.split(">").tail
    sequenceTexts.map(parseFastaSequence)
  }

  /**
   * Show the final result of the MergeFasta computation, i.e. a combined sequence.
   * If any sequence fragments were unmatched, show an error and list them.
   */
  def showResult(initialSeqSet: SequenceSet, combinedSeq: String): Unit = {
    val unmatchedSeqs = initialSeqSet.collect.filterNot(combinedSeq.contains(_)).toList
    if (unmatchedSeqs.nonEmpty)
      println(s"ERROR: these sequences were unmatched: $unmatchedSeqs")
    val combinedLength = combinedSeq.length
    println(s"\nCombined sequence (length $combinedLength):\n\n$combinedSeq\n")
  }
}