/*
 * Copyright (C) 2016 James McCabe <james@oranda.com>
 */

import MergeFasta._
import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest._
import Matchers._

class MergeFastaTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "MergeFastaTest"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null)
      sc.stop()
  }

  def fixture =
    new {
      val initialSeqSet: SequenceSet = sc.parallelize(Seq("ATTAGACCTG", "CCTGCCGGAA", "AGACCTGCCG", "GCCGGAATAC"))
      val matrix: OverlapMatrix = sc.parallelize(Seq(
        ("ATTAGACCTG", Seq(("CCTGCCGGAA", 4), ("AGACCTGCCG", 7), ("GCCGGAATAC", 1))),
        ("CCTGCCGGAA", Seq(("ATTAGACCTG", 1), ("AGACCTGCCG", 1), ("GCCGGAATAC", 7))),
        ("AGACCTGCCG", Seq(("ATTAGACCTG", 0), ("CCTGCCGGAA", 7), ("GCCGGAATAC", 4))),
        ("GCCGGAATAC", Seq(("ATTAGACCTG", 0), ("CCTGCCGGAA", 1), ("AGACCTGCCG", 0)))))
      val matrixReduced: OverlapMatrixReduced = sc.parallelize(Seq(
        ("ATTAGACCTG" -> ("AGACCTGCCG", 7)),
        ("CCTGCCGGAA" -> ("GCCGGAATAC", 7)),
        ("AGACCTGCCG" -> ("CCTGCCGGAA", 7))))
    }

  "followSequences" should "follow sequences in an overlap matrix to form a combined String" in {
    val overlapMatrix = fixture.matrixReduced.collectAsMap()
    val startingSeq =  "ATTAGACCTG"
    val fullSeqSoFar = startingSeq

    MergeFasta.followSequences(overlapMatrix, startingSeq, fullSeqSoFar) should equal ("ATTAGACCTGCCGGAATAC")
  }

  "reduceToGodMatches" should "filter out pairs that don't overlap by more than half their length" in {
    MergeFasta.reduceToGoodMatches(fixture.matrix).collect() should equal (fixture.matrixReduced.collect())
  }

  "buildOverlapMatrix" should "form a matrix of overlap information from sequence fragments" in {
    MergeFasta.buildOverlapMatrix(fixture.initialSeqSet).collect() should equal (fixture.matrix.collect())
  }

  "findOverlappingPairs" should "form a matrix of overlap information" in {
    MergeFasta.findOverlappingPairs(fixture.initialSeqSet).collect() should equal (fixture.matrixReduced.collect())
  }

  "findStartingSeq" should "find a sequence which is a key but not in a value anywhere" in {
    MergeFasta.findStartingSeq(fixture.matrixReduced) should equal ("ATTAGACCTG")
  }

  "mergeSequences" should "assemble sequence fragments into a single sequence" in {
    MergeFasta.mergeSequences(fixture.initialSeqSet) should equal ("ATTAGACCTGCCGGAATAC")
  }

  "matchingLength" should "find the length of the matching part in two overlapping Strings" in {
    MergeFasta.matchingLength("ATTAGACCTG", "AGACCTGCCG") should equal(7)
  }

  "matchingLength" should "return 0 if two Strings don't overlap, left to right" in {
    MergeFasta.matchingLength("ATTAGACCTG", "CGACCTGCCG") should equal(0)
  }
}
