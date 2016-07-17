Context
=======

DNA may be sequenced by fragmenting a chromosome into many small subsequences. In the FASTA format,
each sequence is composed of characters: one of T/C/G/A. Here are four sample sequences:

```
>Frag_56
ATTAGACCTG
>Frag_57
CCTGCCGGAA
>Frag_58
AGACCTGCCG
>Frag_59
GCCGGAATAC
```

These sequences must be reassembled by finding overlapping parts. 
In this case the result would be `ATTAGACCTGCCGGAATAC`.
This project implements a simple algorithm for doing this pair matching. 


Approach
========

The number of sequence fragments in a real-life scenario would be many millions. To process this data quickly, it
is important to use an algorithm that can run tasks in parallel, and choose software that can distribute
those tasks over multiple processors (nodes) in a resilient way. [Spark](http://spark.apache.org) is the
software used here. The algorithm is implemented in Scala and goes as follows:

Compare each sequence with every other sequence to form a matrix. For each pair, calculate the
number of characters at the end of the first that match the characters at the beginning of the
second (0 if there is no match). For the toy data above, this matrix would look like this:

| Key sequence   | Other sequences                                 |
| ------------   | :---------------------------------------------- |
| ATTAGACCTG     | (CCTGCCGGAA, 4) (AGACCTGCCG, 7) (GCCGGAATAC, 1) |
| CCTGCCGGAA     | (ATTAGACCTG, 0) (AGACCTGCCG, 0) (GCCGGAATAC, 7) |
| AGACCTGCCG     | (ATTAGACCTG, 0) (CCTGCCGGAA, 7) (GCCGGAATAC, 4) |
| GCCGGAATAC     | (ATTAGACCTG, 0) (CTGCCGGAA, 1) (AGACCTGCCG, 0)  |
<br> 
For each row in the matrix, eliminate all values except the one with the longest match.
Also eliminate inadequate matches, i.e. where the common part is not more than half the
length of a sequence. The reduced matrix looks like: 

| Key sequence   | Sequence with overlap length |
| ------------   | :----------------------------| 
|ATTAGACCTG      | (AGACCTGCCG, 7)              |
|CCTGCCGGAA      | (GCCGGAATAC, 7)              |
|AGACCTGCCG      | (CCTGCCGGAA, 7)              |
      
Now get the starting sequence by finding the key that does not appear as a value anywhere: `ATTAGACCTG`. 
"Follow" this to AGACCTGCCG then follow that to `CTGCCGGAA` and that t `GCCGGAATAC` 
where the trail ends. Having collected this trail, combine each pair using the given 
length (7 in all cases here) to get a final sequence of `ATTAGACCTGCCGGAATAC`.       
       
To see how this algorithm is implemented in Scala, go to the [code](src/main/scala/fastamerge/MergeFasta.scala).

          
Running
=======
     
1. Make sure you have [Scala](http://www.scala-lang.org/download/) and [sbt](http://www.scala-sbt.org/download.html) installed.
2. If you don't have Spark 1.6 or greater, [download and install it](http://spark.apache.org/downloads.html).
3. Consider turning off excessive logging in Spark. 
 In Spark's `conf` directory, copy `log4j.properties.template` to  `log4j.properties`
 and set `log4j.rootCategory` to `WARN`.
4. Clone this project (or download and unzip).
5. In the project's root, from the command-line run `sbt package`.
6. In the project's root, from the command-line run 
  ```
  spark-submit --master local[4] target/scala-2.11/fastamerge_2.11-0.1.jar
  ```
  This processes the data in [`fasta_data.txt`](fasta_data.txt).                                   
7. At the end of the output you should see the combined FASTA sequence, 
 which will be the same as in the file [`fasta_data_expected_result.txt`](fasta_data_expected_result.txt).
      
      
Running the tests
=================
      
In this project's root, from the command-line run `sbt test`.
