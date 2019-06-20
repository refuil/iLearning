package com.xiaoi.spark.example

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object ExamplesIDFAndWord2Vec {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    /* Replace 'PATH' with the path to the 20 Newsgroups Data */
    val path = "/PATH/20news-bydate-train/*"
    val rdd = sc.wholeTextFiles(path)//读取所有文件
    // count the number of records in the dataset
    println(rdd.count)
    /*
    ...
    14/10/12 14:27:54 INFO FileInputFormat: Total input paths to process : 11314
    ...
    11314
    */
    val text = rdd.map { case (file, text) => text }//file 是 文件 dir，text是文件内容


    // split text on any non-word tokens
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase))
    println(nonWordSplit.distinct.count)
    // 130126
    // inspect a look at a sample of tokens
    println(nonWordSplit.distinct.sample(true, 0.3, 42).take(100).mkString(","))
    /*
    bone,k29p,w1w3s1,odwyer,dnj33n,bruns,_congressional,mmejv5,mmejv5,artur,125215,entitlements,beleive,1pqd9hinnbmi,
    jxicaijp,b0vp,underscored,believiing,qsins,1472,urtfi,nauseam,tohc4,kielbasa,ao,wargame,seetex,museum,typeset,pgva4,
    dcbq,ja_jp,ww4ewa4g,animating,animating,10011100b,10011100b,413,wp3d,wp3d,cannibal,searflame,ets,1qjfnv,6jx,6jx,
    detergent,yan,aanp,unaskable,9mf,bowdoin,chov,16mb,createwindow,kjznkh,df,classifieds,hour,cfsmo,santiago,santiago,
    1r1d62,almanac_,almanac_,chq,nowadays,formac,formac,bacteriophage,barking,barking,barking,ipmgocj7b,monger,projector,
    hama,65e90h8y,homewriter,cl5,1496,zysec,homerific,00ecgillespie,00ecgillespie,mqh0,suspects,steve_mullins,io21087,
    funded,liberated,canonical,throng,0hnz,exxon,xtappcontext,mcdcup,mcdcup,5seg,biscuits
    */


    // filter out numbers
    val regex = """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches)
    println(filterNumbers.distinct.count)
    // 84912
    println(filterNumbers.distinct.sample(true, 0.3, 42).take(100).mkString(","))

    // examine potential stopwords
    val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)
    val oreringDesc = Ordering.by[(String, Int), Int](_._2)
    println(tokenCounts.top(20)(oreringDesc).mkString("\n"))
    /*
    (the,146532)
    (to,75064)
    (of,69034)
    (a,64195)
    (ax,62406)
    (and,57957)
    (i,53036)
    (in,49402)
    (is,43480)
    (that,39264)
    (it,33638)
    (for,28600)
    (you,26682)
    (from,22670)
    (s,22337)
    (edu,21321)
    (on,20493)
    (this,20121)
    (be,19285)
    (t,18728)
    */

    // filter out stopwords
    val stopwords = Set(
      "the","a","an","of","or","in","for","by","on","but", "is", "not", "with", "as", "was", "if",
      "they", "are", "this", "and", "it", "have", "from", "at", "my", "be", "that", "to"
    )
    val tokenCountsFilteredStopwords = tokenCounts.filter { case (k, v) => !stopwords.contains(k) }
    println(tokenCountsFilteredStopwords.top(20)(oreringDesc).mkString("\n"))

    // filter out rare tokens with total occurence < 2
    val rareTokens = tokenCounts.filter{ case (k, v) => v < 2 }.map { case (k, v) => k }.collect.toSet
    val tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter { case (k, v) => k.size >= 2 }
    println(tokenCountsFilteredSize.top(20)(oreringDesc).mkString("\n"))

    //    val tokenCountsFilteredAll = tokenCountsFilteredSize.filter { case (k, v) => !rareTokens.contains(k) }
    //    println(tokenCountsFilteredAll.top(20)(oreringAsc).mkString("\n"))

    def tokenize(line: String): Seq[String] = {
      line.split("""\W+""")
        .map(_.toLowerCase)
        .filter(token => regex.pattern.matcher(token).matches)
        .filterNot(token => stopwords.contains(token))
        .filterNot(token => rareTokens.contains(token))
        .filter(token => token.size >= 2)
        .toSeq
    }

    // check that our tokenizer achieves the same result as all the steps above
    println(text.flatMap(doc => tokenize(doc)).distinct.count)
    // 51801
    // tokenize each document
    val tokens = text.map(doc => tokenize(doc))
    println(tokens.first.take(20))

    // === train TF-IDF model === //

    import org.apache.spark.mllib.linalg.{ SparseVector => SV }
    import org.apache.spark.mllib.feature.HashingTF
    import org.apache.spark.mllib.feature.IDF
    // set the dimensionality of TF-IDF vectors to 2^18
    val dim = math.pow(2, 18).toInt
    val hashingTF = new HashingTF(dim)

    val tf = hashingTF.transform(tokens)
    // cache data in memory
    tf.cache
    val v = tf.first.asInstanceOf[SV]
    println(v.size)
    // 262144
    println(v.values.size)
    // 706
    println(v.values.take(10).toSeq)
    // WrappedArray(1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 2.0, 1.0, 1.0)
    println(v.indices.take(10).toSeq)
    // WrappedArray(313, 713, 871, 1202, 1203, 1209, 1795, 1862, 3115, 3166)

    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    val v2 = tfidf.first.asInstanceOf[SV]

    println(v2.values.size)
    // 706
    println(v2.values.take(10).toSeq)
    // WrappedArray(2.3869085659322193, 4.670445463955571, 6.561295835827856, 4.597686109673142,  ...
    println(v2.indices.take(10).toSeq)
    // WrappedArray(313, 713, 871, 1202, 1203, 1209, 1795, 1862, 3115, 3166)


    val hockeyText = rdd.filter { case (file, text) =>
      file.contains("hockey") }
    val hockeyTF = hockeyText.mapValues(doc =>
      hashingTF.transform(tokenize(doc)))
    val hockeyTfIdf = idf.transform(hockeyTF.map(_._2))

    // compute cosine similarity using Breeze
    import breeze.linalg._
    val hockey1 = hockeyTfIdf.sample(true, 0.1, 42).first.asInstanceOf[SV]
    val breeze1 = new SparseVector(hockey1.indices, hockey1.values, hockey1.size)
    val hockey2 = hockeyTfIdf.sample(true, 0.1, 43).first.asInstanceOf[SV]
    val breeze2 = new SparseVector(hockey2.indices, hockey2.values, hockey2.size)
    val cosineSim = breeze1.dot(breeze2) / (norm(breeze1) * norm(breeze2))
    println(cosineSim)
    // 0.060250114361164626

    /**
      * word2vec
      */
    import org.apache.spark.mllib.feature.Word2Vec
    val word2vec = new Word2Vec()
    word2vec.setSeed(42) // we do this to generate the same results each time
    val word2vecModel = word2vec.fit(tokens)
    // evaluate a few words
    word2vecModel.findSynonyms("hockey", 20).foreach(println)
    /*
    (sport,0.6828256249427795)
    (ecac,0.6718048453330994)
    (hispanic,0.6519884467124939)
    (glens,0.6447514891624451)
    (woofers,0.6351765394210815)
    (boxscores,0.6009076237678528)
    (tournament,0.6006366014480591)
    (champs,0.5957855582237244)
    (aargh,0.584071934223175)
    (playoff,0.5834275484085083)
    (ahl,0.5784651637077332)
    (ncaa,0.5680188536643982)
    (pool,0.5612311959266663)
    (olympic,0.5552600026130676)
    (champion,0.5549421310424805)
    (filinuk,0.5528956651687622)
    (yankees,0.5502706170082092)
    (motorcycles,0.5484763979911804)
    (calder,0.5481109023094177)
    (rec,0.5432182550430298)
    */
    word2vecModel.findSynonyms("legislation", 20).foreach(println)


  }
}
