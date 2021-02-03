import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import org.apache.spark.sql.{SaveMode, SparkSession}

// For implicit conversions like converting RDDs to DataFrames nuu
val browser = JsoupBrowser()
val dates = Seq("2000-2001", "2001-2002", "2002-2003","2003-2004", "2004-2005",
  "2005-2006","2006-2007", "2007-2008", "2008-2009","2009-2010", "2010-2011", "2011-2012",
  "2012-2013","2013-2014", "2014-2015", "2015-2016","2016-2017", "2017-2018", "2018-2019")

val spark = SparkSession.builder().master("local").getOrCreate()

val sqlContext = spark.sqlContext
import sqlContext.implicits._

val data = Seq(("","","","",""))
var allDataDF = data.toDF("homePosition", "awayPosition", "homeTeam", "awayTeam", "score")

dates.foreach { date =>
  for (week <- 10 to 38) {
    val doc = browser.get(s"https://www.worldfootball.net/schedule/eng-premier-league-$date-spieltag/$week/")
    val doc2 = browser.get(s"https://www.worldfootball.net/schedule/eng-premier-league-$date-spieltag/${week -1}/")

    val matchTableData = doc >> elementList(".box .data .standard_tabelle tbody")
    val matchTable = matchTableData >> elementList("tr .hell a, tr .dunkel a")

    val content = doc2 >> elementList(".content .portfolio")
    val leagueTableContent = content >> elementList("div:nth-child(7)")
    val leagueTable = leagueTableContent >> elementList("tbody tr:not(:first-child)")

    val league = leagueTable.flatMap(_ >> allText("td:nth-child(1), td:nth-child(3)"))

    var teamPositionArr: List[(String, String)] = List()

    val leagueFlattend = league.flatten
    for (n <- 0 to 19) {
      var position = leagueFlattend(n).head.toString
      var team = leagueFlattend(n).tail
      if (team.exists(_.isDigit)) {
        position += leagueFlattend(n).tail.head
        team = team.tail
      }

      teamPositionArr = teamPositionArr :+ ((position, team))
    }

    //all the matches and scores
    val matches = matchTable.flatMap(_ >> allText("[title]"))

    //Matches and Scores
    val matchesSplit = matches.grouped(3).toList

    //team postion and name
    val teamsPosition = teamPositionArr

    val matchesSplitDF = matchesSplit.map(x => x match {
      case List(a, b, c) => (a, b, c)
    }
    ).toDF("homeTeam", "awayTeam", "score")
    val teamsPositionDF = teamsPosition.toDF("position", "team")

    val joinedWithAwayPosition = teamsPositionDF.join(matchesSplitDF,
      matchesSplitDF("awayTeam") === teamsPositionDF("team"), "inner")
      .withColumnRenamed("position", "awayPosition")
      .drop("team")

    val allMatchesDf = teamsPositionDF.join(joinedWithAwayPosition,
      matchesSplitDF("homeTeam") === teamsPositionDF("team"), "inner")
      .withColumnRenamed("position", "homePosition")
      .drop("team") //.drop("homeTeam").drop("awayTeam")
    allDataDF = allDataDF.union(allMatchesDf)
  }
}
//allDataDF.filter("score !=''")
//allDataDF.write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save("/Users/paula/brian/json/AllPremierLeagueData.txt")
allDataDF.write.csv("/Users/paula/brian/csv2")
spark.close()
