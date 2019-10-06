package com.example

import com.example.entity.{Crime, OffenseCode}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object BostonCrimesMap extends App {

    val crimeFilepath = args(0)
    val offenseCodesFilePath = args(1)
    val resultFilePath = args(2)




  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("boston_crimes_shitov")
    .getOrCreate()

  import spark.implicits._

  val crimes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(crimeFilepath)
    .as[Crime]


  val offense_codes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(offenseCodesFilePath)
    .withColumn("CRIME_TYPE", substring_index($"NAME", "-", 1))
    .as[OffenseCode]


  val offense_codes_br = spark.sparkContext.broadcast(offense_codes)

  val filteredCrimes = crimes
    .filter($"DISTRICT".isNotNull)

  val crimesWithOffenceCodes = filteredCrimes
    .join(offense_codes_br.value, filteredCrimes("OFFENSE_CODE") === offense_codes_br.value("CODE"))
    .select("INCIDENT_NUMBER", "DISTRICT", "MONTH", "Lat", "Long", "CRIME_TYPE").cache()

  val crimesDistrictAnalytics = filteredCrimes
    .groupBy($"DISTRICT")
    .agg(expr("COUNT(INCIDENT_NUMBER) as crimes_total"),
      expr("AVG(Lat) as lat"),
      expr("AVG(Long) as lng")
    )


  val crimesByDistrictByMonth = crimesWithOffenceCodes
    .groupBy($"DISTRICT", $"MONTH")
    .agg(expr("count(INCIDENT_NUMBER) as CRIMES_CNT")).createOrReplaceTempView("crimesByDistrictByMonth")

  val crimesDistrictMedian = spark.sql(
    "select " +
      " DISTRICT" +
      " ,percentile_approx(CRIMES_CNT, 0.5) as crimes_monthly " +
      " from crimesByDistrictByMonth" +
      " group by DISTRICT")


  val crimesByDistrictByCrimeTypes = crimesWithOffenceCodes
    .groupBy($"DISTRICT", $"CRIME_TYPE")
    .agg(expr("count(INCIDENT_NUMBER) as CRIMES_CNT"))
    .selectExpr("*", "row_number() over(partition by DISTRICT order by CRIMES_CNT desc) as rn")
    .filter($"rn" <= 3)
    .drop($"rn")
    .drop($"CRIMES_CNT")
    .groupBy($"DISTRICT")
    .agg(concat_ws(", ", collect_list($"CRIME_TYPE")).alias("frequent_crime_types"))


  val finalResult =
    crimesDistrictAnalytics
      .join(crimesDistrictMedian, "DISTRICT")
      .join(crimesByDistrictByCrimeTypes, "DISTRICT")
      .select($"DISTRICT", $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")

  finalResult.repartition(1).write.mode("OVERWRITE").parquet(resultFilePath)


}





