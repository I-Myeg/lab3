import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.knowm.xchart._
import org.knowm.xchart.style.Styler

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

object SparkSqlReport {

  def main(args: Array[String]): Unit = {
    System.setProperty("java.awt.headless", "true")

    val inputPath =
      if (args.nonEmpty) args(0)
      else "data/data-1903-2025-12-23.csv"

    val outDirName =
      if (args.length > 1) args(1)
      else "out"

    val projectDir = new File(".").getCanonicalPath
    val outDirAbs = projectDir + File.separator + outDirName
    Files.createDirectories(Paths.get(outDirAbs))
    println("OUTPUT DIR = " + outDirAbs)

    val spark = SparkSession.builder()
      .appName("Spark SQL Report (Scala)")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "Europe/Bucharest")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.types._

    val schema = StructType(Seq(
      StructField("ID", StringType, true),
      StructField("Name", StringType, true),
      StructField("global_id", StringType, true),
      StructField("IsNetObject", StringType, true),
      StructField("OperatingCompany", StringType, true),
      StructField("TypeObject", StringType, true),
      StructField("AdmArea", StringType, true),
      StructField("District", StringType, true),
      StructField("Address", StringType, true),
      StructField("PublicPhone", StringType, true),
      StructField("SeatsCount", StringType, true),
      StructField("SocialPrivileges", StringType, true),
      StructField("Longitude_WGS84", StringType, true),
      StructField("Latitude_WGS84", StringType, true),
      StructField("geoData", StringType, true),
      StructField("geodata_center", StringType, true),
      StructField("_c16", StringType, true)
    ))

    val raw = spark.read
      .option("header", "true")
      .option("sep", ";")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .schema(schema)
      .csv(inputPath)

    val df0 = raw
      .filter(col("ID").isNotNull)
      .filter(trim(col("ID")) =!= lit("Код"))
      .drop("Unnamed: 16")
      .drop("_c16")

    val df = df0
      .withColumn("global_id", col("global_id").cast("long"))
      .withColumn("SeatsCount", regexp_replace(col("SeatsCount"), ",", ".").cast("int"))
      .withColumn("Longitude_WGS84", regexp_replace(col("Longitude_WGS84"), ",", ".").cast("double"))
      .withColumn("Latitude_WGS84", regexp_replace(col("Latitude_WGS84"), ",", ".").cast("double"))
      .withColumn(
        "IsNetObject_bool",
        when(lower(trim(col("IsNetObject"))) === "да", lit(true))
          .when(lower(trim(col("IsNetObject"))) === "yes", lit(true))
          .otherwise(lit(false))
      )
      .withColumn(
        "SocialPrivileges_bool",
        when(lower(trim(col("SocialPrivileges"))) === "да", lit(true))
          .when(lower(trim(col("SocialPrivileges"))) === "yes", lit(true))
          .otherwise(lit(false))
      )

    df.cache()
    df.createOrReplaceTempView("catering")

    println("=== Schema ===")
    df.printSchema()
    println(s"=== Rows: ${df.count()} ===")
    println("=== Sample ===")
    df.select("ID","Name","TypeObject","AdmArea","District","SeatsCount","IsNetObject","SocialPrivileges")
      .show(10, truncate = false)

    val q1 = spark.sql("""
      SELECT TypeObject, COUNT(*) AS cnt
      FROM catering
      WHERE TypeObject IS NOT NULL AND trim(TypeObject) <> ''
      GROUP BY TypeObject
      ORDER BY cnt DESC
      LIMIT 10
    """)
    q1.show(false)

    println("EX1: saving chart...")
    barChart(
      q1,
      xCol = "TypeObject",
      yCol = "cnt",
      title = "Top-10 видов объектов (TypeObject) по количеству",
      outPathNoExt = s"$outDirAbs/ex1_top10_typeobject"
    )
    println("EX1: chart saved OK")

    val q2 = spark.sql("""
      SELECT IsNetObject_bool AS isNet, COUNT(*) AS cnt
      FROM catering
      GROUP BY IsNetObject_bool
      ORDER BY isNet
    """)
    q2.show(false)

    println("EX2: saving chart...")
    pieChart(
      q2,
      labelCol = "isNet",
      valueCol = "cnt",
      title = "Доля сетевых объектов (IsNetObject)",
      outPathNoExt = s"$outDirAbs/ex2_net_share_pie"
    )
    println("EX2: chart saved OK")

    val topAreas: Seq[String] = spark.sql("""
      SELECT AdmArea, COUNT(*) AS cnt
      FROM catering
      WHERE AdmArea IS NOT NULL AND trim(AdmArea) <> ''
      GROUP BY AdmArea
      ORDER BY cnt DESC
      LIMIT 6
    """).select(col("AdmArea").cast("string"))
      .collect()
      .map(r => r.getString(0))
      .toSeq

    val q3 = df
      .filter(col("SeatsCount").isNotNull && col("SeatsCount") > 0)
      .filter(col("AdmArea").isin(topAreas: _*))
      .select(col("AdmArea").cast("string").as("AdmArea"), col("SeatsCount").cast("double").as("SeatsCount"))

    println("EX3: saving chart...")
    boxPlot(
      q3,
      categoryCol = "AdmArea",
      valueCol = "SeatsCount",
      title = "Распределение SeatsCount по топ-6 AdmArea",
      outPathNoExt = s"$outDirAbs/ex3_seats_boxplot"
    )
    println("EX3: chart saved OK")

    val q4 = spark.sql("""
      SELECT
        AdmArea,
        SUM(CASE WHEN SocialPrivileges_bool THEN 1 ELSE 0 END) AS with_priv,
        SUM(CASE WHEN NOT SocialPrivileges_bool THEN 1 ELSE 0 END) AS without_priv
      FROM catering
      WHERE AdmArea IS NOT NULL AND trim(AdmArea) <> ''
      GROUP BY AdmArea
      ORDER BY (with_priv + without_priv) DESC
      LIMIT 10
    """)
    q4.show(false)

    println("EX4: saving chart...")
    stackedBarChart(
      q4,
      xCol = "AdmArea",
      series = Seq("with_priv" -> "С льготами", "without_priv" -> "Без льгот"),
      title = "Топ-10 AdmArea: соцльготы (stacked)",
      outPathNoExt = s"$outDirAbs/ex4_privileges_stacked"
    )
    println("EX4: chart saved OK")

    val q5 = spark.sql("""
      SELECT
        District,
        COUNT(*) AS n,
        ROUND(AVG(SeatsCount), 2) AS avg_seats
      FROM catering
      WHERE SeatsCount IS NOT NULL
        AND SeatsCount > 0
        AND SeatsCount <= 500
        AND District IS NOT NULL AND trim(District) <> ''
      GROUP BY District
      HAVING COUNT(*) >= 30
      ORDER BY avg_seats DESC, n DESC
      LIMIT 15
    """)
    q5.show(false)


    println(s"\nDONE. Charts + CSV saved into: $outDirAbs")
    spark.stop()
  }


  private def barChart(df: DataFrame, xCol: String, yCol: String, title: String, outPathNoExt: String): Unit = {
    val rows = df.select(col(xCol).cast("string"), col(yCol).cast("double")).collect()

    val xJava: java.util.List[String] =
      rows.map(r => Option(r.getString(0)).getOrElse("")).toList.asJava

    val yJava: java.util.List[java.lang.Number] =
      rows.map(r => Double.box(r.getDouble(1)) : java.lang.Number).toList.asJava

    val chart = new CategoryChartBuilder()
      .width(1200).height(600)
      .title(title)
      .xAxisTitle(xCol).yAxisTitle(yCol)
      .build()

    chart.getStyler.setLegendVisible(false)
    chart.getStyler.setPlotGridLinesVisible(true)
    chart.getStyler.setXAxisLabelRotation(35)

    chart.addSeries(yCol, xJava, yJava)
    BitmapEncoder.saveBitmap(chart, outPathNoExt, BitmapEncoder.BitmapFormat.PNG)
  }

  private def stackedBarChart(df: DataFrame, xCol: String, series: Seq[(String, String)], title: String, outPathNoExt: String): Unit = {
    val rows = df.collect()

    val xJava: java.util.List[String] =
      rows.map(r => Option(r.getAs[String](xCol)).getOrElse("")).toList.asJava

    val chart = new CategoryChartBuilder()
      .width(1300).height(650)
      .title(title)
      .xAxisTitle(xCol).yAxisTitle("count")
      .build()

    chart.getStyler.setStacked(true)
    chart.getStyler.setLegendPosition(Styler.LegendPosition.InsideNE)
    chart.getStyler.setXAxisLabelRotation(35)

    series.foreach { case (colName, label) =>
      val yJava: java.util.List[java.lang.Number] =
        rows.map(r => r.getAs[Number](colName).doubleValue())
          .map(v => Double.box(v) : java.lang.Number)
          .toList.asJava

      chart.addSeries(label, xJava, yJava)
    }

    BitmapEncoder.saveBitmap(chart, outPathNoExt, BitmapEncoder.BitmapFormat.PNG)
  }

  private def pieChart(df: DataFrame, labelCol: String, valueCol: String, title: String, outPathNoExt: String): Unit = {
    val rows = df.select(col(labelCol).cast("string"), col(valueCol).cast("double")).collect()

    val chart = new PieChartBuilder()
      .width(900).height(650)
      .title(title)
      .build()

    rows.foreach { r =>
      val label = Option(r.getString(0)).getOrElse("null")
      val value = r.getDouble(1)
      chart.addSeries(label, value)
    }

    BitmapEncoder.saveBitmap(chart, outPathNoExt, BitmapEncoder.BitmapFormat.PNG)
  }

  private def boxPlot(df: DataFrame, categoryCol: String, valueCol: String, title: String, outPathNoExt: String): Unit = {
    val rows = df
      .select(col(categoryCol).cast("string"), col(valueCol).cast("double"))
      .collect()

    val grouped: Map[String, List[java.lang.Number]] =
      rows.groupBy(r => Option(r.getString(0)).getOrElse("")).map { case (k, arr) =>
        val nums: List[java.lang.Number] =
          arr.map(r => Double.box(r.getDouble(1)) : java.lang.Number).toList
        k -> nums
      }

    val chart = new BoxChartBuilder()
      .width(1400).height(650)
      .title(title)
      .xAxisTitle(categoryCol).yAxisTitle(valueCol)
      .build()

    chart.getStyler.setLegendVisible(false)
    chart.getStyler.setXAxisLabelRotation(35)

    grouped.toSeq.sortBy(_._1).foreach { case (cat, values) =>
      chart.addSeries(cat, values.asJava)
    }

    BitmapEncoder.saveBitmap(chart, outPathNoExt, BitmapEncoder.BitmapFormat.PNG)
  }
}
