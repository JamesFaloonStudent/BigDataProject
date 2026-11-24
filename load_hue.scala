// Load the modified HUE dataset (Makonin, 2019; 10-second intervals) into HDFS
// as one file per day:
//
//     /energyDataset/YYYY/MM/DD/datafile_YYYY_MM_DD.txt
//
// Input files: consumption_N.txt (tab-separated, with a header line)
//     LOG_ID  HOUSE_ID  CONDATE  CONHOUR  ENERGY_READING  FLAG


import org.apache.spark.sql.functions._

// Define Starting Variables
val src  = "hdfs:///raw/hue/consumption_1.txt"   // where the consumption files are
val tmp  = "hdfs:///tmp/hue_stage"               // temporary Spark output
val root = "/energyDataset"                      // final HDFS location
val writeHeader = true                           // header line in each daily file


// Create a tuple of column names and their expected positions in the input files
val cols = Seq("LOG_ID", "HOUSE_ID", "CONDATE", "CONHOUR", "ENERGY_READING", "FLAG")

// 1. Read the files as text and split on whitespace (works whether the
//    columns are separated by tabs or spaces), dropping the header lines
val raw = spark.read.text(src)
  .select(split(trim(col("value")), "\\s+").as("f"))
  .filter(size(col("f")) === cols.length && col("f")(0) =!= "LOG_ID")

val df = raw.select(cols.zipWithIndex.map { case (c, i) => col("f")(i).as(c) }: _*)
  .withColumn("year",  substring(col("CONDATE"), 1, 4))
  .withColumn("month", substring(col("CONDATE"), 6, 2))
  .withColumn("day",   substring(col("CONDATE"), 9, 2))
  .filter(col("CONDATE").rlike("^\\d{4}-\\d{2}-\\d{2}$"))

// 2. Write one file per day into a temporary folder
//    (Spark names these year=YYYY/month=MM/day=DD/part-xxxxx.csv)
df.repartition(col("year"), col("month"), col("day"))
  .sortWithinPartitions(col("HOUSE_ID").cast("int"), col("CONHOUR"), col("LOG_ID").cast("long"))
  .write
  .mode("overwrite")
  .option("sep", "\t")
  .option("header", writeHeader.toString)
  .partitionBy("year", "month", "day")
  .csv(tmp)

// 3. Move each part file to /energyDataset/YYYY/MM/DD/datafile_YYYY_MM_DD.txt
val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
val parts = fs.globStatus(new org.apache.hadoop.fs.Path(tmp + "/year=*/month=*/day=*/part-*"))

parts.foreach { st =>
  val p = st.getPath // p = path 
  val d = p.getParent.getName.split("=")(1) // d = day 
  val m = p.getParent.getParent.getName.split("=")(1) // m = month
  val y = p.getParent.getParent.getParent.getName.split("=")(1) // y = year

  val destDir = new org.apache.hadoop.fs.Path(s"$root/$y/$m/$d")
  val dest    = new org.apache.hadoop.fs.Path(destDir, s"datafile_${y}_${m}_${d}.txt")
  fs.mkdirs(destDir)
  if (fs.exists(dest)) fs.delete(dest, false)   // overwrite on re-runs
  fs.rename(p, dest)
}

// 4. Clean up the temporary folder
fs.delete(new org.apache.hadoop.fs.Path(tmp), true)

println(s"Done: wrote ${parts.length} daily files under $root/YYYY/MM/DD/")
System.exit(0)
