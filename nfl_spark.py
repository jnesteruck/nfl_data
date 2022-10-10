from pyspark.sql import SparkSession
# from methods import *

# ---------- BUILD SPARK SESSION ---------- #
spark = SparkSession.builder \
    .master("local") \
    .appName("mlb") \
    .getOrCreate()

# ---------- SPARK CONTEXT ---------- #
sc = spark.sparkContext
sc.setLogLevel("WARN")

# ---------- SET FILEPATH ---------- #
path = "file:/mnt/c/Users/jan94/OneDrive/Documents/code/MLB_PROJECT/baseball-ref-files/mlb2022.csv"

mlb2022DF = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path)

# mlb2022DF.show()
mlb2022DF.createOrReplaceTempView("mlb2022")

# spark.sql("SELECT MONTH(Date) Month, ROUND(SUM(R)/COUNT(*), 2) RunsScored, ROUND(SUM(RA)/COUNT(*), 2) \
#            RunsAllowed, ROUND(SUM(R-RA)/COUNT(*), 2) RunDiff FROM mlb2022 WHERE Tm='LAD'\
#            GROUP BY Month HAVING RunsScored IS NOT NULL ORDER BY Month;").show()


# spark.sql("SELECT Tm, ROUND(SUM(R)/COUNT(*), 2) RunsScored, ROUND(SUM(RA)/COUNT(*), 2) RunsAllowed, \
#            ROUND(SUM(R-RA)/COUNT(*), 2) RunDiff FROM mlb2022 GROUP BY Tm ORDER BY RunDiff DESC, Tm;").show(30)

# spark.sql("SELECT Date, Tm, SUM(R) RunsScored, SUM(RA) RunsAllowed, SUM(R-RA) RunDiff \
#            FROM mlb2022 GROUP BY Tm ORDER BY Tm, Date;").show()

# spark.sql("SELECT t1.No, MAX(t1.Date), t1.Tm, t1.R, SUM(t2.R) RunsScored, t1.RA, SUM(t2.RA) RunsAllowed, \
#            t1.R-t1.RA, SUM(t2.R-t2.RA) RunDiff FROM mlb2022 t1 JOIN mlb2022 t2 ON t1.Date>=t2.Date \
#            WHERE t1.Tm='LAD' GROUP BY t1.No, t1.Tm, t1.R, t1.RA ORDER BY t1.No;").show()

spark.sql("SELECT * FROM mlb2022 WHERE Tm='LAD';").show()

spark.stop()