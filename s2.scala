/*
chcp 65001 && spark-shell -i C:/Users/isupo/Documents/s2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.option("delimiter",",")
        .option("header", "true")
        //.option("encoding", "windows-1251")
        .csv("C:/Users/isupo/Documents/fifa_s2.csv")
		df1=df1
		//ID,Name,Age,Nationality,Overall,Potential,Club,Value,Wage,Preferred Foot,International Reputation,Skill Moves,Position,Joined,Contract Valid Until,Height,Weight,Release Clause
		.withColumn("ID",col("ID").cast("int"))
		.withColumn("Name",col("Name").cast("string"))
		.withColumn("Age",col("Age").cast("int"))
		.//withColumn("Age",col("Age"), when(col("Age").like("%20%"),"20")
		/*.withColumn("total_income",col("total_income").cast("float")).dropDuplicates()
		.withColumn("purpose_category", 
		when(col("purpose").like("%авто%"),"операции с автомобилем")
      when(col("purpose").like("%недвиж%")||col("purpose").like("%жиль%"),"операции с недвижимостью")
      //.otherwise("Unknown")
	  )*/
	/*.withColumn("total_income2",
	when(col("total_income").isNotNull,col("total_income"))
	.otherwise(avg("total_income").over(Window.partitionBy("income_type").orderBy("income_type"))))
	.withColumn("total_income2",col("total_income2").cast("float"))*/
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl2a")
        .mode("overwrite").save()
		df1.show()
val s = df1.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
val df2 = df1.agg(s.head, s.tail:_*)
val t = df2.columns.map(c => df2.select(lit(c).alias("col_name"), col(c).alias("null_count")))
val df_agg_col = t.reduce((df1, df2) => df1.union(df2))
df_agg_col.show()
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)