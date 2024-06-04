/*
chcp 65001 && spark-shell -i C:/Users/isupo/Documents/s3.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("C:/Users/isupo/Documents/s3.xlsx")
		df1.show()
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl3a")
        .mode("overwrite").save()

		var q = """
		SELECT fieldname, GROUP_CONCAT(DISTINCT fieldvalue ORDER BY fieldvalue  SEPARATOR ', ') AS 'fieldvalue'
		FROM tasketl3a
		GROUP BY fieldname;
		"""
		
		import java.sql._;
		def sqlexecute(sql: String) = {
			var conn: Connection = null;
			var stmt: Statement = null;
			try {
				Class.forName("com.mysql.cj.jdbc.driver");
				conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?user=is_max&password=")
				stmt = conn.createStatement();
				stmt.executeUpdate(sql);
				println(sql+" complete");
			} catch {
				case e: Exception => println("exception caught: "+ e);
			}
		}
		sqlexecute("drop table spark.tasketl3b")
		sqlexecute(q)

		spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl3a")
        
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl3a")
        .mode("overwrite").save()
	println("task 3")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)