import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
warnings.filterwarnings("ignore")
t0=time.time()
con=create_engine("mysql://is_max:@localhost/spark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()
columns = ["id","category_id","rate","title","author"]

data = [("1", "1", "5", "java", "author1"),
        ("2", "1", "5", "scala", "author2"),
        ("3", "1", "5", "python", "author3")]

#задание 1
if 1==2:
    df = spark.createDataFrame(data, columns)
    df.show()

    df.withColumn("id",col("id").cast("int"))\
        .withColumn("category_id",col("category_id").cast("int"))\
        .withColumn("rate",col("rate").cast("int"))\
        .withColumn("dt", current_timestamp())\
        .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4a")\
        .mode("overwrite").save()

    df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("C:/Users/isupo/Documents/s4.xlsx").where(col("title") == "news")

    df1.show()

    df1.withColumn("dt", current_timestamp())\
        .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4a")\
        .mode("append").save()

#задание 4
if 1==1:
    '''
    sql.execute("""drop table if exists spark.`tasketl4b`""",con)
    sql.execute("""CREATE TABLE if not exists spark.`tasketl4b` (
            `№` INT(10) NULL DEFAULT NULL,
            `Месяц` DATE NULL DEFAULT NULL,
            `Сумма платежа` FLOAT NULL DEFAULT NULL,
            `Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
            `Платеж по процентам` FLOAT NULL DEFAULT NULL,
            `Остаток долга` FLOAT NULL DEFAULT NULL,
            `проценты` FLOAT NULL DEFAULT NULL,
            `долг` FLOAT NULL DEFAULT NULL
    )
    COLLATE='utf8mb4_0900_ai_ci'
    ENGINE=InnoDB""",con)
    '''
    from pyspark.sql.window import Window
    from pyspark.sql.functions import sum as sum1
    w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df1 = spark.read.format("com.crealytics.spark.excel")\
            .option("sheetName", "Sheet1")\
            .option("useHeader", "false")\
            .option("treatEmptyValuesAsNulls", "false")\
            .option("inferSchema", "true").option("addColorColumns", "true")\
            .option("usePlainNumberFormat","true")\
            .option("startColumn", 0)\
            .option("endColumn", 99)\
            .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
            .option("maxRowsInMemory", 20)\
            .option("excerptSize", 10)\
            .option("header", "true")\
            .format("excel")\
            .load("C:/Users/isupo/Documents/s4_2.xlsx").limit(1000)\
            .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
            .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
    df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")\
            .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4b")\
            .mode("append").save()

#задание домашнее
    
    df2 = spark.read.format("com.crealytics.spark.excel")\
            .option("sheetName", "2")\
            .option("useHeader", "false")\
            .option("treatEmptyValuesAsNulls", "false")\
            .option("inferSchema", "true").option("addColorColumns", "true")\
            .option("usePlainNumberFormat","true")\
            .option("startColumn", 0)\
            .option("endColumn", 99)\
            .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
            .option("maxRowsInMemory", 20)\
            .option("excerptSize", 10)\
            .option("header", "true")\
            .format("excel")\
            .load("C:/Users/isupo/Documents/s4_2.xlsx").limit(1000)\
            .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
            .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
    '''
    df2.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")\
            .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4c")\
            .mode("append").save()'''

    df3 = spark.read.format("com.crealytics.spark.excel")\
            .option("sheetName", "3")\
            .option("useHeader", "false")\
            .option("treatEmptyValuesAsNulls", "false")\
            .option("inferSchema", "true").option("addColorColumns", "true")\
            .option("usePlainNumberFormat","true")\
            .option("startColumn", 0)\
            .option("endColumn", 99)\
            .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
            .option("maxRowsInMemory", 20)\
            .option("excerptSize", 10)\
            .option("header", "true")\
            .format("excel")\
            .load("C:/Users/isupo/Documents/s4_2.xlsx").limit(1000)\
            .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
            .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
    df3.show()
    '''
    df3.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=is_max&password=")\
            .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4d")\
            .mode("append").save()'''
    
    df4 = df1.toPandas()
    # Get current axis 
    ax = plt.gca()
    ax.ticklabel_format(style='plain')
    # bar plot
    df4.plot(kind='line', 
            x='№', 
            y='долг', 
            color='green', ax=ax)
    df4.plot(kind='line', 
            x='№', 
            y='проценты', 
            color='red', ax=ax)
    # set the title 
    plt.title('Выплаты')
    plt.grid ( True )
    ax.set(xlabel=None)
    # show the plot 
    plt.show()

    df5 = df2.toPandas()
    # Get current axis 
    ax = plt.gca()
    ax.ticklabel_format(style='plain')
    # bar plot
    df5.plot(kind='line', 
            x='№', 
            y='долг', 
            color='green', ax=ax)
    df5.plot(kind='line', 
            x='№', 
            y='проценты', 
            color='red', ax=ax)
    # set the title 
    plt.title('Выплаты')
    plt.grid ( True )
    ax.set(xlabel=None)
    # show the plot 
    plt.show()

    df6 = df3.toPandas()
    # Get current axis 
    ax = plt.gca()
    ax.ticklabel_format(style='plain')
    # bar plot
    df6.plot(kind='line', 
            x='№', 
            y='долг', 
            color='green', ax=ax)
    df6.plot(kind='line', 
            x='№', 
            y='проценты', 
            color='red', ax=ax)
    # set the title 
    plt.title('Выплаты')
    plt.grid ( True )
    ax.set(xlabel=None)
    # show the plot 
    plt.show()
    
spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))
