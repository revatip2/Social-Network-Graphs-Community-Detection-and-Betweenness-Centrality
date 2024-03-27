from pyspark import SparkContext
import sys
import time
import os
from itertools import combinations
from graphframes import *
from pyspark.sql import *


os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell"

filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
output_file_path = sys.argv[3]


spark = SparkSession.builder.appName("graphs").getOrCreate()

sc = SparkContext.getOrCreate()

start = time.time()

yelp_data_rdd = sc.textFile(input_file_path)
row_one = yelp_data_rdd.first()
yelp_data_rdd = yelp_data_rdd.filter(lambda a: a != row_one).map(lambda a: a.split(','))

biz_rdd = yelp_data_rdd.map(lambda row: (row[0], {row[1]}))
uz_biz_rdd = biz_rdd.reduceByKey(lambda a, b: a.union(b))
uz_biz_data = dict(uz_biz_rdd.collect())

user_ids = uz_biz_rdd.keys().collect()
user_pairs = list(combinations(user_ids, 2))

user_pairs_rdd = sc.parallelize(user_pairs)

edges = []
for u in user_pairs:
    (uz1, uz2) = u
    biz1 = uz_biz_data[uz1]
    biz2 = uz_biz_data[uz2]
    common_businesses = biz1.intersection(biz2)
    if len(common_businesses) >= filter_threshold:
        edges.append((uz1, uz2))
        edges.append((uz2, uz1))

unique_user_ids = set()
for tuple_item in edges:
    unique_user_ids.add(tuple_item[0])
    unique_user_ids.add(tuple_item[1])

unique_user_ids_list = list(unique_user_ids)
print(len(unique_user_ids_list))

edges_df = spark.createDataFrame(edges, ["src", "dst"])
vertices_df = spark.createDataFrame([Row(id=user_id) for user_id in unique_user_ids_list])

gf = GraphFrame(vertices_df, edges_df)
result = gf.labelPropagation(maxIter=5)

print(result.rdd.map(lambda x: (x[1],x[0])).take(5))

grouped_data_rdd = result.rdd.map(lambda x: (x[1], x[0])).groupByKey()
sorted_data_rdd = grouped_data_rdd.map(lambda x: sorted(list(x[1]))).sortBy(lambda x: (x[0])).sortBy(lambda x: (len(x)))

sorted_data = sorted_data_rdd.collect()
print(len(edges))

with open(output_file_path, 'w') as text_file:
    for i in edges:
        text_file.write(str(edges)+ "\n")


end = time.time()

print('Duration: ',end - start)

sc.stop()