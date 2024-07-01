from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:/Users/turtle/Documents/SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

print(sorted(result.items()))
sortedResults = collections.OrderedDict(sorted(result.items()))
print(sortedResults)
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
