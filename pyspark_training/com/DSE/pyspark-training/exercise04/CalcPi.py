from random import random
from pyspark.context import xrange, SparkContext


def sample(p):
    x, y = random(), random()
    return 1 if x * x + y * y < 1 else 0


# create spark session
sc = SparkContext("local", "rdd-training")

NUM_SAMPLES = 10  # Increment by 10x
count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample) \
    .reduce(lambda a, b: a + b)
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
