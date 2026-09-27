"""Run with: python examples/run.py --remote sc://localhost:50051"""

import argparse
import os

os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")

from pyspark.sql.connect.session import SparkSession
from pyspark_graph_algorithms import GraphAlgorithms


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", default="sc://localhost:50051")
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--iterations", type=int, default=10)
    args = parser.parse_args()
    spark = SparkSession.builder.remote(args.remote).create()
    try:
        vertices = spark.createDataFrame([(0,), (1,), (2,), (3,), (9,)], "id long")
        edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0), (2, 3)], "src long, dst long")
        graph = GraphAlgorithms(spark)
        with graph.pagerank(vertices, edges, max_iterations=args.iterations,
                            partitions=args.partitions) as result:
            result.frame.orderBy("id").show()
        with graph.wcc(vertices, edges, partitions=args.partitions) as result:
            result.frame.orderBy("id").show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
