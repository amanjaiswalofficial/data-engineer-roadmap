from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("TotalByCustomer")
sc = SparkContext(conf=conf)


def get_data(item):
    split_row = item.split(",")
    customer_id = int(split_row[0])
    amount = float(split_row[2])
    return (customer_id, amount)


input = sc.textFile(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/customer-orders.csv"
)

customers = input.map(get_data)
total_amount_spent = customers.reduceByKey(lambda x, y: int(x + y))
total_amount_spent_sorted = (
    total_amount_spent.map(lambda x: (x[1], x[0])).sortByKey().collect()
)

for item in total_amount_spent_sorted:
    print(item)
