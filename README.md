A minimalistic implementation of Spark datasource.  

```python
# Basic usage
df.write \
    .option('redis_host', 'localhost') \
    .option('redis_port', '6379') \
    .option('redis_column_name', 'graph_node') \
    .option('redis_set_key', prefix) \
    .format(source='spark.to.redis') \
    .save()
```
  
Grab the [latest release](https://gitlab.com/borismilner/spark_datasource/-/releases)