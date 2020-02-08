A minimalistic implementation of a Spark datasource.  

```python
# Basic usage
df.write \
    .option('redis_host', 'localhost') \
    .option('redis_port', '6379') \
    .option('redis_column_name', 'my_df_column_name') \
    .option('redis_set_key', 'my_set_name') \
    .format(source='spark.to.redis') \
    .save()
```
  
Grab the [latest release](https://gitlab.com/borismilner/spark_datasource/-/releases)