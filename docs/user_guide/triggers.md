# Triggers
An important aspect of data pipelines in general (and ADF) is not only the structure of the pipelines, but also when to execute them. adfPy currently supports scheduled triggers (other trigger types are planned to be added in the future). To make life easy, adfPy allows you to use familiar cron-like syntax to specify when the pipeline is to be executed:
```python
pipeline = AdfPipeline(name="copyPipeline", schedule="* * 5 * *")
```
For figuring out what cron expression to use, we recommend [Crontab.guru](https://crontab.guru)

## Start dates
Naturally, when specifying a schedule it is important to think about what the start date should be. If you do not specify a start date, adfPy will use 
```python
datetime.now(tz=timezone.utc)
```
It's strongly recommended to set a valid start time yourself. To do this, pass it in as an additional parameter to your Pipeline:
```python
pipeline = AdfPipeline(name="copyPipeline", schedule="* * 5 * *", start_time=datetime(2022,7,13,2))
```

## Unsupported Cron expressions
Due to ADF's scheduling design, there are a number of cron expressions that unfortunately cannot be supported. The expressions that are not supported are best documented with concrete examples:

{"* * 5 * 5": "At every minute on day-of-month 5 and on Friday."},
{"5 * 5 * 5": "At minute 5 on day-of-month 5 and on Friday."},
{"* 5 5 * 5": "At every minute past hour 5 on day-of-month 5 and on Friday."},
{"5 5 5 * 5": "At 05:05 on day-of-month 5 and on Friday."}

| Cron expression | Natural language translation |
|-----------------| ---------------------------- |
| `* * 5 * 5`     | At every minute on day-of-month 5 and on Friday.|
| `5 * 5 * 5`     | At minute 5 on day-of-month 5 and on Friday.|
| `* 5 5 * 5`     | At every minute past hour 5 on day-of-month 5 and on Friday.|,
| `5 5 5 * 5`     | At 05:05 on day-of-month 5 and on Friday.|
