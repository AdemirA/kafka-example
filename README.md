# Kafka Example 

Kafka example using dotnet core. 

## Settings

Each project has a `appsettings.json` file.

```
{
  "BootstrapServers": "localhost:9092",
  "TopicName": "topic-01"
}
```

In the consumer project you can set the `GroupId`. Enables to test different groups consuming the same messages.
