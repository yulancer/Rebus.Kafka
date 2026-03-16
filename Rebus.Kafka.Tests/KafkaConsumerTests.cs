using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rebus.Kafka;
using Rebus.Kafka.Configs;
using Xunit;

namespace Rebus.Kafka.Tests
{
    public class KafkaConsumerTests
    {
        [Fact]
        public void Consume_WithNullTopics_ThrowsArgumentNullException()
        {
            var sut = CreateSut<string, string>();

            Assert.Throws<ArgumentNullException>(() => sut.Consume((IEnumerable<string>)null));
        }

        [Fact]
        public void Consume_WithEmptyTopics_ThrowsArgumentNullException()
        {
            var sut = CreateSut<string, string>();

            Assert.Throws<ArgumentNullException>(() => sut.Consume(Array.Empty<string>()));
        }

        [Fact]
        public void Consume_WithTopics_SubscribesAndReturnsObservable()
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            var sut = CreateSut<string, string>(consumer: consumer);

            var observable = sut.Consume("topic-1", "topic-2");

            Assert.NotNull(observable);
            consumer.Received(1).Subscribe(Arg.Is<IEnumerable<string>>(x => x.SequenceEqual(new[] { "topic-1", "topic-2" })));
        }

        [Fact]
        public void Consume_WithTopicPartitionOffsets_AssignsAndReturnsObservable()
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            var sut = CreateSut<string, string>(consumer: consumer);

            var tpos = new[]
            {
                new TopicPartitionOffset("topic-1", new Partition(0), new Offset(10)),
                new TopicPartitionOffset("topic-2", new Partition(1), new Offset(20))
            };

            var observable = sut.Consume(tpos);

            Assert.NotNull(observable);
            consumer.Received(1).Assign(Arg.Is<IEnumerable<TopicPartitionOffset>>(x => x.SequenceEqual(tpos)));
        }

        [Fact]
        public void Consume_WithEmptyTopicPartitionOffsets_ThrowsArgumentNullException()
        {
            var sut = CreateSut<string, string>();

            Assert.Throws<ArgumentNullException>(() => sut.Consume(Array.Empty<TopicPartitionOffset>()));
        }

        [Fact]
        public void Commit_ReturnsConsumerCommitResult()
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            var expected = new List<TopicPartitionOffset>
            {
                new TopicPartitionOffset("topic-1", new Partition(0), new Offset(11))
            };

            consumer.Commit().Returns(expected);

            var sut = CreateSut<string, string>(consumer: consumer);

            var result = sut.Commit();

            Assert.Same(expected, result);
            consumer.Received(1).Commit();
        }

        [Fact]
        public void CommitIncrementedOffset_CommitsOffsetsIncrementedByOne()
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            var sut = CreateSut<string, string>(consumer: consumer);

            var source = new[]
            {
                new TopicPartitionOffset("topic-1", new Partition(0), new Offset(10)),
                new TopicPartitionOffset("topic-2", new Partition(1), new Offset(20))
            };

            sut.CommitIncrementedOffset(source);

            consumer.Received(1).Commit(Arg.Is<IEnumerable<TopicPartitionOffset>>(x =>
                x.Count() == 2
                    && x.ElementAt(0).Topic == "topic-1"
                    && x.ElementAt(0).Partition.Value == 0
                    && x.ElementAt(0).Offset.Value == 11
                    && x.ElementAt(1).Topic == "topic-2"
                    && x.ElementAt(1).Partition.Value == 1
                    && x.ElementAt(1).Offset.Value == 21));
        }

        [Fact]
        public void Dispose_DisposesUnderlyingConsumer()
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            var sut = CreateSut<string, string>(consumer: consumer);

            sut.Dispose();

            consumer.Received(1).Dispose();
        }

        [Fact]
        public void Consumer_ReturnsUnderlyingConsumer()
        {
            var consumer = Substitute.For<IConsumer<string, string>>();
            var sut = CreateSut<string, string>(consumer: consumer);

            Assert.Same(consumer, sut.Consumer);
        }

        [Fact]
        public void OnLog_IgnoresEmptyReadMessage()
        {
            var logger = new TestLogger<KafkaConsumer<string, string>>();
            var sut = CreateSut<string, string>(logger: logger);

            InvokePrivateMethod(
                sut,
                "OnLog",
                new object(),
                new LogMessage("test", SyslogLevel.Debug, "test", "MessageSet size 0, error \"Success\""));

            Assert.Empty(logger.Entries);
        }

        [Fact]
        public void OnLog_LogsRegularMessage()
        {
            var logger = new TestLogger<KafkaConsumer<string, string>>();
            var sut = CreateSut<string, string>(logger: logger);

            InvokePrivateMethod(
                sut,
                "OnLog",
                new object(),
                new LogMessage("client-1", SyslogLevel.Debug, "client-1", "some log message"));

            Assert.Contains(logger.Entries, x =>
                x.LogLevel == LogLevel.Debug &&
                x.Message.Contains("Consuming from Kafka."));
        }

        [Fact]
        public void OnError_WithNonFatalError_LogsWarning()
        {
            var logger = new TestLogger<KafkaConsumer<string, string>>();
            var consumer = Substitute.For<IConsumer<string, string>>();
            var sut = CreateSut<string, string>(consumer: consumer, logger: logger);

            InvokePrivateMethod(
                sut,
                "OnError",
                consumer,
                new Error(ErrorCode.Local_BadMsg, "bad message"));

            Assert.Contains(logger.Entries, x =>
                x.LogLevel == LogLevel.Warning &&
                x.Message.Contains("Consumer error:"));
        }

        [Fact]
        public void OnError_WithFatalError_ThrowsKafkaExceptionAndLogsError()
        {
            var logger = new TestLogger<KafkaConsumer<string, string>>();
            var consumer = Substitute.For<IConsumer<string, string>>();
            consumer.Assignment.Returns(new List<TopicPartition>
            {
                new TopicPartition("topic-1", new Partition(0))
            });
            consumer.Position(Arg.Any<TopicPartition>()).Returns(new Offset(123));

            var sut = CreateSut<string, string>(consumer: consumer, logger: logger);

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod(
                    sut,
                    "OnError",
                    consumer,
                    new Error(ErrorCode.Local_Fatal, "fatal error")));

            Assert.IsType<KafkaException>(ex.InnerException);
            Assert.Contains(logger.Entries, x =>
                x.LogLevel == LogLevel.Error &&
                x.Message.Contains("Fatal error consuming from Kafka."));
        }

        [Fact]
        public void ConsumerOnPartitionsAssigned_RaisesEventAndLogsInformation()
        {
            var logger = new TestLogger<KafkaConsumer<string, string>>();
            var consumer = Substitute.For<IConsumer<string, string>>();
            var sut = CreateSut<string, string>(consumer: consumer, logger: logger);

            List<TopicPartition> eventPartitions = null;
            sut.PartitionsAssigned += (_, partitions) => eventPartitions = partitions;

            var assigned = new List<TopicPartition>
            {
                new TopicPartition("topic-1", new Partition(0))
            };

            InvokePrivateMethod(sut, "ConsumerOnPartitionsAssigned", consumer, assigned);

            Assert.Same(assigned, eventPartitions);
            Assert.Contains(logger.Entries, x =>
                x.LogLevel == LogLevel.Information &&
                x.Message.Contains("Assigned partitions:"));
        }

        [Fact]
        public void ConsumerOnPartitionsRevoked_LogsInformation()
        {
            var logger = new TestLogger<KafkaConsumer<string, string>>();
            var consumer = Substitute.For<IConsumer<string, string>>();
            var sut = CreateSut<string, string>(consumer: consumer, logger: logger);

            var revoked = new List<TopicPartitionOffset>
            {
                new TopicPartitionOffset("topic-1", new Partition(0), new Offset(15))
            };

            InvokePrivateMethod(sut, "ConsumerOnPartitionsRevoked", consumer, revoked);

            Assert.Contains(logger.Entries, x =>
                x.LogLevel == LogLevel.Information &&
                x.Message.Contains("Revoked partitions:"));
        }

        [Theory]
        [InlineData(typeof(Null))]
        [InlineData(typeof(Ignore))]
        [InlineData(typeof(byte[]))]
        [InlineData(typeof(string))]
        [InlineData(typeof(long))]
        [InlineData(typeof(int))]
        [InlineData(typeof(double))]
        [InlineData(typeof(float))]
        public void GetDeserializerFor_SupportedTypes_ReturnsDeserializer(Type type)
        {
            var sut = CreateSut<string, string>();

            var result = InvokePrivateGenericMethod(sut, "GetDeserializerFor", type);

            Assert.NotNull(result);
        }

        [Fact]
        public void GetDeserializerFor_UnsupportedType_ThrowsNotSupportedException()
        {
            var sut = CreateSut<string, string>();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateGenericMethod(sut, "GetDeserializerFor", typeof(DateTime)));

            Assert.IsType<NotSupportedException>(ex.InnerException);
            Assert.Equal("T", ex.InnerException.Message);
        }

        [Fact]
        public void Constructor_WithNullBrokerList_ThrowsNullReferenceException()
        {
            var ex = Assert.Throws<NullReferenceException>(() => new KafkaConsumer<string, string>((string)null, (string)null));

            Assert.Equal("brokerList", ex.Message);
        }

        [Fact]
        public void Constructor_WithBlankBrokerList_ThrowsNullReferenceException()
        {
            var ex = Assert.Throws<NullReferenceException>(() => new KafkaConsumer<string, string>(" ", (string)null));

            Assert.Equal("brokerList", ex.Message);
        }

        [Fact]
        public void Constructor_WithNullBootstrapServersInConfig_ThrowsNullReferenceException()
        {
            var ex = Assert.Throws<NullReferenceException>(() =>
                new KafkaConsumer<string, string>(new ConsumerConfig()));

            Assert.Equal("consumerConfig.BootstrapServers", ex.Message);
        }

        [Fact]
        public void Constructor_WithEmptyGroupIdInConfig_GeneratesGroupId()
        {
            var sut = new KafkaConsumer<string, string>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = ""
            });

            var config = GetPrivateField<ConsumerConfig>(sut, "_config");

            Assert.False(string.IsNullOrWhiteSpace(config.GroupId));

            sut.Dispose();
        }

        private static KafkaConsumer<TKey, TValue> CreateSut<TKey, TValue>(
            IConsumer<TKey, TValue> consumer = null,
            ILogger<KafkaConsumer<TKey, TValue>> logger = null,
            ConsumerConfig config = null,
            ConsumerBehaviorConfig behaviorConfig = null)
        {
            var instance = (KafkaConsumer<TKey, TValue>)FormatterServices.GetUninitializedObject(typeof(KafkaConsumer<TKey, TValue>));

            SetPrivateField(instance, "_consumer", consumer ?? Substitute.For<IConsumer<TKey, TValue>>());
            SetPrivateField(instance, "_logger", logger);
            SetPrivateField(instance, "_config", config ?? new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "group-1",
                AllowAutoCreateTopics = false
            });
            SetPrivateField(instance, "_behaviorConfig", behaviorConfig ?? new ConsumerBehaviorConfig
            {
                CommitPeriod = 10
            });

            return instance;
        }

        private static void SetPrivateField(object instance, string fieldName, object value)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            field.SetValue(instance, value);
        }

        private static T GetPrivateField<T>(object instance, string fieldName)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return (T)field.GetValue(instance);
        }

        private static object InvokePrivateMethod(object instance, string methodName, params object[] args)
        {
            var method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(method);
            return method.Invoke(instance, args);
        }

        private static object InvokePrivateGenericMethod(object instance, string methodName, Type genericArgument, params object[] args)
        {
            var method = instance.GetType()
                .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
                .Single(x => x.Name == methodName && x.IsGenericMethodDefinition);

            return method.MakeGenericMethod(genericArgument).Invoke(instance, args);
        }

        private sealed class TestLogger<T> : ILogger<T>
        {
            public List<LogEntry> Entries { get; } = new();

            public IDisposable BeginScope<TState>(TState state) => NullScope.Instance;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter)
            {
                Entries.Add(new LogEntry
                {
                    LogLevel = logLevel,
                    Message = formatter(state, exception),
                    Exception = exception
                });
            }
        }

        private sealed class LogEntry
        {
            public LogLevel LogLevel { get; set; }
            public string Message { get; set; }
            public Exception Exception { get; set; }
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}