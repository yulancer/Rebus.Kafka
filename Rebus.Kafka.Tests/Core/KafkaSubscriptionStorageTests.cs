using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rebus.Exceptions;
using Rebus.Kafka.Configs;
using Rebus.Kafka.Core;
using Rebus.Kafka.Dispatcher;
using Rebus.Kafka.SchemaRegistry;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Transport;
using Xunit;

namespace Rebus.Kafka.Tests.Core
{
    public class KafkaSubscriptionStorageTests
    {
        [Fact]
        public async Task GetSubscriberAddresses_ReturnsMagicSubscriptionTopic_WithInvalidCharactersReplaced()
        {
            var sut = CreateUninitializedSut();

            var result = await sut.GetSubscriberAddresses("orders/new topic#1");

            Assert.Single(result);
            Assert.Equal($"{Constants.MagicSubscriptionPrefix}orders_new_topic_1", result[0]);
        }

        [Fact]
        public void IsCentralized_ReturnsTrue()
        {
            var loggerFactory = Substitute.For<IRebusLoggerFactory>();
            var asyncTaskFactory = Substitute.For<IAsyncTaskFactory>();
            var sut = new KafkaSubscriptionStorage(loggerFactory, asyncTaskFactory, "localhost:9092", "input-queue", "group-1", default);

            Assert.True(sut.IsCentralized);
        }

        [Fact]
        public void IsInitialized_WhenInitializationTaskIsNull_ReturnsFalse()
        {
            var sut = CreateUninitializedSut();

            Assert.False(GetPrivateProperty<bool>(sut, "IsInitialized"));
        }

        [Fact]
        public void IsInitialized_WhenInitializationTaskCompleted_ReturnsTrue()
        {
            var sut = CreateUninitializedSut();
            SetPrivateField(sut, "_initializationTask", Task.CompletedTask);

            Assert.True(GetPrivateProperty<bool>(sut, "IsInitialized"));
        }

        [Fact]
        public void CreateTopics_WhenBootstrapServersMissing_ThrowsArgumentException()
        {
            var sut = CreateUninitializedSut();
            SetPrivateField(sut, "_config", new ConsumerConfig { BootstrapServers = null });

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod(sut, "CreateTopics", new object[] { new[] { "topic-1" } }));

            var inner = Assert.IsType<ArgumentException>(ex.InnerException);
            Assert.Equal("BootstrapServers it shouldn't be null!", inner.Message);
        }

        [Fact]
        public void ConsumerSubscribe_WithNullTopics_DoesNothing()
        {
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            var sut = CreateUninitializedSut(consumer: consumer);

            var ex = Record.Exception(() =>
                InvokePrivateMethod(sut, "ConsumerSubscribe", new object[] { null }));

            Assert.Null(ex);
            consumer.DidNotReceive().Subscribe(Arg.Any<IEnumerable<string>>());
        }

        [Fact]
        public void ConsumerSubscribe_WithEmptyTopics_DoesNothing()
        {
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            var sut = CreateUninitializedSut(consumer: consumer);

            var ex = Record.Exception(() =>
                InvokePrivateMethod(sut, "ConsumerSubscribe", new object[] { Array.Empty<string>() }));

            Assert.Null(ex);
            consumer.DidNotReceive().Subscribe(Arg.Any<IEnumerable<string>>());
        }

        [Fact]
        public async Task UnregisterSubscriber_WhenLastTopicRemoved_ClosesConsumer_AndTaskCompletesOnPartitionsRevoked()
        {
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            var sut = CreateUninitializedSut(consumer: consumer);

            var subscriptions = GetPrivateField<ConcurrentDictionary<string, string[]>>(sut, "_subscriptions");
            subscriptions["input-queue"] = new[] { "input-queue" };

            var task = sut.UnregisterSubscriber("input-queue", "whatever");

            consumer.Received(1).Close();
            Assert.False(task.IsCompleted);

            InvokePrivateMethod(
                sut,
                "ConsumerOnPartitionsRevokedHandler",
                consumer,
                new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset("input-queue", new Partition(0), new Offset(10))
                });

            await task;
            Assert.True(task.IsCompletedSuccessfully);
        }

        [Fact]
        public void ConsumerOnLogHandler_IgnoresEmptyReadMessage()
        {
            var log = new TestLog();
            var sut = CreateUninitializedSut(log: log);

            InvokePrivateMethod(
                sut,
                "ConsumerOnLogHandler",
                Substitute.For<IConsumer<string, byte[]>>(),
                new LogMessage("some-name", SyslogLevel.Debug, "some-facility", "MessageSet size 0, error \"Success\""));

            Assert.Empty(log.Entries);
        }

        [Fact]
        public void ConsumerOnLogHandler_LogsRegularMessage()
        {
            var log = new TestLog();
            var sut = CreateUninitializedSut(log: log);

            InvokePrivateMethod(
                sut,
                "ConsumerOnLogHandler",
                Substitute.For<IConsumer<string, byte[]>>(),
                new LogMessage("some-name", SyslogLevel.Debug, "some-facility", "some-message"));

            Assert.Contains(log.Entries, x => x.Level == "Debug" && x.Message.Contains("Consuming from Kafka"));
        }

        [Fact]
        public void ConsumerOnStatisticsHandler_LogsInfo()
        {
            var log = new TestLog();
            var sut = CreateUninitializedSut(log: log);

            InvokePrivateMethod(
                sut,
                "ConsumerOnStatisticsHandler",
                Substitute.For<IConsumer<string, byte[]>>(),
                "{\"stats\":1}");

            Assert.Contains(log.Entries, x => x.Level == "Info" && x.Message.Contains("Consumer statistics"));
        }

        [Fact]
        public void ConsumerOnErrorHandler_WithNonFatalError_LogsWarning()
        {
            var log = new TestLog();
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            var sut = CreateUninitializedSut(consumer: consumer, log: log);

            InvokePrivateMethod(
                sut,
                "ConsumerOnErrorHandler",
                consumer,
                new Error(ErrorCode.Local_BadMsg, "bad message"));

            Assert.Contains(log.Entries, x => x.Level == "Warn" && x.Message.Contains("Consumer error"));
        }

        [Fact]
        public void ConsumerOnErrorHandler_WithFatalError_ThrowsKafkaException_AndLogsError()
        {
            var log = new TestLog();
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            consumer.Assignment.Returns(new List<TopicPartition>
            {
                new TopicPartition("topic-1", new Partition(0))
            });
            consumer.Position(Arg.Any<TopicPartition>()).Returns(new Offset(42));

            var sut = CreateUninitializedSut(consumer: consumer, log: log);

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod(
                    sut,
                    "ConsumerOnErrorHandler",
                    consumer,
                    new Error(ErrorCode.Local_Fatal, "fatal error")));

            Assert.IsType<KafkaException>(ex.InnerException);
            Assert.Contains(log.Entries, x => x.Level == "Error" && x.Message.Contains("Fatal error consuming from Kafka"));
        }

        [Fact]
        public async Task ConsumerOnPartitionsAssignedHandler_CompletesWaitingSubscriptionTask()
        {
            var log = new TestLog();
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            var sut = CreateUninitializedSut(consumer: consumer, log: log);

            var waitAssigned = GetWaitDictionary(sut, "_waitAssigned");
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            AddWaitEntry(waitAssigned, new[] { "topic-1" }, "topic-1", tcs);

            InvokePrivateMethod(
                sut,
                "ConsumerOnPartitionsAssignedHandler",
                consumer,
                new List<TopicPartition>
                {
                    new TopicPartition("topic-1", new Partition(0))
                });

            await tcs.Task;
            Assert.Contains(log.Entries, x => x.Level == "Info" && x.Message.Contains("Subscribe on"));
        }

        [Fact]
        public async Task ConsumerOnPartitionsRevokedHandler_CompletesWaitingUnsubscribeTask()
        {
            var log = new TestLog();
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            var sut = CreateUninitializedSut(consumer: consumer, log: log);

            var waitRevoked = GetWaitDictionary(sut, "_waitRevoked");
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            AddWaitEntry(waitRevoked, new[] { "topic-1" }, "topic-1", tcs);

            InvokePrivateMethod(
                sut,
                "ConsumerOnPartitionsRevokedHandler",
                consumer,
                new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset("topic-1", new Partition(0), new Offset(5))
                });

            await tcs.Task;
            Assert.Contains(log.Entries, x => x.Level == "Info" && x.Message.Contains("Unsubscribe from"));
        }

        [Fact]
        public void ConsumerOnPartitionsLostHandler_LogsWarning()
        {
            var log = new TestLog();
            var sut = CreateUninitializedSut(log: log);

            InvokePrivateMethod(
                sut,
                "ConsumerOnPartitionsLostHandler",
                Substitute.For<IConsumer<string, byte[]>>(),
                new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset("topic-1", new Partition(0), new Offset(9))
                });

            Assert.Contains(log.Entries, x => x.Level == "Warn" && x.Message.Contains("Partitions lost"));
        }

        [Fact]
        public void ConsumerOnOffsetsCommittedHandler_LogsDebug()
        {
            var log = new TestLog();
            var sut = CreateUninitializedSut(log: log);

            var committed = new CommittedOffsets(
                new List<TopicPartitionOffsetError>
                {
                    new TopicPartitionOffsetError(
                        new TopicPartition("topic-1", new Partition(0)),
                        new Offset(12),
                        new Error(ErrorCode.NoError))
                },
                new Error(ErrorCode.NoError));

            InvokePrivateMethod(
                sut,
                "ConsumerOnOffsetsCommittedHandler",
                Substitute.For<IConsumer<string, byte[]>>(),
                committed);

            Assert.Contains(log.Entries, x => x.Level == "Debug" && x.Message.Contains("Offsets committed"));
        }

        [Fact]
        public void CommitIncrementedOffset_WhenCommitSucceeds_ReturnsOk()
        {
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            var sut = CreateUninitializedSut(consumer: consumer);

            var result = InvokePrivateMethod(
                sut,
                "CommitIncrementedOffset",
                new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset("topic-1", new Partition(0), new Offset(10)),
                    new TopicPartitionOffset("topic-2", new Partition(1), new Offset(20))
                });

            Assert.True(GetResultSuccess(result));
            consumer.Received(1).Commit(Arg.Is<IEnumerable<TopicPartitionOffset>>(x =>
                x.Count() == 2
                && x.ElementAt(0).Topic == "topic-1"
                && x.ElementAt(0).Offset.Value == 11
                && x.ElementAt(1).Topic == "topic-2"
                && x.ElementAt(1).Offset.Value == 21));
        }

        [Fact]
        public void CommitIncrementedOffset_WhenCommitThrows_ReturnsFailedResult()
        {
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            consumer.When(x => x.Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>()))
                .Do(_ => throw new InvalidOperationException("commit failed"));

            var sut = CreateUninitializedSut(consumer: consumer);

            var result = InvokePrivateMethod(
                sut,
                "CommitIncrementedOffset",
                new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset("topic-1", new Partition(0), new Offset(10))
                });

            Assert.True(GetResultFailure(result));
            Assert.Contains("commit failed", GetResultReason(result));
        }

        [Fact]
        public void FirstConstructor_WithNullBrokerList_ThrowsNullReferenceException()
        {
            var loggerFactory = Substitute.For<IRebusLoggerFactory>();
            var asyncTaskFactory = Substitute.For<IAsyncTaskFactory>();

            var ex = Assert.Throws<NullReferenceException>(() =>
            {
                new KafkaSubscriptionStorage(
                    loggerFactory,
                    asyncTaskFactory,
                    (string)null,
                    "input-queue",
                    (string)null,
                    default(CancellationToken));
            });

            Assert.Equal("brokerList", ex.Message);
        }

        [Fact]
        public void FirstConstructor_WithMagicSubscriptionPrefix_ThrowsArgumentException()
        {
            var loggerFactory = Substitute.For<IRebusLoggerFactory>();
            loggerFactory.GetLogger<KafkaSubscriptionStorage>().Returns(new TestLog());
            var asyncTaskFactory = Substitute.For<IAsyncTaskFactory>();

            var ex = Assert.Throws<ArgumentException>(() =>
                new KafkaSubscriptionStorage(
                    loggerFactory,
                    asyncTaskFactory, "some-broker-list", $"{Constants.MagicSubscriptionPrefix}some-input-queue-name", "some-group-id",
                    default));

            Assert.Contains("magic subscription prefix", ex.Message);
        }

        [Fact]
        public void SecondConstructor_WithNullConfig_ThrowsNullReferenceException()
        {
            var loggerFactory = Substitute.For<IRebusLoggerFactory>();
            loggerFactory.GetLogger<KafkaSubscriptionStorage>().Returns(new TestLog());
            var asyncTaskFactory = Substitute.For<IAsyncTaskFactory>();

            var ex = Assert.Throws<NullReferenceException>(() =>
                new KafkaSubscriptionStorage(
                    loggerFactory,
                    asyncTaskFactory,
                    "localhost:9092",
                    "input-queue",
                    (ConsumerConfig)null,
                    default));

            Assert.Equal("config", ex.Message);
        }

        [Fact]
        public void SecondConstructor_WithEnableAutoCommitTrue_LogsWarning_AndGeneratesGroupId()
        {
            var log = new TestLog();
            var loggerFactory = Substitute.For<IRebusLoggerFactory>();
            loggerFactory.GetLogger<KafkaSubscriptionStorage>().Returns(log);
            var asyncTaskFactory = Substitute.For<IAsyncTaskFactory>();

            var sut = new KafkaSubscriptionStorage(
                loggerFactory,
                asyncTaskFactory,
                "localhost:9092",
                "input-queue",
                new ConsumerConfig
                {
                    EnableAutoCommit = true,
                    GroupId = null
                },
                default);

            var config = GetPrivateField<ConsumerConfig>(sut, "_config");

            Assert.True(log.Entries.Any(x => x.Level == "Warn" && x.Message.Contains("EnableAutoCommit")));
            Assert.False(string.IsNullOrWhiteSpace(config.GroupId));
        }

        [Fact]
        public void ThirdConstructor_StoresBehaviorConfig()
        {
            var log = new TestLog();
            var loggerFactory = Substitute.For<IRebusLoggerFactory>();
            loggerFactory.GetLogger<KafkaSubscriptionStorage>().Returns(log);
            var asyncTaskFactory = Substitute.For<IAsyncTaskFactory>();

            var behaviorConfig = new ConsumerBehaviorConfig
            {
                CommitPeriod = 7
            };

            var sut = new KafkaSubscriptionStorage(
                loggerFactory,
                asyncTaskFactory,
                "localhost:9092",
                "input-queue",
                new ConsumerAndBehaviorConfig
                {
                    GroupId = "group-1",
                    BehaviorConfig = behaviorConfig
                },
                default);

            var storedBehavior = GetPrivateField<ConsumerBehaviorConfig>(sut, "_behaviorConfig");

            Assert.Same(behaviorConfig, storedBehavior);
        }

        private static KafkaSubscriptionStorage CreateUninitializedSut(
            IConsumer<string, byte[]> consumer = null,
            TestLog log = null)
        {
            var sut = (KafkaSubscriptionStorage)FormatterServices.GetUninitializedObject(typeof(KafkaSubscriptionStorage));

            SetPrivateField(sut, "_consumer", consumer ?? Substitute.For<IConsumer<string, byte[]>>());
            SetPrivateField(sut, "_config", new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "group-1",
                AllowAutoCreateTopics = false
            });
            SetPrivateField(sut, "_behaviorConfig", new ConsumerBehaviorConfig());
            SetPrivateField(sut, "_subscriptions", new ConcurrentDictionary<string, string[]>());
            SetPrivateField(
                sut,
                "_waitAssigned",
                CreateWaitDictionaryInstance(GetWaitDictionaryFieldType("_waitAssigned")));
            SetPrivateField(
                sut,
                "_waitRevoked",
                CreateWaitDictionaryInstance(GetWaitDictionaryFieldType("_waitRevoked")));
            SetPrivateField(sut, "_topicRegex", new System.Text.RegularExpressions.Regex("[^a-zA-Z0-9\\._\\-]+"));
            SetPrivateField(sut, "_log", log ?? new TestLog());
            SetPrivateField(sut, "_rebusLoggerFactory", Substitute.For<IRebusLoggerFactory>());
            SetPrivateField(sut, "_asyncTaskFactory", Substitute.For<IAsyncTaskFactory>());

            return sut;
        }

        private static object CreateViaCtor(Type dummy, params object[] args)
        {
            return Activator.CreateInstance(typeof(KafkaSubscriptionStorage), args);
        }

        private static object InvokePrivateMethod(object instance, string methodName, params object[] args)
        {
            var method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(method);
            return method!.Invoke(instance, args);
        }

        private static void SetPrivateField(object instance, string fieldName, object value)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            field!.SetValue(instance, value);
        }

        private static T GetPrivateField<T>(object instance, string fieldName)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return (T)field!.GetValue(instance)!;
        }

        private static T GetPrivateProperty<T>(object instance, string propertyName)
        {
            var property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(property);
            return (T)property!.GetValue(instance)!;
        }

        private static Type GetWaitDictionaryFieldType(string fieldName)
        {
            var field = typeof(KafkaSubscriptionStorage).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return field!.FieldType;
        }

        private static object CreateWaitDictionaryInstance(Type type)
        {
            return Activator.CreateInstance(type)!;
        }

        private static IDictionary GetWaitDictionary(object instance, string fieldName)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return (IDictionary)field!.GetValue(instance)!;
        }

        private static void AddWaitEntry(IDictionary dictionary, IEnumerable<string> topics, string topic, TaskCompletionSource<bool> tcs)
        {
            var pairType = dictionary.GetType().GenericTypeArguments[1];
            var pair = Activator.CreateInstance(pairType, topic, tcs)!;
            dictionary.Add(topics.ToArray(), pair);
        }

        private static bool GetResultSuccess(object result) =>
            (bool)result.GetType().GetProperty("Success", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(result)!;

        private static bool GetResultFailure(object result) =>
            (bool)result.GetType().GetProperty("Failure", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(result)!;

        private static string GetResultReason(object result) =>
            (string)result.GetType().GetProperty("Reason", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(result)!;

        private sealed class TestLog : ILog
        {
            public List<LogEntry> Entries { get; } = new();

            public void Debug(string message, params object[] objs) => Entries.Add(new LogEntry("Debug", Format(message, objs)));
            public void Info(string message, params object[] objs) => Entries.Add(new LogEntry("Info", Format(message, objs)));
            public void Warn(string message, params object[] objs) => Entries.Add(new LogEntry("Warn", Format(message, objs)));
            public void Warn(Exception exception, string message, params object[] objs)
            {
                Warn(message, objs);
            }

            public void Error(string message, params object[] objs) => Entries.Add(new LogEntry("Error", Format(message, objs)));
            public void Error(Exception exception, string message, params object[] objs)
            {
                Error(message, objs);
            }

            private static string Format(string message, object[] objs) => objs == null || objs.Length == 0 ? message : string.Format(message.Replace("{", "{0:"), objs);

            public sealed class LogEntry
            {
                public LogEntry(string level, string message)
                {
                    Level = level;
                    Message = message;
                }

                public string Level { get; }
                public string Message { get; }
            }
        }
    }
}