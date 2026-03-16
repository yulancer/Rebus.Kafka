using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rebus.Kafka;
using Xunit;

namespace Rebus.Kafka.Tests
{
    public class KafkaAdminTestsExt
    {
        [Fact]
        public void Constructor_WithNullBootstrapServers_ThrowsArgumentException()
        {
            var ex = Assert.Throws<ArgumentException>(() => new KafkaAdmin(null));

            Assert.Equal("BootstrapServers it shouldn't be null!", ex.Message);
        }

        [Fact]
        public void Constructor_WithEmptyBootstrapServers_ThrowsArgumentException()
        {
            var ex = Assert.Throws<ArgumentException>(() => new KafkaAdmin(""));

            Assert.Equal("BootstrapServers it shouldn't be null!", ex.Message);
        }

        [Fact]
        public void GetExistingTopics_ReturnsFilteredTopics()
        {
            var adminClient = Substitute.For<IAdminClient>();
            adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(new Metadata(
                new List<BrokerMetadata>(),
                new List<TopicMetadata>
                {
                    CreateTopicMetadata("existing-1", ErrorCode.NoError),
                    CreateTopicMetadata("unknown-1", ErrorCode.UnknownTopicOrPart),
                    CreateTopicMetadata("existing-2", ErrorCode.Local_UnknownTopic)
                },
                0,
                "origin"));

            var sut = new TestKafkaAdmin("localhost:9092", adminClient);

            var result = sut.GetExistingTopics(new[]
            {
                new TopicSpecification { Name = "existing-1", NumPartitions = 1, ReplicationFactor = 1 }
            });

            Assert.Equal(2, result.Count);
            Assert.Contains(result, x => x.Topic == "existing-1");
            Assert.Contains(result, x => x.Topic == "existing-2");
        }

        [Fact]
        public async Task CreateTopicsAsync_WhenAllTopicsAlreadyExist_DoesNotCallCreate()
        {
            var adminClient = Substitute.For<IAdminClient>();
            adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(new Metadata(
                new List<BrokerMetadata>(),
                new List<TopicMetadata>
                {
                    CreateTopicMetadata("topic-1", ErrorCode.NoError),
                    CreateTopicMetadata("topic-2", ErrorCode.NoError)
                },
                0,
                "origin"));

            var sut = new TestKafkaAdmin("localhost:9092", adminClient);

            var result = await sut.CreateTopicsAsync(
                new TopicSpecification { Name = "topic-1", NumPartitions = 1, ReplicationFactor = 1 },
                new TopicSpecification { Name = "topic-2", NumPartitions = 1, ReplicationFactor = 1 });

            Assert.Equal(2, result.Count);
            await adminClient.DidNotReceive()
                .CreateTopicsAsync(Arg.Any<IEnumerable<TopicSpecification>>(), Arg.Any<CreateTopicsOptions>());
        }

        [Fact]
        public async Task CreateTopicsAsync_WhenTopicIsMissing_CreatesOnlyMissingTopic()
        {
            var logger = new TestLogger();
            var adminClient = Substitute.For<IAdminClient>();

            adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(
                new Metadata(
                    new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                    {
                        CreateTopicMetadata("topic-1", ErrorCode.NoError)
                    },
                    0,
                    "origin"),
                new Metadata(
                    new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                    {
                        CreateTopicMetadata("topic-1", ErrorCode.NoError),
                        CreateTopicMetadata("topic-2", ErrorCode.NoError)
                    },
                    0,
                    "origin"));

            adminClient.CreateTopicsAsync(
                Arg.Any<IEnumerable<TopicSpecification>>(),
                Arg.Any<CreateTopicsOptions>())
                .Returns(Task.CompletedTask);

            var sut = new TestKafkaAdmin("localhost:9092", adminClient, logger);

            var result = await sut.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = "topic-1", NumPartitions = 1, ReplicationFactor = 1 },
                new TopicSpecification { Name = "topic-2", NumPartitions = 2, ReplicationFactor = 1 }
            });

            await adminClient.Received(1).CreateTopicsAsync(
                Arg.Is<IEnumerable<TopicSpecification>>(x =>
                    x.Count() == 1 &&
                    x.First().Name == "topic-2" &&
                    x.First().NumPartitions == 2 &&
                    x.First().ReplicationFactor == 1),
                Arg.Is<CreateTopicsOptions>(x => x.ValidateOnly == false));

            Assert.Contains(logger.Entries, x => x.LogLevel == LogLevel.Information);
            Assert.Equal(2, result.Count);
        }

        [Fact]
        public async Task CreateTopicsAsync_WhenTopicStillMissing_ThrowsArgumentException()
        {
            var adminClient = Substitute.For<IAdminClient>();

            adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(
                new Metadata(
                    new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                    {
                        CreateTopicMetadata("topic-1", ErrorCode.NoError)
                    },
                    0,
                    "origin"),
                new Metadata(
                    new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                    {
                        CreateTopicMetadata("topic-1", ErrorCode.NoError)
                    },
                    0,
                    "origin"));

            adminClient.CreateTopicsAsync(
                Arg.Any<IEnumerable<TopicSpecification>>(),
                Arg.Any<CreateTopicsOptions>())
                .Returns(Task.CompletedTask);

            var sut = new TestKafkaAdmin("localhost:9092", adminClient);

            var ex = await Assert.ThrowsAsync<ArgumentException>(() => sut.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = "topic-1", NumPartitions = 1, ReplicationFactor = 1 },
                new TopicSpecification { Name = "topic-2", NumPartitions = 1, ReplicationFactor = 1 }
            }));

            Assert.Equal("newTopicSpecifications", ex.ParamName);
            Assert.Contains("Failed to create topics: \"topic-2\"!", ex.Message);
        }

        [Fact]
        public async Task CreateTopicsAsync_WhenCreateThrows_LogsErrorAndRethrows()
        {
            var logger = new TestLogger();
            var adminClient = Substitute.For<IAdminClient>();

            adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(new Metadata(
                new List<BrokerMetadata>(),
                new List<TopicMetadata>(),
                0,
                "origin"));

            var exception = CreateCreateTopicsException("topic-1", "boom");

            adminClient.CreateTopicsAsync(
                Arg.Any<IEnumerable<TopicSpecification>>(),
                Arg.Any<CreateTopicsOptions>())
                .Returns<Task>(_ => throw exception);

            var sut = new TestKafkaAdmin("localhost:9092", adminClient, logger);

            var ex = await Assert.ThrowsAsync<CreateTopicsException>(() => sut.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = "topic-1", NumPartitions = 1, ReplicationFactor = 1 }
            }));

            Assert.Same(exception, ex);
            Assert.Contains(logger.Entries, x => x.LogLevel == LogLevel.Error);
        }

        [Fact]
        public async Task DeleteTopicsAsync_WhenTopicDoesNotExist_DoesNotCallDelete()
        {
            var adminClient = Substitute.For<IAdminClient>();
            adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(new Metadata(
                new List<BrokerMetadata>(),
                new List<TopicMetadata>
                {
                    CreateTopicMetadata("topic-1", ErrorCode.NoError)
                },
                0,
                "origin"));

            var sut = new TestKafkaAdmin("localhost:9092", adminClient);

            var result = await sut.DeleteTopicsAsync(new[] { "topic-2" });

            Assert.Single(result);
            Assert.Equal("topic-1", result[0].Topic);

            await adminClient.DidNotReceive()
                .DeleteTopicsAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<DeleteTopicsOptions>());
        }

        [Fact]
        public async Task DeleteTopicsAsync_WhenTopicExists_DeletesIt()
        {
            var logger = new TestLogger();
            var adminClient = Substitute.For<IAdminClient>();

            adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(
                new Metadata(
                    new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                    {
                        CreateTopicMetadata("topic-1", ErrorCode.NoError),
                        CreateTopicMetadata("topic-2", ErrorCode.NoError)
                    },
                    0,
                    "origin"),
                new Metadata(
                    new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                    {
                        CreateTopicMetadata("topic-1", ErrorCode.NoError)
                    },
                    0,
                    "origin"));

            adminClient.DeleteTopicsAsync(
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<DeleteTopicsOptions>())
                .Returns(Task.CompletedTask);

            var sut = new TestKafkaAdmin("localhost:9092", adminClient, logger);
            var options = new DeleteTopicsOptions();

            var result = await sut.DeleteTopicsAsync(new[] { "topic-2" }, options);

            await adminClient.Received(1).DeleteTopicsAsync(
                Arg.Is<IEnumerable<string>>(x => x.Single() == "topic-2"),
                Arg.Is<DeleteTopicsOptions>(x => ReferenceEquals(x, options)));

            Assert.Contains(logger.Entries, x => x.LogLevel == LogLevel.Warning);
            Assert.Single(result);
            Assert.Equal("topic-1", result[0].Topic);
        }

        [Fact]
        public async Task DeleteTopicsAsync_WhenTopicStillExists_ThrowsArgumentException()
        {
            var adminClient = Substitute.For<IAdminClient>();

            adminClient.GetMetadata(Arg.Any<TimeSpan>()).Returns(
                new Metadata(
                    new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                    {
                        CreateTopicMetadata("topic-1", ErrorCode.NoError),
                        CreateTopicMetadata("topic-2", ErrorCode.NoError)
                    },
                    0,
                    "origin"),
                new Metadata(
                    new List<BrokerMetadata>(),
                    new List<TopicMetadata>
                    {
                        CreateTopicMetadata("topic-1", ErrorCode.NoError),
                        CreateTopicMetadata("topic-2", ErrorCode.NoError)
                    },
                    0,
                    "origin"));

            adminClient.DeleteTopicsAsync(
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<DeleteTopicsOptions>())
                .Returns(Task.CompletedTask);

            var sut = new TestKafkaAdmin("localhost:9092", adminClient);

            var ex = await Assert.ThrowsAsync<ArgumentException>(() => sut.DeleteTopicsAsync(new[] { "topic-2" }));

            Assert.Equal("deletedTopicSpecifications", ex.ParamName);
            Assert.Contains("Failed to delete topics: \"topic-2\"!", ex.Message);
        }

        private static TopicMetadata CreateTopicMetadata(string topic, ErrorCode errorCode)
        {
            return new TopicMetadata(topic, new List<PartitionMetadata>(),
                new Error(errorCode));
        }

        private static CreateTopicsException CreateCreateTopicsException(string topic, string reason)
        {
            return new CreateTopicsException(new List<CreateTopicReport>
            {
                new CreateTopicReport
                {
                    Topic = topic,
                    Error = new Error(ErrorCode.Local_Fail, reason)
                }
            });
        }

        private sealed class TestKafkaAdmin : KafkaAdmin
        {
            private readonly IAdminClient _adminClient;

            public TestKafkaAdmin(string bootstrapServers, IAdminClient adminClient, ILogger log = null)
                : base(bootstrapServers, log)
            {
                _adminClient = adminClient;
            }

            internal override IAdminClient CreateAdminClient() => _adminClient;
        }

        private sealed class TestLogger : ILogger
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