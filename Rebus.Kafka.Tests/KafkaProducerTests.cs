using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Rebus.Kafka;
using Xunit;

namespace Rebus.Kafka.Tests
{
    public class KafkaProducerTests
    {
        [Fact]
        public async Task ProduceAsync_ReturnsDeliveryResult()
        {
            var producer = Substitute.For<IProducer<string, string>>();
            var expected = new DeliveryResult<string, string>
            {
                Topic = "topic-1",
                Partition = new Partition(2),
                Offset = new Offset(15),
                Message = new Message<string, string> { Key = "k1", Value = "v1" }
            };

            producer
                .ProduceAsync("topic-1", Arg.Any<Message<string, string>>())
                .Returns(Task.FromResult(expected));

            var sut = CreateSut<string, string>(producer: producer);

            var message = new Message<string, string> { Key = "k1", Value = "v1" };

            var result = await sut.ProduceAsync("topic-1", message);

            Assert.Same(expected, result);
            await producer.Received(1).ProduceAsync("topic-1", message);
        }

        [Fact]
        public async Task ProduceAsync_WhenProducerThrows_LogsErrorAndRethrows()
        {
            var producer = Substitute.For<IProducer<string, string>>();
            var logger = new TestLogger<KafkaProducer<string, string>>();
            var exception = new InvalidOperationException("produce failed");

            producer
                .ProduceAsync("topic-1", Arg.Any<Message<string, string>>())
                .Returns<Task<DeliveryResult<string, string>>>(_ => throw exception);

            var sut = CreateSut<string, string>(producer: producer, logger: logger);

            var message = new Message<string, string> { Key = "k1", Value = "v1" };

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.ProduceAsync("topic-1", message));

            Assert.Same(exception, ex);
            Assert.Contains(logger.Entries, x =>
                x.LogLevel == LogLevel.Error &&
                x.Exception == exception &&
                x.Message.Contains("Error producing to Kafka."));
        }

        [Fact]
        public void Dispose_FlushesAndDisposesUnderlyingProducer()
        {
            var producer = Substitute.For<IProducer<string, string>>();
            var sut = CreateSut<string, string>(producer: producer);

            sut.Dispose();

            producer.Received(1).Flush(Arg.Is<TimeSpan>(x => x == TimeSpan.FromSeconds(5)));
            producer.Received(1).Dispose();
        }

        [Fact]
        public void Dispose_WhenProducerIsNull_DoesNotThrow()
        {
            var sut = CreateSut<string, string>(producer: null);

            var ex = Record.Exception(() => sut.Dispose());

            Assert.NotNull(ex);
        }

        [Fact]
        public void OnLog_LogsDebugMessage()
        {
            var logger = new TestLogger<KafkaProducer<string, string>>();
            var producer = Substitute.For<IProducer<string, string>>();
            var sut = CreateSut<string, string>(producer: producer, logger: logger);

            InvokePrivateMethod(
                sut,
                "OnLog",
                producer,
                new LogMessage("client-1", SyslogLevel.Debug, "facility", "some log message"));

            Assert.Contains(logger.Entries, x =>
                x.LogLevel == LogLevel.Debug &&
                x.Message.Contains("Producing to Kafka."));
        }

        [Fact]
        public void OnError_LogsWarning()
        {
            var logger = new TestLogger<KafkaProducer<string, string>>();
            var producer = Substitute.For<IProducer<string, string>>();
            var sut = CreateSut<string, string>(producer: producer, logger: logger);

            InvokePrivateMethod(
                sut,
                "OnError",
                producer,
                new Error(ErrorCode.Local_BadMsg, "bad message"));

            Assert.Contains(logger.Entries, x =>
                x.LogLevel == LogLevel.Warning &&
                x.Message.Contains("Producer error:"));
        }

        [Theory]
        [InlineData(typeof(Null))]
        [InlineData(typeof(byte[]))]
        [InlineData(typeof(string))]
        [InlineData(typeof(long))]
        [InlineData(typeof(int))]
        [InlineData(typeof(double))]
        [InlineData(typeof(float))]
        public void GetSerializerFor_SupportedTypes_ReturnsSerializer(Type type)
        {
            var sut = CreateSut<string, string>();

            var result = InvokePrivateGenericMethod(sut, "GetSerializerFor", type);

            Assert.NotNull(result);
        }

        [Fact]
        public void GetSerializerFor_UnsupportedType_ThrowsNotSupportedException()
        {
            var sut = CreateSut<string, string>();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateGenericMethod(sut, "GetSerializerFor", typeof(DateTime)));

            Assert.IsType<NotSupportedException>(ex.InnerException);
            Assert.Equal("T", ex.InnerException.Message);
        }

        [Fact]
        public void Constructor_WithNullBrokerList_ThrowsNullReferenceException()
        {
            var ex = Assert.Throws<NullReferenceException>(() => new KafkaProducer<string, string>((string)null));

            Assert.Equal("brokerList", ex.Message);
        }

        [Fact]
        public void Constructor_WithBlankBrokerList_ThrowsNullReferenceException()
        {
            var ex = Assert.Throws<NullReferenceException>(() => new KafkaProducer<string, string>(" "));

            Assert.Equal("brokerList", ex.Message);
        }

        [Fact]
        public void Constructor_WithNullBootstrapServersInConfig_ThrowsNullReferenceException()
        {
            var ex = Assert.Throws<NullReferenceException>(() =>
                new KafkaProducer<string, string>(new ProducerConfig()));

            Assert.Equal("producerConfig.BootstrapServers", ex.Message);
        }

        [Fact]
        public void Constructor_WithNullDependentKafkaProducer_ThrowsArgumentNullException()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new KafkaProducer<string, string>((KafkaProducer<string, string>)null));

            Assert.Equal("dependentKafkaProducer", ex.ParamName);
        }

        private static KafkaProducer<TKey, TValue> CreateSut<TKey, TValue>(
            IProducer<TKey, TValue> producer = null,
            ILogger<KafkaProducer<TKey, TValue>> logger = null)
        {
            var instance = (KafkaProducer<TKey, TValue>)FormatterServices.GetUninitializedObject(typeof(KafkaProducer<TKey, TValue>));

            SetPrivateField(instance, "_producer", producer);
            SetPrivateField(instance, "_logger", logger);

            return instance;
        }

        private static void SetPrivateField(object instance, string fieldName, object value)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            field.SetValue(instance, value);
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