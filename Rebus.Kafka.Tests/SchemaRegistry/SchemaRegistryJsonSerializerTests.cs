using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Rebus.Kafka.SchemaRegistry;
using Rebus.Messages;
using Rebus.Serialization;
using Xunit;
using Headers = Rebus.Messages.Headers;
using Type = System.Type;

namespace Rebus.Kafka.Tests.SchemaRegistry
{
    public class SchemaRegistryJsonSerializerTests
    {
        [Fact]
        public async Task Serialize_WithTopicStrategy_ReturnsTransportMessage_WithSerializedBody_AndClonedHeaders()
        {
            var sut = CreateSut();

            var serializers = GetPrivateField<ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object>>(sut, "_serializers");
            serializers[(typeof(TestMessage), SubjectNameStrategy.Topic)] = new FakeJsonSerializer("serialized-body");

            var headers = new Dictionary<string, string>
            {
                [Headers.Intent] = Headers.IntentOptions.PublishSubscribe,
                [KafkaHeaders.KafkaTopic] = "topic-1",
                ["h1"] = "v1",
            };

            var message = new Message(headers, new TestMessage { Text = "hello" });

            var result = await sut.Serialize(message);

            Assert.NotNull(result);
            Assert.Equal("serialized-body", Encoding.UTF8.GetString(result.Body));
            Assert.Equal("v1", result.Headers["h1"]);
            Assert.Equal("topic-1", result.Headers[KafkaHeaders.KafkaTopic]);
            Assert.NotSame(headers, result.Headers);
        }

        [Fact]
        public async Task Serialize_WithRecordStrategy_ReturnsTransportMessage()
        {
            var sut = CreateSut();

            var serializers = GetPrivateField<ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object>>(sut, "_serializers");
            serializers[(typeof(TestMessage), SubjectNameStrategy.Record)] = new FakeJsonSerializer("record-body");

            var headers = new Dictionary<string, string>
            {
                [Headers.Intent] = Headers.IntentOptions.PointToPoint,
                [KafkaHeaders.KafkaTopic] = "queue-topic",
            };

            var message = new Message(headers, new TestMessage { Text = "hello" });

            var result = await sut.Serialize(message);

            Assert.NotNull(result);
            Assert.Equal("record-body", Encoding.UTF8.GetString(result.Body));
        }

        [Fact]
        public async Task Serialize_UsesCachedSerializer()
        {
            var sut = CreateSut();

            var fakeSerializer = new FakeJsonSerializer("cached");
            var serializers = GetPrivateField<ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object>>(sut, "_serializers");
            serializers[(typeof(TestMessage), SubjectNameStrategy.Topic)] = fakeSerializer;

            await sut.Serialize(new Message(
                new Dictionary<string, string>
                {
                    [Headers.Intent] = Headers.IntentOptions.PublishSubscribe,
                    [KafkaHeaders.KafkaTopic] = "topic-1",
                },
                new TestMessage { Text = "one" }));

            await sut.Serialize(new Message(
                new Dictionary<string, string>
                {
                    [Headers.Intent] = Headers.IntentOptions.PublishSubscribe,
                    [KafkaHeaders.KafkaTopic] = "topic-1",
                },
                new TestMessage { Text = "two" }));

            Assert.Equal(2, fakeSerializer.Calls);
            Assert.Single(serializers);
        }

        [Fact]
        public async Task Serialize_WithAutoRegisterSchemas_AddsSchemaIdHeader()
        {
            var sut = CreateSut(autoRegisterSchemas: true);

            var serializers = GetPrivateField<ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object>>(sut, "_serializers");
            serializers[(typeof(TestMessage), SubjectNameStrategy.Topic)] = new FakeJsonSerializer(CreateConfluentPayloadWithSchemaId(123));

            var headers = new Dictionary<string, string>
            {
                [Headers.Intent] = Headers.IntentOptions.PublishSubscribe,
                [KafkaHeaders.KafkaTopic] = "topic-1",
            };

            var message = new Message(headers, new TestMessage { Text = "hello" });

            var result = await sut.Serialize(message);

            Assert.Equal("123", result.Headers[KafkaHeaders.ValueSchemaId]);
            Assert.Equal("123", message.Headers[KafkaHeaders.ValueSchemaId]);
        }

        [Fact]
        public async Task Deserialize_ReturnsMessage_WithDeserializedBody_AndClonedHeaders()
        {
            var sut = CreateSut();

            var deserializers = GetPrivateField<ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object>>(sut, "_deserializers");
            deserializers[(typeof(TestMessage), SubjectNameStrategy.Topic)] = new FakeJsonDeserializer(new TestMessage { Text = "deserialized-value" });

            var headers = new Dictionary<string, string>
            {
                [Headers.Type] = typeof(TestMessage).AssemblyQualifiedName!,
                [Headers.Intent] = Headers.IntentOptions.PublishSubscribe,
                [KafkaHeaders.KafkaTopic] = "topic-1",
                ["custom"] = "123",
            };

            var transportMessage = new TransportMessage(headers, new byte[] { 1, 2, 3 });

            var result = await sut.Deserialize(transportMessage);

            Assert.NotNull(result);
            Assert.Equal("123", result.Headers["custom"]);
            Assert.NotSame(headers, result.Headers);

            var body = Assert.IsType<TestMessage>(result.Body);
            Assert.Equal("deserialized-value", body.Text);
        }

        [Fact]
        public async Task Deserialize_WithRecordStrategy_UsesInputQueueName()
        {
            var sut = CreateSut(inputQueueName: "input-queue");

            var deserializers = GetPrivateField<ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object>>(sut, "_deserializers");
            deserializers[(typeof(TestMessage), SubjectNameStrategy.Record)] = new FakeJsonDeserializer(new TestMessage { Text = "record-value" });

            var headers = new Dictionary<string, string>
            {
                [Headers.Type] = typeof(TestMessage).AssemblyQualifiedName!,
                [Headers.Intent] = Headers.IntentOptions.PointToPoint,
            };

            var transportMessage = new TransportMessage(headers, new byte[] { 1, 2, 3 });

            var result = await sut.Deserialize(transportMessage);

            var body = Assert.IsType<TestMessage>(result.Body);
            Assert.Equal("record-value", body.Text);
        }

        [Fact]
        public async Task Deserialize_UsesCachedDeserializer()
        {
            var sut = CreateSut();

            var fakeDeserializer = new FakeJsonDeserializer(new TestMessage { Text = "cached-value" });
            var deserializers = GetPrivateField<ConcurrentDictionary<(Type type, SubjectNameStrategy strategy), object>>(sut, "_deserializers");
            deserializers[(typeof(TestMessage), SubjectNameStrategy.Topic)] = fakeDeserializer;

            var headers = new Dictionary<string, string>
            {
                [Headers.Type] = typeof(TestMessage).AssemblyQualifiedName!,
                [Headers.Intent] = Headers.IntentOptions.PublishSubscribe,
                [KafkaHeaders.KafkaTopic] = "topic-1",
            };

            var transportMessage = new TransportMessage(headers, new byte[] { 10, 20, 30 });

            await sut.Deserialize(transportMessage);
            await sut.Deserialize(transportMessage);

            Assert.Equal(2, fakeDeserializer.Calls);
            Assert.Single(deserializers);
        }

        [Fact]
        public async Task Deserialize_WithoutTypeHeader_ThrowsInvalidOperationException()
        {
            var sut = CreateSut();

            var transportMessage = new TransportMessage(
                new Dictionary<string, string>
                {
                    [Headers.Intent] = Headers.IntentOptions.PublishSubscribe,
                    [KafkaHeaders.KafkaTopic] = "topic-1",
                },
                new byte[] { 1, 2, 3 });

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.Deserialize(transportMessage));

            Assert.Equal($"Missing \"{Headers.Type}\" header.", ex.Message);
        }

        [Fact]
        public void ExtractSchemaId_ReturnsSchemaId_FromConfluentPayload()
        {
            var sut = CreateSut();

            var result = InvokePrivateMethod<string>(sut, "ExtractSchemaId", CreateConfluentPayloadWithSchemaId(321));

            Assert.Equal("321", result);
        }

        [Fact]
        public void ExtractSchemaId_WithInvalidPayload_ThrowsInvalidDataException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<string>(sut, "ExtractSchemaId", new byte[] { 1, 2, 3, 4 }));

            var inner = Assert.IsType<InvalidDataException>(ex.InnerException);
            Assert.Equal("Invalid Confluent wire format", inner.Message);
        }

        [Fact]
        public void GetStrategy_WithPublishSubscribe_ReturnsTopic()
        {
            var sut = CreateSut();

            var result = InvokePrivateMethod<SubjectNameStrategy>(
                sut,
                "GetStrategy",
                new Dictionary<string, string>
                {
                    [Headers.Intent] = Headers.IntentOptions.PublishSubscribe,
                });

            Assert.Equal(SubjectNameStrategy.Topic, result);
        }

        [Fact]
        public void GetStrategy_WithPointToPoint_ReturnsRecord()
        {
            var sut = CreateSut();

            var result = InvokePrivateMethod<SubjectNameStrategy>(
                sut,
                "GetStrategy",
                new Dictionary<string, string>
                {
                    [Headers.Intent] = Headers.IntentOptions.PointToPoint,
                });

            Assert.Equal(SubjectNameStrategy.Record, result);
        }

        [Fact]
        public void GetStrategy_WithNullHeaders_ThrowsArgumentNullException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetParameterCountException>(() =>
                InvokePrivateMethod<SubjectNameStrategy>(sut, "GetStrategy", null));

            Assert.Null(ex.InnerException);
        }

        [Fact]
        public void GetStrategy_WithoutIntentHeader_ThrowsInvalidOperationException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<SubjectNameStrategy>(sut, "GetStrategy", new Dictionary<string, string>()));

            var inner = Assert.IsType<InvalidOperationException>(ex.InnerException);
            Assert.Equal($"Missing \"{Headers.Intent}\" header.", inner.Message);
        }

        [Fact]
        public void GetStrategy_WithUnsupportedIntent_ThrowsNotSupportedException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<SubjectNameStrategy>(
                    sut,
                    "GetStrategy",
                    new Dictionary<string, string>
                    {
                        [Headers.Intent] = "unsupported",
                    }));

            var inner = Assert.IsType<NotSupportedException>(ex.InnerException);
            Assert.Equal($"The \"{Headers.Intent}\" header with the value \"unsupported\" is not supported.", inner.Message);
        }

        [Fact]
        public void GetSubjectName_WithRecordStrategy_ReturnsInputQueueName()
        {
            var sut = CreateSut(inputQueueName: "input-queue");

            var result = InvokePrivateMethod<string>(
                sut,
                "GetSubjectName",
                new Dictionary<string, string>(),
                SubjectNameStrategy.Record);

            Assert.Equal("input-queue", result);
        }

        [Fact]
        public void GetSubjectName_WithRecordStrategy_AndNullInputQueueName_ThrowsInvalidOperationException()
        {
            var sut = CreateSut(inputQueueName: null);

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<string>(
                    sut,
                    "GetSubjectName",
                    new Dictionary<string, string>(),
                    SubjectNameStrategy.Record));

            var inner = Assert.IsType<InvalidOperationException>(ex.InnerException);
            Assert.Equal("If there is no _inputQueueName, a unidirectional mode is expected, but a message deserialization request has been received!", inner.Message);
        }

        [Fact]
        public void GetSubjectName_WithTopicStrategy_ReturnsKafkaTopic()
        {
            var sut = CreateSut();

            var result = InvokePrivateMethod<string>(
                sut,
                "GetSubjectName",
                new Dictionary<string, string>
                {
                    [KafkaHeaders.KafkaTopic] = "topic-1",
                },
                SubjectNameStrategy.Topic);

            Assert.Equal("topic-1", result);
        }

        [Fact]
        public void GetSubjectName_WithTopicStrategy_AndMissingKafkaTopic_ThrowsNotSupportedException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<string>(
                    sut,
                    "GetSubjectName",
                    new Dictionary<string, string>(),
                    SubjectNameStrategy.Topic));

            var inner = Assert.IsType<NotSupportedException>(ex.InnerException);
            Assert.Equal($"There is not enough header \"{KafkaHeaders.KafkaTopic}\" for serialization through the schema registry.", inner.Message);
        }

        [Fact]
        public void GetSubjectName_WithUnsupportedStrategy_ThrowsNotSupportedException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<string>(
                    sut,
                    "GetSubjectName",
                    new Dictionary<string, string>(),
                    (SubjectNameStrategy)999));

            var inner = Assert.IsType<NotSupportedException>(ex.InnerException);
            Assert.Equal("Not Supported SubjectNameStrategy: 999", inner.Message);
        }

        [Fact]
        public void PullSubjectName_WithTopicStrategy_ReturnsTopic()
        {
            var sut = CreateSut();

            var result = InvokePrivateMethod<string>(
                sut,
                "PullSubjectName",
                new Dictionary<string, string>
                {
                    [KafkaHeaders.KafkaTopic] = "topic-1",
                },
                SubjectNameStrategy.Topic);

            Assert.Equal("topic-1", result);
        }

        [Fact]
        public void PullSubjectName_WithRecordStrategy_ReturnsTopicWithValueSuffix()
        {
            var sut = CreateSut();

            var result = InvokePrivateMethod<string>(
                sut,
                "PullSubjectName",
                new Dictionary<string, string>
                {
                    [KafkaHeaders.KafkaTopic] = "topic-1",
                },
                SubjectNameStrategy.Record);

            Assert.Equal("topic-1-value", result);
        }

        [Fact]
        public void PullSubjectName_WithNullHeaders_ThrowsArgumentNullException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<string>(
                    sut,
                    "PullSubjectName",
                    null,
                    SubjectNameStrategy.Topic));

            Assert.IsType<ArgumentNullException>(ex.InnerException);
        }

        [Fact]
        public void PullSubjectName_WithMissingKafkaTopic_ThrowsNotSupportedException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<string>(
                    sut,
                    "PullSubjectName",
                    new Dictionary<string, string>(),
                    SubjectNameStrategy.Topic));

            var inner = Assert.IsType<NotSupportedException>(ex.InnerException);
            Assert.Equal($"There is not enough header \"{KafkaHeaders.KafkaTopic}\" for serialization through the schema registry.", inner.Message);
        }

        [Fact]
        public void PullSubjectName_WithUnsupportedStrategy_ThrowsNotSupportedException()
        {
            var sut = CreateSut();

            var ex = Assert.Throws<TargetInvocationException>(() =>
                InvokePrivateMethod<string>(
                    sut,
                    "PullSubjectName",
                    new Dictionary<string, string>
                    {
                        [KafkaHeaders.KafkaTopic] = "topic-1",
                    },
                    (SubjectNameStrategy)999));

            var inner = Assert.IsType<NotSupportedException>(ex.InnerException);
            Assert.Equal("Strategy 999 not supported.", inner.Message);
        }

        private static SchemaRegistryJsonSerializer CreateSut(bool autoRegisterSchemas = false, string inputQueueName = "input-queue")
        {
            return new SchemaRegistryJsonSerializer(
                new SchemaRegistryConfig
                {
                    Url = "http://unused-for-unit-tests",
                },
                inputQueueName,
                autoRegisterSchemas);
        }

        private static T GetPrivateField<T>(object instance, string fieldName)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return (T)field!.GetValue(instance)!;
        }

        private static T InvokePrivateMethod<T>(object instance, string methodName, params object[] args)
        {
            var method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(method);
            return (T)method!.Invoke(instance, args)!;
        }

        private static byte[] CreateConfluentPayloadWithSchemaId(int schemaId)
        {
            var schemaIdBytes = BitConverter.GetBytes(schemaId);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(schemaIdBytes);
            }

            var payload = new byte[5 + 3];
            payload[0] = 0x00;
            Buffer.BlockCopy(schemaIdBytes, 0, payload, 1, 4);
            payload[5] = 11;
            payload[6] = 22;
            payload[7] = 33;
            return payload;
        }

        private sealed class FakeJsonSerializer
        {
            private readonly byte[] _result;
            public int Calls { get; private set; }

            public FakeJsonSerializer(string result)
            {
                _result = Encoding.UTF8.GetBytes(result);
            }

            public FakeJsonSerializer(byte[] result)
            {
                _result = result;
            }

            public Task<byte[]> SerializeAsync(TestMessage value, SerializationContext context)
            {
                Calls++;
                return Task.FromResult(_result);
            }
        }

        private sealed class FakeJsonDeserializer
        {
            private readonly TestMessage _result;
            public int Calls { get; private set; }

            public FakeJsonDeserializer(TestMessage result)
            {
                _result = result;
            }

            public Task<TestMessage> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
            {
                Calls++;
                return Task.FromResult(_result);
            }
        }

        private sealed class TestMessage
        {
            public string Text { get; set; }
        }
    }
}