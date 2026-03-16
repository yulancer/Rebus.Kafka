using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Google.Protobuf.WellKnownTypes;
using Rebus.Kafka.SchemaRegistry;
using Rebus.Messages;
using Rebus.Serialization;
using Xunit;
using Headers = Rebus.Messages.Headers;
using Type = System.Type;

namespace Rebus.Kafka.Tests.SchemaRegistry
{
    public class SchemaRegistryProtobufSerializerTests
    {
        [Fact]
        public async Task Serialize_ReturnsTransportMessage_WithSerializedBody_AndClonedHeaders()
        {
            var sut = CreateSut();

            var serializers = GetPrivateField<ConcurrentDictionary<Type, object>>(sut, "_serializers");
            serializers[typeof(StringValue)] = new FakeStringValueSerializer("serialized-body");

            var headers = new Dictionary<string, string>
            {
                ["h1"] = "v1"
            };

            var message = new Message(headers, new StringValue { Value = "hello" });

            var result = await sut.Serialize(message);

            Assert.NotNull(result);
            Assert.Equal("serialized-body", System.Text.Encoding.UTF8.GetString(result.Body));
            Assert.Equal("v1", result.Headers["h1"]);
            Assert.NotSame(headers, result.Headers);
        }

        [Fact]
        public async Task Serialize_UsesCachedSerializer()
        {
            var sut = CreateSut();

            var fakeSerializer = new FakeStringValueSerializer("cached");
            var serializers = GetPrivateField<ConcurrentDictionary<Type, object>>(sut, "_serializers");
            serializers[typeof(StringValue)] = fakeSerializer;

            await sut.Serialize(new Message(new Dictionary<string, string>(), new StringValue { Value = "one" }));
            await sut.Serialize(new Message(new Dictionary<string, string>(), new StringValue { Value = "two" }));

            Assert.Equal(2, fakeSerializer.Calls);
            Assert.Single(serializers);
        }

        [Fact]
        public async Task Deserialize_ReturnsMessage_WithDeserializedBody_AndClonedHeaders()
        {
            var sut = CreateSut();

            var deserializers = GetPrivateField<ConcurrentDictionary<Type, object>>(sut, "_deserializers");
            deserializers[typeof(StringValue)] = new FakeStringValueDeserializer("deserialized-value");

            var headers = new Dictionary<string, string>
            {
                [Headers.Type] = typeof(StringValue).AssemblyQualifiedName!,
                ["custom"] = "123"
            };

            var transportMessage = new TransportMessage(headers, new byte[] { 1, 2, 3 });

            var result = await sut.Deserialize(transportMessage);

            Assert.NotNull(result);
            Assert.Equal("123", result.Headers["custom"]);
            Assert.NotSame(headers, result.Headers);

            var body = Assert.IsType<StringValue>(result.Body);
            Assert.Equal("deserialized-value", body.Value);
        }

        [Fact]
        public async Task Deserialize_UsesCachedDeserializer()
        {
            var sut = CreateSut();

            var fakeDeserializer = new FakeStringValueDeserializer("cached-value");
            var deserializers = GetPrivateField<ConcurrentDictionary<Type, object>>(sut, "_deserializers");
            deserializers[typeof(StringValue)] = fakeDeserializer;

            var headers = new Dictionary<string, string>
            {
                [Headers.Type] = typeof(StringValue).AssemblyQualifiedName!
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
                new Dictionary<string, string>(),
                new byte[] { 1, 2, 3 });

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.Deserialize(transportMessage));

            Assert.Equal("Missing message type header.", ex.Message);
        }

        [Fact]
        public void GetSubjectName_WithTopicStrategy_ReturnsFullNameWithValueSuffix()
        {
            var sut = CreateSut();
            SetPrivateField(sut, "_subjectNameStrategy", SubjectNameStrategy.Topic);

            var result = InvokeGetSubjectName(sut, typeof(StringValue));

            Assert.Equal($"{typeof(StringValue).FullName}-value", result);
        }

        [Fact]
        public void GetSubjectName_WithRecordStrategy_ReturnsFullName()
        {
            var sut = CreateSut();
            SetPrivateField(sut, "_subjectNameStrategy", SubjectNameStrategy.Record);

            var result = InvokeGetSubjectName(sut, typeof(StringValue));

            Assert.Equal(typeof(StringValue).FullName, result);
        }

        [Fact]
        public void GetSubjectName_WithUnsupportedStrategy_ThrowsNotSupportedException()
        {
            var sut = CreateSut();
            SetPrivateField(sut, "_subjectNameStrategy", (SubjectNameStrategy)999);

            var ex = Assert.Throws<TargetInvocationException>(() => InvokeGetSubjectName(sut, typeof(StringValue)));

            var inner = Assert.IsType<NotSupportedException>(ex.InnerException);
            Assert.Equal("Strategy 999 not supported.", inner.Message);
        }

        private static SchemaRegistryProtobufSerializer CreateSut()
        {
            return new SchemaRegistryProtobufSerializer(
                new Confluent.SchemaRegistry.SchemaRegistryConfig
                {
                    Url = "http://unused-for-unit-tests"
                });
        }

        private static string InvokeGetSubjectName(SchemaRegistryProtobufSerializer sut, Type type)
        {
            var method = typeof(SchemaRegistryProtobufSerializer)
                .GetMethod("GetSubjectName", BindingFlags.Instance | BindingFlags.NonPublic);

            Assert.NotNull(method);

            return (string)method!.Invoke(sut, new object[] { type })!;
        }

        private static T GetPrivateField<T>(object instance, string fieldName)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return (T)field!.GetValue(instance)!;
        }

        private static void SetPrivateField(object instance, string fieldName, object value)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            field!.SetValue(instance, value);
        }

        private sealed class FakeStringValueSerializer
        {
            private readonly byte[] _result;
            public int Calls { get; private set; }

            public FakeStringValueSerializer(string result)
            {
                _result = System.Text.Encoding.UTF8.GetBytes(result);
            }

            public Task<byte[]> SerializeAsync(StringValue value, SerializationContext context)
            {
                Calls++;
                return Task.FromResult(_result);
            }
        }

        private sealed class FakeStringValueDeserializer
        {
            private readonly StringValue _result;
            public int Calls { get; private set; }

            public FakeStringValueDeserializer(string value)
            {
                _result = new StringValue { Value = value };
            }

            public Task<StringValue> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
            {
                Calls++;
                return Task.FromResult(_result);
            }
        }
    }
}