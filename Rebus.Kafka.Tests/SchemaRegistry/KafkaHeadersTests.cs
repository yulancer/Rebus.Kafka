using Rebus.Kafka.SchemaRegistry;
using Xunit;

namespace Rebus.Kafka.Tests.SchemaRegistry
{
    public class KafkaHeadersTests
    {
        [Fact]
        public void Create_ReturnsCorrectHeader()
        {
            var message = new TestMessage();
            var key = 123;
            
            var result = KafkaHeaders.Create(message, key);
            
            Assert.Single(result);
            Assert.True(result.ContainsKey(KafkaHeaders.KafkaKey));
            Assert.Equal("TestMessage-123", result[KafkaHeaders.KafkaKey]);
        }

        private class TestMessage { }
    }
}
