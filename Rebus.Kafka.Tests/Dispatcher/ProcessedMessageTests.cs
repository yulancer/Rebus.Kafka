using Confluent.Kafka;
using Rebus.Kafka.Dispatcher;
using Rebus.Messages;
using System.Collections.Generic;
using Xunit;

namespace Rebus.Kafka.Tests.Dispatcher
{
    public class ProcessedMessageTests
    {
        [Fact]
        public void Constructor_SetsPropertiesCorrectly()
        {
            var tpo = new TopicPartitionOffset("test-topic", 1, 100);
            var status = MessageProcessingStatuses.Completed;
            var message = new TransportMessage(new Dictionary<string, string>(), new byte[0]);
            
            var processed = new ProcessedMessage(tpo, status, message);
            
            Assert.Equal(tpo, processed.TopicPartitionOffset);
            Assert.Equal(status, processed.Status);
            Assert.Equal(message, processed.Message);
        }

        [Theory]
        [InlineData(MessageProcessingStatuses.Processing, "Processing; Topic:test-topic, Partition:1, Offset:100")]
        [InlineData(MessageProcessingStatuses.Completed, "Completed;  Topic:test-topic, Partition:1, Offset:100")]
        [InlineData(MessageProcessingStatuses.Reprocess, "Reprocess;  Topic:test-topic, Partition:1, Offset:100")]
        internal void ToString_ReturnsExpectedFormat(MessageProcessingStatuses status, string expected)
        {
            var tpo = new TopicPartitionOffset("test-topic", 1, 100);
            var processed = new ProcessedMessage(tpo, status);
            
            var result = processed.ToString();
            
            Assert.Equal(expected, result);
        }
    }
}
