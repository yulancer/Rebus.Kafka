using Rebus.Kafka.Extensions;
using Rebus.Messages;
using System.Collections.Generic;
using Xunit;

namespace Rebus.Kafka.Tests.Extensions
{
    public class TransportMessageExtensionsTests
    {
        [Fact]
        public void ToReadableText_NullMessage_ReturnsEmptyString()
        {
            TransportMessage message = null;
            var result = message.ToReadableText();
            Assert.Equal("", result);
        }

        [Fact]
        public void ToReadableText_ValidMessage_ReturnsJson()
        {
            var headers = new Dictionary<string, string> { { "test-header", "test-value" } };
            var body = new byte[] { 1, 2, 3 };
            var message = new TransportMessage(headers, body);
            
            var result = message.ToReadableText();
            
            Assert.Contains("test-header", result);
            Assert.Contains("test-value", result);
        }

        [Fact]
        public void GetId_NullMessage_ReturnsNull()
        {
            TransportMessage message = null;
            var result = message.GetId();
            Assert.Null(result);
        }

        [Fact]
        public void GetId_MessageWithId_ReturnsId()
        {
            const string messageId = "test-id";
            var headers = new Dictionary<string, string> { { Headers.MessageId, messageId } };
            var message = new TransportMessage(headers, new byte[0]);
            
            var result = message.GetId();
            
            Assert.Equal(messageId, result);
        }

        [Fact]
        public void GetId_MessageWithoutId_ReturnsNull()
        {
            var headers = new Dictionary<string, string>();
            var message = new TransportMessage(headers, new byte[0]);
            
            var result = message.GetId();
            
            Assert.Null(result);
        }
    }
}
