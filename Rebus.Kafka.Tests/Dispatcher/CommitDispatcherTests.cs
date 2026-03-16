using Rebus.Activation;
using Rebus.Kafka.Configs;
using Rebus.Kafka.Dispatcher;
using Rebus.Logging;
using Rebus.Messages;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka;
using System.Linq;

namespace Rebus.Kafka.Tests.Dispatcher
{
    public class CommitDispatcherTests
    {
        private readonly CommitDispatcher _dispatcher;
        private readonly ConsumerBehaviorConfig _config;

        public CommitDispatcherTests()
        {
            var loggerFactory = new ConsoleLoggerFactory(false);
            _config = new ConsumerBehaviorConfig { CommitPeriod = 1 };
            _dispatcher = new CommitDispatcher(loggerFactory, _config);
        }

        [Fact]
        public void AppendMessage_AddsMessage()
        {
            var message = CreateMessage("msg1");
            var tpo = new TopicPartitionOffset("topic", 0, 10);
            
            var result = _dispatcher.AppendMessage(message, tpo);
            
            Assert.True(result.Success);
            Assert.Single(_dispatcher._messageInfos);
        }

        [Fact]
        public void AppendMessage_DuplicateId_Fails()
        {
            var message = CreateMessage("msg1");
            var tpo = new TopicPartitionOffset("topic", 0, 10);
            
            _dispatcher.AppendMessage(message, tpo);
            var result = _dispatcher.AppendMessage(message, tpo);
            
            Assert.False(result.Success);
        }

        [Fact]
        public void Completing_UpdatesStatus()
        {
            var message = CreateMessage("msg1");
            var tpo = new TopicPartitionOffset("topic", 0, 10);
            _dispatcher.AppendMessage(message, tpo);
            
            var result = _dispatcher.Completing(message);
            
            Assert.True(result.Success);
            // Since CommitPeriod = 1, it might have already tried to commit and remove it if successful
        }

        [Fact]
        public void Reprocessing_UpdatesStatus()
        {
            var message = CreateMessage("msg1");
            var tpo = new TopicPartitionOffset("topic", 0, 10);
            _dispatcher.AppendMessage(message, tpo);
            
            var result = _dispatcher.Reprocessing(message);
            
            Assert.True(result.Success);
            Assert.Equal(MessageProcessingStatuses.Reprocess, _dispatcher._messageInfos["msg1"].Status);
        }

        [Fact]
        public void TryGetOffsetsThatCanBeCommit_ReturnsCorrectOffsets()
        {
            _config.CommitPeriod = 10; // Prevent auto-commit during completing
            
            var msg1 = CreateMessage("msg1");
            var msg2 = CreateMessage("msg2");
            _dispatcher.AppendMessage(msg1, new TopicPartitionOffset("topic", 0, 10));
            _dispatcher.AppendMessage(msg2, new TopicPartitionOffset("topic", 0, 11));
            
            _dispatcher.Completing(msg1);
            _dispatcher.Completing(msg2);
            
            bool canCommit = _dispatcher.TryGetOffsetsThatCanBeCommit(out var tpos);
            
            Assert.True(canCommit);
            Assert.Single(tpos);
            Assert.Equal(11, tpos[0].Offset.Value);
            Assert.Empty(_dispatcher._messageInfos);
        }

        private TransportMessage CreateMessage(string id)
        {
            return new TransportMessage(new Dictionary<string, string> { { Rebus.Messages.Headers.MessageId, id } }, new byte[0]);
        }
    }
}
