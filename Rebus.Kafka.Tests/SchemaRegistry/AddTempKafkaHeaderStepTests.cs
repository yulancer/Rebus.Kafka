using Rebus.Kafka.SchemaRegistry;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Transport;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Rebus.Kafka.Tests.SchemaRegistry
{
    public class AddTempKafkaHeaderStepTests
    {
        [Fact]
        public async Task Process_AddsTopicAndKeyHeaders()
        {
            var step = new AddTempKafkaHeaderStep();
            var headers = new Dictionary<string, string> { { Headers.MessageId, "test-id" } };
            var message = new Message(headers, new TestBody());
            
            var context = new OutgoingStepContext(message, new FakeTransactionContext(), new Rebus.Pipeline.Send.DestinationAddresses(new[] { "test-topic" }));
            
            bool nextCalled = false;
            Task Next() { nextCalled = true; return Task.CompletedTask; }
            
            await step.Process(context, Next);
            
            Assert.True(nextCalled);
            Assert.Equal("test-topic", message.Headers[KafkaHeaders.KafkaTopic]);
            Assert.Equal("TestBody-test-id", message.Headers[KafkaHeaders.KafkaKey]);
        }

        [Fact]
        public async Task Process_DoesNotOverrideExistingKey()
        {
            var step = new AddTempKafkaHeaderStep();
            var headers = new Dictionary<string, string> 
            { 
                { Headers.MessageId, "test-id" },
                { KafkaHeaders.KafkaKey, "existing-key" }
            };
            var message = new Message(headers, new TestBody());
            
            var context = new OutgoingStepContext(message, new FakeTransactionContext(), new Rebus.Pipeline.Send.DestinationAddresses(new string[0]));
            
            await step.Process(context, () => Task.CompletedTask);
            
            Assert.Equal("existing-key", message.Headers[KafkaHeaders.KafkaKey]);
        }

        private class TestBody { }

        private class FakeTransactionContext : ITransactionContext
        {
            public void Dispose() { }
            public void OnCommitted(Func<ITransactionContext, Task> callback) { }
            public void OnCompleted(Func<ITransactionContext, Task> callback) { }
            public void OnAborted(Func<ITransactionContext, Task> callback) { }
            public void OnDisposed(Action<ITransactionContext> callback) { }
            public void OnRollingBack(Func<ITransactionContext, Task> callback) { }
            public Task Commit() => Task.CompletedTask;
            public Task Rollback() => Task.CompletedTask;
            public void Abort() { }
            public ConcurrentDictionary<string, object> Items { get; } = new ConcurrentDictionary<string, object>();
            
            public void OnAck(Func<ITransactionContext, Task> callback) { }
            public void OnNack(Func<ITransactionContext, Task> callback) { }
            public void SetResult(bool ack, bool nack) { }
            public void OnCommit(Func<ITransactionContext, Task> callback) { }
            public void OnRollback(Func<ITransactionContext, Task> callback) { }
        }
    }
}
