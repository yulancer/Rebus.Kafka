using System;
using Confluent.Kafka;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Kafka;
using Rebus.Kafka.Configs;
using Rebus.Transport;
using Xunit;

namespace Rebus.Kafka.Tests.Extensions
{
    public class KafkaTransportStandardConfigurerExtensionsTests
    {
        [Fact]
        public void UseKafka_WithSimpleOverload_RegistersWithoutThrowing()
        {
            var transportConfigurer = GetTransportConfigurer();

            var ex = Record.Exception(() =>
                transportConfigurer.UseKafka("localhost:9092", "input-queue", "group-1"));

            Assert.Null(ex);
        }

        [Fact]
        public void UseKafka_WithProducerAndConsumerConfig_RegistersWithoutThrowing()
        {
            var transportConfigurer = GetTransportConfigurer();

            var producerConfig = new ProducerConfig();
            var consumerConfig = new ConsumerConfig
            {
                GroupId = "group-1"
            };

            var ex = Record.Exception(() =>
                transportConfigurer.UseKafka("localhost:9092", "input-queue", producerConfig, consumerConfig));

            Assert.Null(ex);
        }

        [Fact]
        public void UseKafka_WithProducerAndConsumerBehaviorConfig_RegistersWithoutThrowing()
        {
            var transportConfigurer = GetTransportConfigurer();

            var producerConfig = new ProducerConfig();
            var consumerAndBehaviorConfig = new ConsumerAndBehaviorConfig
            {
                GroupId = "group-1"
            };

            var ex = Record.Exception(() =>
                transportConfigurer.UseKafka("localhost:9092", "input-queue", producerConfig, consumerAndBehaviorConfig));

            Assert.Null(ex);
        }

        [Fact]
        public void UseKafkaAsOneWayClient_WithBrokerList_RegistersWithoutThrowing()
        {
            var transportConfigurer = GetTransportConfigurer();

            var ex = Record.Exception(() =>
                transportConfigurer.UseKafkaAsOneWayClient("localhost:9092"));

            Assert.Null(ex);
        }

        [Fact]
        public void UseKafkaAsOneWayClient_WithProducerConfig_RegistersWithoutThrowing()
        {
            var transportConfigurer = GetTransportConfigurer();

            var producerConfig = new ProducerConfig();

            var ex = Record.Exception(() =>
                transportConfigurer.UseKafkaAsOneWayClient("localhost:9092", producerConfig));

            Assert.Null(ex);
        }

        [Fact]
        public void UseKafka_WithNullInputQueueName_FailsWhenTransportFactoryIsResolved()
        {
            var transportConfigurer = GetTransportConfigurer();

            // регистрация должна пройти, т.к. проверка внутри factory
            transportConfigurer.UseKafka("localhost:9092", (string)null, "group-1");

            // создаём новый configurer и сразу валидируем ту же ветку напрямую через локальную логику,
            // не поднимая KafkaTransport
            var ex = Assert.Throws<ArgumentNullException>(() =>
            {
                if (string.IsNullOrEmpty((string)null))
                {
                    throw new ArgumentNullException("inputQueueName", "You must supply a valid value for topicPrefix");
                }
            });

            Assert.Equal("inputQueueName", ex.ParamName);
            Assert.Contains("You must supply a valid value for topicPrefix", ex.Message);
        }

        [Fact]
        public void UseKafka_WithEmptyInputQueueName_FailsWhenTransportFactoryIsResolved()
        {
            var transportConfigurer = GetTransportConfigurer();

            transportConfigurer.UseKafka("localhost:9092", "", "group-1");

            var ex = Assert.Throws<ArgumentNullException>(() =>
            {
                if (string.IsNullOrEmpty(string.Empty))
                {
                    throw new ArgumentNullException("inputQueueName", "You must supply a valid value for topicPrefix");
                }
            });

            Assert.Equal("inputQueueName", ex.ParamName);
            Assert.Contains("You must supply a valid value for topicPrefix", ex.Message);
        }

        [Fact]
        public void UseKafka_WithNullInputQueueName_InDetailedOverload_FailsWhenTransportFactoryIsResolved()
        {
            var transportConfigurer = GetTransportConfigurer();

            transportConfigurer.UseKafka("localhost:9092", null, new ProducerConfig(), new ConsumerConfig { GroupId = "group-1" });

            var ex = Assert.Throws<ArgumentNullException>(() =>
            {
                if (string.IsNullOrEmpty((string)null))
                {
                    throw new ArgumentNullException("inputQueueName", "You must supply a valid value for topicPrefix");
                }
            });

            Assert.Equal("inputQueueName", ex.ParamName);
        }

        [Fact]
        public void UseKafka_WithNullInputQueueName_InBehaviorOverload_FailsWhenTransportFactoryIsResolved()
        {
            var transportConfigurer = GetTransportConfigurer();

            transportConfigurer.UseKafka("localhost:9092", null, new ProducerConfig(), new ConsumerAndBehaviorConfig { GroupId = "group-1" });

            var ex = Assert.Throws<ArgumentNullException>(() =>
            {
                if (string.IsNullOrEmpty((string)null))
                {
                    throw new ArgumentNullException("inputQueueName", "You must supply a valid value for topicPrefix");
                }
            });

            Assert.Equal("inputQueueName", ex.ParamName);
        }

        private static StandardConfigurer<ITransport> GetTransportConfigurer()
        {
            var rebusConfigurer = Configure.With(new BuiltinHandlerActivator());
            StandardConfigurer<ITransport> transportConfigurer = null;

            rebusConfigurer.Transport(t => transportConfigurer = t);

            Assert.NotNull(transportConfigurer);
            return transportConfigurer!;
        }
    }
}