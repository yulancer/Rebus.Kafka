using System;
using System.Linq;
using System.Reflection;
using Confluent.SchemaRegistry;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Kafka;
using Xunit;

namespace Rebus.Kafka.Tests.Extensions
{
    public class SchemaRegistryRebusConfigurerExtensionsTests
    {
        [Fact]
        public void UseSchemaRegistryJson_ReturnsSameConfigurer()
        {
            var configurer = Configure.With(new BuiltinHandlerActivator());
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "http://unused-for-tests"
            };

            var result = configurer.UseSchemaRegistryJson(schemaRegistryConfig, autoRegisterSchemas: true);

            Assert.Same(configurer, result);
        }

        [Fact]
        public void UseSchemaRegistryProtobuf_ReturnsSameConfigurer()
        {
            var configurer = Configure.With(new BuiltinHandlerActivator());
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "http://unused-for-tests"
            };

            var result = configurer.UseSchemaRegistryProtobuf(schemaRegistryConfig, autoRegisterSchemas: true);

            Assert.Same(configurer, result);
        }

        [Fact]
        public void UseSchemaRegistryAvro_ReturnsSameConfigurer()
        {
            var configurer = Configure.With(new BuiltinHandlerActivator());
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "http://unused-for-tests"
            };

            var result = configurer.UseSchemaRegistryAvro(schemaRegistryConfig, autoRegisterSchemas: true);

            Assert.Same(configurer, result);
        }

        [Fact]
        public void UseSchemaRegistryProtobuf_HasObsoleteAttribute()
        {
            var method = typeof(SchemaRegistryRebusConfigurerExtensions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(x => x.Name == "UseSchemaRegistryProtobuf");

            var attribute = method.GetCustomAttribute<ObsoleteAttribute>();

            Assert.NotNull(attribute);
            Assert.Equal("It's not working yet", attribute!.Message);
        }

        [Fact]
        public void UseSchemaRegistryAvro_HasObsoleteAttribute()
        {
            var method = typeof(SchemaRegistryRebusConfigurerExtensions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(x => x.Name == "UseSchemaRegistryAvro");

            var attribute = method.GetCustomAttribute<ObsoleteAttribute>();

            Assert.NotNull(attribute);
            Assert.Equal("It's not working yet", attribute!.Message);
        }
    }
}