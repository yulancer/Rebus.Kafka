using System;
using System.Reflection;
using Xunit;

namespace Rebus.Kafka.Tests.Extensions
{
    public class OptionsConfigurerExtensionsTests
    {
        [Fact]
        public void TopicAttribute_WithValidName_SetsName()
        {
            var attribute = new TopicAttribute("orders-topic");

            Assert.Equal("orders-topic", attribute.Name);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public void TopicAttribute_WithInvalidName_ThrowsArgumentNullException(string name)
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new TopicAttribute(name));

            Assert.Equal("name", ex.ParamName);
        }

        [Fact]
        public void ShortTopicConvention_WithTopicAttribute_ReturnsAttributeName()
        {
            var convention = CreateConvention();

            var result = InvokeGetTopic(convention, typeof(MessageWithTopicAttribute));

            Assert.Equal("custom-topic", result);
        }

        [Fact]
        public void ShortTopicConvention_WithoutTopicAttribute_ReturnsTypeFullName()
        {
            var convention = CreateConvention();

            var result = InvokeGetTopic(convention, typeof(MessageWithoutTopicAttribute));

            Assert.Equal(typeof(MessageWithoutTopicAttribute).FullName, result);
        }

        [Fact]
        public void ShortTopicConvention_CachesTopicName()
        {
            var convention = CreateConvention();

            var first = InvokeGetTopic(convention, typeof(MessageWithTopicAttribute));
            var second = InvokeGetTopic(convention, typeof(MessageWithTopicAttribute));

            Assert.Equal("custom-topic", first);
            Assert.Equal(first, second);
        }

        private static object CreateConvention()
        {
            var conventionType = typeof(OptionsConfigurerExtensions)
                .GetNestedType("ShortTopicAttributeOrFullNameConvention", BindingFlags.NonPublic);

            Assert.NotNull(conventionType);

            return Activator.CreateInstance(conventionType!);
        }

        private static string InvokeGetTopic(object convention, Type type)
        {
            var method = convention.GetType().GetMethod("GetTopic", BindingFlags.Instance | BindingFlags.Public);
            Assert.NotNull(method);

            return (string)method!.Invoke(convention, new object[] { type })!;
        }

        [Topic("custom-topic")]
        private sealed class MessageWithTopicAttribute
        {
        }

        private sealed class MessageWithoutTopicAttribute
        {
        }
    }
}