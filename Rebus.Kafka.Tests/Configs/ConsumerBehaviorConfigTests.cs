using Rebus.Kafka.Configs;
using Xunit;

namespace Rebus.Kafka.Tests.Configs
{
    public class ConsumerBehaviorConfigTests
    {
        [Fact]
        public void DefaultValues_AreCorrect()
        {
            var config = new ConsumerBehaviorConfig();
            Assert.Equal(5, config.CommitPeriod);
        }

        [Fact]
        public void Property_CanBeUpdated()
        {
            var config = new ConsumerBehaviorConfig { CommitPeriod = 10 };
            Assert.Equal(10, config.CommitPeriod);
        }
    }
}
