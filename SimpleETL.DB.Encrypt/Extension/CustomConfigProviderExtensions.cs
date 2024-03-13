
using Microsoft.Extensions.Configuration;

namespace SimpleETL.DB.Encrypt.Extension
{
    public static class CustomConfigProviderExtensions
    {
        public static IConfigurationBuilder AddEncryptedProvider(this IConfigurationBuilder builder)
        {
            return builder.Add(new CustomConfigProvider());
        }
    }
}
