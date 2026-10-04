// SPDX-License-Identifier: Apache-2.0
using System.Security.Cryptography;
using System.Text;

namespace Talaria.StateStores.Redis;

internal static class RedisKeySpace
{
    public static string Prefix(TalariaRedisOptions options, string applicationName)
    {
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(options.KeyPrefix + applicationName)))[..24];
        return $"{options.KeyPrefix}v2:{{{hash}}}:";
    }

    public static string Component(string value)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
}
