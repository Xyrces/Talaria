// SPDX-License-Identifier: Apache-2.0
using System.Security.Cryptography;
using System.Text;

namespace Talaria.Core.Registration;

/// <summary>Stable defaults shared by publishers, consumers, and topology declarations.</summary>
public static class MessageNames
{
    public static string Contract(Type type)
    {
        if (type.IsGenericType || type.IsArray) throw new ArgumentException("Message contracts must be named, non-generic types.", nameof(type));
        return type.FullName ?? type.Name;
    }

    public static string Destination(string contract)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(contract);
        var name = new StringBuilder();
        foreach (var c in contract)
            name.Append(char.IsAsciiLetterOrDigit(c) ? char.ToLowerInvariant(c) : '-');
        // Hash prevents normalized names and long names from colliding across contracts.
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(contract)))[..12].ToLowerInvariant();
        return name.ToString()[..Math.Min(name.Length, 36)] + "-" + hash;
    }

    internal static string StripAssemblyQualification(string messageType)
    {
        var depth = 0;
        for (var i = 0; i < messageType.Length; i++)
        {
            switch (messageType[i])
            {
                case '[':
                    depth++;
                    break;
                case ']':
                    if (depth > 0) depth--;
                    break;
                case ',' when depth == 0:
                    return messageType[..i].Trim();
            }
        }
        return messageType.Trim();
    }
}
