// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Configuration;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Core.Abstractions;

namespace Talaria.Client.Api.Sagas;

public class OnboardingState
{
    public string AccountId { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public bool VerificationSent { get; set; }
    public bool VerificationReceived { get; set; }
    public int ReminderCount { get; set; }
}

public class CreateAccountCommand
{
    public string AccountId { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}

public class AccountVerifiedEvent
{
    public string AccountId { get; set; } = string.Empty;
}

public class SendVerificationEmailCommand
{
    public string AccountId { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}

/// <summary>
/// Topic names consumed by the onboarding sample saga. Defaults match the sample;
/// override per deployment via configuration keys (<c>Talaria:Topics:OnboardingCommands</c>,
/// <c>Talaria:Topics:AccountEvents</c>, <c>Talaria:Topics:EmailCommands</c>).
/// </summary>
public sealed record OnboardingSagaTopics(
    string OnboardingCommands,
    string AccountEvents,
    string EmailCommands)
{
    /// <summary>Resolves topic names from <see cref="IConfiguration"/> with sample defaults.</summary>
    public static OnboardingSagaTopics FromConfiguration(IConfiguration configuration) => new(
        configuration["Talaria:Topics:OnboardingCommands"] ?? "onboarding-commands",
        configuration["Talaria:Topics:AccountEvents"] ?? "account-events",
        configuration["Talaria:Topics:EmailCommands"] ?? "email-commands");

    internal static readonly OnboardingSagaTopics Defaults = new(
        "onboarding-commands", "account-events", "email-commands");
}

public static class OnboardingSagaConfigurator
{
    public static void ConfigureOnboardingSaga(IServiceProvider services)
        => ConfigureOnboardingSaga(services, OnboardingSagaTopics.Defaults);

    public static void ConfigureOnboardingSaga(IServiceProvider services, OnboardingSagaTopics topics)
    {
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("OnboardingSaga");
        var tracker = services.GetRequiredService<ProcessingTracker>();

        services.MapSaga<OnboardingState>(sagas =>
        {
            sagas.StartedBy<CreateAccountCommand>(
                topics.OnboardingCommands,
                async (msg, context) =>
                {
                    logger.LogInformation("Creating account {AccountId}", msg.AccountId);
                    tracker.Increment($"created:{msg.AccountId}");

                    var state = new OnboardingState
                    {
                        AccountId = msg.AccountId,
                        Email = msg.Email,
                        VerificationSent = true
                    };

                    context.Dispatch(new SendVerificationEmailCommand
                    {
                        AccountId = msg.AccountId,
                        Email = msg.Email
                    });

                    // Return transition to save the current state and wait for the next event.
                    return context.Transition(state);
                },
                correlateBy: msg => msg.AccountId);

            sagas.On<AccountVerifiedEvent>(
                topics.AccountEvents,
                async (state, msg, context) =>
                {
                    logger.LogInformation("Received verification for {AccountId}", msg.AccountId);

                    state.VerificationReceived = true;
                    // Return complete to finalize the saga and clear its state.
                    return context.Complete();
                },
                correlateBy: msg => msg.AccountId);

            // The email handler listens on the email-commands topic (see Program.cs).
            sagas.DispatchTo<SendVerificationEmailCommand>(topics.EmailCommands);
        });
    }
}
