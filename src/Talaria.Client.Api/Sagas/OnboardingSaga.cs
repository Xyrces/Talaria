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

public static class OnboardingSagaConfigurator
{
    public static void ConfigureOnboardingSaga(IServiceProvider services)
    {
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("OnboardingSaga");
        var tracker = services.GetRequiredService<ProcessingTracker>();

        services.MapSaga<OnboardingState>(sagas =>
        {
            sagas.StartedBy<CreateAccountCommand>(
                "onboarding-commands",
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
                "account-events",
                async (state, msg, context) =>
                {
                    logger.LogInformation("Received verification for {AccountId}", msg.AccountId);

                    state.VerificationReceived = true;
                    // Return complete to finalize the saga and clear its state.
                    return context.Complete();
                },
                correlateBy: msg => msg.AccountId);

            // The email handler listens on "email-commands" (see Program.cs).
            sagas.DispatchTo<SendVerificationEmailCommand>("email-commands");
        });
    }
}
