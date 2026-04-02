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
        services.MapSaga<OnboardingState>(sagas => 
        {
            sagas.StartedBy<CreateAccountCommand>(
                "onboarding-commands",
                async (msg, context) => 
                {
                    // Using console for simpler logging out of DI space within config, 
                    // or could resolve ILoggerFactory from services if needed.
                    Console.WriteLine($"[SAGA] Creating account {msg.AccountId} for {msg.Email}");

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

                    Console.WriteLine($"[SAGA] Dispatched email for {msg.AccountId}. Waiting for verification.");
                    // Return transition to save the current state and wait for the next event.
                    return context.Transition(state);
                },
                correlateBy: msg => msg.AccountId);

            sagas.On<AccountVerifiedEvent>(
                "account-events",
                async (state, msg, context) => 
                {
                    Console.WriteLine($"[SAGA] Received verification for {msg.AccountId}");
                    
                    state.VerificationReceived = true;
                    // Return complete to finalize the saga and clear its state.
                    return context.Complete();
                },
                correlateBy: msg => msg.AccountId);
        });
    }
}
