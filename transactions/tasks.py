from django.utils import timezone
from celery import shared_task
from django.db import transaction
from accounts.models import UserBankAccount
from transactions.constants import INTEREST
from transactions.models import Transaction


@shared_task(name="calculate_interest")
def calculate_interest():
    """
    Task to calculate and apply interest to eligible user bank accounts.
    """
    # Fetch accounts eligible for interest calculation
    accounts = UserBankAccount.objects.filter(
        balance__gt=0,
        interest_start_date__lte=timezone.now(),  # Ensures eligibility based on start date
        initial_deposit_date__isnull=False        # Accounts must have an initial deposit
    ).select_related('account_type')  # Optimizes queries by prefetching related account type

    this_month = timezone.now().month  # Current month for interest calculation
    created_transactions = []
    updated_accounts = []

    # Use atomic transactions to ensure database consistency
    with transaction.atomic():
        for account in accounts:
            try:
                # Check if the current month is an interest calculation month for the account
                if this_month in account.get_interest_calculation_months():
                    # Calculate interest for the account
                    interest = account.account_type.calculate_interest(account.balance)

                    # Update the account balance
                    account.balance += interest

                    # Create a transaction object for the interest applied
                    created_transactions.append(
                        Transaction(
                            account=account,
                            transaction_type=INTEREST,
                            amount=interest
                        )
                    )
                    # Add the account to the list for bulk update
                    updated_accounts.append(account)

            except Exception as e:
                # Log errors for individual accounts (replace print with proper logging in production)
                print(f"Error processing account ID {account.id}: {e}")

        # Bulk create transactions if there are any
        if created_transactions:
            Transaction.objects.bulk_create(created_transactions)

        # Bulk update account balances if there are any updates
        if updated_accounts:
            UserBankAccount.objects.bulk_update(updated_accounts, ['balance'])
