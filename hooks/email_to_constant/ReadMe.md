# Email To Constant Pre-commit Hook

## Overview

This hook provides automated functionality to rep## Example Usage

Instead of:
```python
email_op = EmailOperator(
    to=["team@domain.com", "team@external_domain.com"],
    subject="Report Ready"
)
```

Use:
```python
import constants.dist.internal as internal
import constants.dist.external as external

email_op = EmailOperator(
    to=[internal.TEAM, external.EXTERNAL_DOMAIN_TEAM],
    subject="Report Ready"
)
``` addresses with constants throughout Airflow DAG files. The tool scans Python files, identifies email addresses, and replaces them with references to constants defined in centralized configuration files.

## Email Constants Management

### Purpose

Using email constants instead of hardcoded email addresses provides several benefits:
- **Maintainability**: When an email address changes, it can be updated in a single location
- **Consistency**: Ensures the same email address is used everywhere it's needed
- **Clarity**: Makes it clear what the purpose of each email address is through the constant name
- **Error reduction**: Prevents typos in email addresses

### Implementation

The system uses centralized email constants defined in:
```
/constants/distribution_lists/internal.py
/constants/distribution_lists/external.py
```

These files contain named constants for all email addresses used throughout the DAGs:

```python
# Internal email constants
SUPPORT_INBOX = "IT-InvestmentSystemsSupport@macquarie.com"
OPERATIONS_PLATFORM_US = "COGTechMAMOperationsPlatformUS@macquarie.com"
VEHICLES_DEVELOPMENT_INBOX = "COGTechOPSAirflowTeamPHL@macquarie.com"
# ... and many more ...

# External email constants
STATE_STREET_DERIVATIVES_RECON = "DerivativesRecon@statestreet.com"
BNY_TRADE_TEAM = "TradeTeam@bnymellon.com" 
```

### How the Tool Works

The email replacement tool performs the following operations:

1. **Discovery**: Scans all Python files in the DAGs directory to find hardcoded email addresses using regex pattern matching
2. **Classification**: Categorizes emails as internal (@macquarie.com) or external (other domains)
3. **Constant Creation**: Generates appropriate constant names for emails not yet in constants files using naming conventions
4. **Constants File Update**: Adds new constants to the appropriate distribution lists file with comments indicating automated addition
5. **Code Replacement**: Replaces hardcoded emails with their constant references in all relevant files
6. **Import Insertion**: Adds proper import statements for the constants used, preserving existing imports

## Implementation Details

### Core Components

The email replacement system consists of several key components:

1. **Email Classification**:
   - Internal emails (@macquarie.com) are stored in `internal_distribution_lists.py`
   - External emails (e.g., @statestreet.com, @bnymellon.com) are stored in `external_distribution_lists.py`
   - Emails are organized by team or purpose (OUR_TEAM_EMAILS, REPORT_DISTRIBUTION_EMAILS, etc.)

2. **Email Detection**:
   - Uses regex pattern matching to find email addresses in quotes within DAG files
   - Identifies both single and double quoted email strings

3. **Constant Naming**:
   - Converts email addresses to uppercase constant names
   - Replaces special characters with underscores
   - Adds domain-specific prefixes for external emails (STATE_STREET_, BNY_)

4. **Code Transformation**:
   - Adds appropriate imports to the top of each modified file
   - Replaces quoted email strings with constant references
   - Preserves file formatting and structure

### Running the Hook

To execute the email replacement across all DAGs:

```bash
python -m hooks.email_to_constant
```

To run it on specific files:

```bash
python -m hooks.email_to_constant path/to/file1.py path/to/file2.py
```

## Best Practices

When working with emails in DAG files:

1. **Never hardcode email addresses** in DAG files
2. **Always import** the appropriate module:
   - `import constants.dist.internal as internal` for internal emails
   - `import constants.dist.external as external` for external emails
3. **Use module-prefixed constants** (e.g., `internal.SUPPORT_INBOX`, `external.BNY_TRADE_TEAM`)
4. **Run the email replacement hook** if you need to convert existing hardcoded emails

## Maintenance

When email addresses change:
1. Update only the constant value in the appropriate distribution lists file
2. No changes needed in any DAG files

This ensures that all DAGs will automatically use the updated email address.
