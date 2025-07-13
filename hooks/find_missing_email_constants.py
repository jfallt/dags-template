import os
from config.constants.dist import internal, external
from email_to_constant.functions import (
    find_python_files,
    find_emails_in_file,
    email_to_constant_name,
    root_directory,
)


def find_missing_emails():
    """Find all emails in the dags directory that are not yet in the EMAIL_TO_CONSTANT mapping."""
    # Find all Python files in the dags directory
    dags_dir = os.path.join(root_directory, "dags")

    python_files = find_python_files(dags_dir, exclude_dirs=["__pycache__"])

    print(f"Scanning {len(python_files)} Python files for missing emails...")

    # Collect all internal and external emails separately
    internal_emails = set()
    external_emails = set()

    # Convert imported constants to dictionaries for case-insensitive comparison
    internal_dict = {
        email.lower(): constant
        for email, constant in vars(internal).items()
        if not email.startswith("__")
    }
    external_dict = {
        email.lower(): constant
        for email, constant in vars(external).items()
        if not email.startswith("__")
    }

    for file_path in python_files:
        emails_found = find_emails_in_file(file_path)
        for email in emails_found:
            if email.lower().endswith("@macquarie.com"):
                internal_emails.add(email)
            else:
                external_emails.add(email)

    # Find internal emails that are not in the constants
    missing_internal_emails = {
        email
        for email in internal_emails
        if email.lower() not in {v.lower(): v for k, v in internal_dict.items()}
    }

    # Find external emails that are not in the constants
    missing_external_emails = {
        email
        for email in external_emails
        if email.lower() not in {v.lower(): v for k, v in external_dict.items()}
    }

    # Report missing internal emails
    if missing_internal_emails:
        print(
            f"Found {len(missing_internal_emails)} internal emails not yet in constants:"
        )
        for email in sorted(missing_internal_emails):
            suggested_constant = email_to_constant_name(email)
            print(f"  - {email} (Suggested constant: {suggested_constant})")
    else:
        print("All found internal emails are already in the constants file.")

    # Report missing external emails
    if missing_external_emails:
        print(
            f"Found {len(missing_external_emails)} external emails not yet in constants:"
        )
        for email in sorted(missing_external_emails):
            # Extract domain and prepend it to the constant name
            domain = email.split("@")[-1].split(".")[0].upper()
            suggested_constant = f"{domain}_{email_to_constant_name(email)}"
            print(f"  - {email} (Suggested constant: {suggested_constant})")
    else:
        print("All found external emails are already in the constants file.")


find_missing_emails()
