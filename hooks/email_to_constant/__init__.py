import os
import sys
from hooks.email_to_constant.functions import (
    find_python_files,
    find_emails_in_file,
    email_to_constant_name,
    add_constant_to_file,
    add_import_statement,
    replace_email_with_constant,
)
from constants.dist import internal, external

# Add root directory to PATH in order to import project modules
root_directory = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(root_directory)

DOMAIN = "@company.com"

def main(python_files=None):
    if not python_files:
        # Find all Python files in the dags directory
        dags_dir = os.path.join(ops_directory, "dags")
        python_files = find_python_files(dags_dir, exclude_dirs=["__pycache__"])
    else:
        # Ensure the provided list is a list of Python files in the dags directory
        python_files = [f for f in python_files if f.endswith(".py") and "dags/" in f]

    files_modified = 0
    internal_new_constants_added = 0
    external_new_constants_added = 0

    # Find all email addresses in Python files
    all_emails = set()
    for file_path in python_files:
        emails_found = find_emails_in_file(file_path)
        all_emails.update(emails_found)

    # Create lowercase lookup dictionaries where the constant name is the key and email is the value
    internal_emails = {
        v.lower(): k for k, v in vars(internal).items() if not k.startswith("__")
    }
    external_emails = {
        v.lower(): k for k, v in vars(external).items() if not k.startswith("__")
    }

    # Add any new emails to the constants file and update the distribution lists dicts
    for email in all_emails:
        if (
            email.lower().endswith(DOMAIN)
            and email.lower() not in internal_emails
        ):
            constant_name = email_to_constant_name(email)

            # Make sure we don't have duplicate constant names
            base_name = constant_name
            counter = 1
            while any(constant_name == v for v in internal_emails.values()):
                constant_name = f"{base_name}_{counter}"
                counter += 1

            add_constant_to_file(email, constant_name, "internal.py")
            internal_emails[email] = constant_name
            internal_new_constants_added += 1
        elif (
            not email.lower().endswith(DOMAIN)
            and email.lower() not in external_emails
        ):
            constant_name = email_to_constant_name(email)

            # Make sure we don't have duplicate constant names
            base_name = constant_name
            counter = 1
            while any(constant_name == v for v in external_emails.values()):
                constant_name = f"{base_name}_{counter}"
                counter += 1

            add_constant_to_file(email, constant_name, "external.py")
            external_emails[email] = constant_name
            external_new_constants_added += 1

    # Second pass: replace emails with constants
    for file_path in python_files:
        emails_found = find_emails_in_file(file_path)
        file_modified_internal = False
        file_modified_external = False

        for email in emails_found:
            # Case-insensitive lookup in internal distribution lists
            email_lower = email.lower()

            if email_lower in internal_emails:
                constant_name = internal_emails[email_lower]
                if replace_email_with_constant(
                    file_path, email, constant_name, "internal"
                ):
                    file_modified_internal = True
            elif email_lower in external_emails:
                constant_name = external_emails[email_lower]
                if replace_email_with_constant(
                    file_path, email, constant_name, "external"
                ):
                    file_modified_external = True

        file_modified = file_modified_internal or file_modified_external
        if file_modified:
            if file_modified_internal:
                add_import_statement(
                    file_path,
                    "internal",
                )
            if file_modified_external:
                add_import_statement(
                    file_path,
                    "external",
                )
            files_modified += 1
            print(f"Updated {file_path}")

    print(f"Modified {files_modified} files")
    print(
        f"Added {internal_new_constants_added} new constants to internal.py"
    )
    print(
        f"Added {external_new_constants_added} new constants to external.py"
    )
