import os
import re
import sys

# Add root directory to PATH in order to import project modules
root_directory = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(root_directory)


def find_python_files(directory, exclude_dirs=None):
    """Find all Python files in the directory and subdirectories."""
    if exclude_dirs is None:
        exclude_dirs = []

    python_files = []
    for root, dirs, files in os.walk(directory):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in exclude_dirs]

        for file in files:
            if file.endswith(".py"):
                python_files.append(os.path.join(root, file))

    return python_files


def find_emails_in_file(file_path):
    """Find all email addresses in a file."""
    with open(file_path) as f:
        content = f.read()

    # Find all strings that look like email addresses
    # This pattern matches common email formats inside quotes, including special characters like &
    pattern = r'["\']([a-zA-Z0-9_.+&-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)["\']'
    matches = re.findall(pattern, content)
    return matches


def email_to_constant_name(email):
    """Convert an email address to a suitable constant name."""
    # Remove the domain part
    local_part = email.split("@")[0]

    # Replace special characters with underscores
    cleaned = "".join(c if c.isalnum() else "_" for c in local_part)

    # Handle multiple consecutive underscores
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")

    # Remove leading/trailing underscores
    cleaned = cleaned.strip("_")

    # Convert to uppercase for constant naming convention
    return cleaned.upper()


def add_constant_to_file(email, constant_name, constants_file):
    """Add a new constant to the internal_distribution_lists module."""
    constants_file = os.path.join(
        root_directory, "constants", "dist", constants_file
    )

    # Read the current content of the file
    with open(constants_file) as f:
        content = f.read()

    # Check if the constant already exists
    pattern = rf"{re.escape(constant_name)}\s*=\s*['\"].*['\"]"
    if re.search(pattern, content):
        return  # Already exists

    # Add the new constant at the end of the file
    with open(constants_file, "a") as f:
        f.write(
            f'\n# Added automatically by replace_emails_with_constants.py\n{constant_name} = "{email}"\n'
        )

    print(f'Added new constant {constant_name} = "{email}" to constants file')


def replace_email_with_constant(file_path, email, constant, distribution_list=None):
    """Replace a specific email address with its constant reference."""
    with open(file_path) as f:
        content = f.read()

    # If distribution_list is provided, use module.CONSTANT format
    if distribution_list:
        prefix = f"{distribution_list}."
        constant_ref = f"{prefix}{constant}"
    else:
        constant_ref = constant

    # Replace the email string with the constant reference
    # We need to handle different patterns like '"email"' and "'email'"
    new_content = content.replace(f'"{email}"', constant_ref)
    new_content = new_content.replace(f"'{email}'", constant_ref)

    if new_content != content:
        with open(file_path, "w") as f:
            f.write(new_content)
        return True

    return False


def add_import_statement(file_path, distribution_list):
    """Add import statement for the module instead of individual constants."""
    with open(file_path) as f:
        content = f.read()

    # Check if there's already an import of the module
    import_pattern = (
        rf"import constants\.dist\.{distribution_list}(?:\s+as\s+[a-zA-Z_]+)?"
    )
    # Look for both standalone imports and comma-separated imports
    one_line_import_pattern = rf"import\s+.*constants\.dist\.{distribution_list}(?:\s+as\s+[a-zA-Z_]+)?.*"
    module_import_pattern = rf"from constants\.dist import {distribution_list}"

    # Check for existing import patterns
    module_import_match = re.search(module_import_pattern, content)
    import_match = re.search(import_pattern, content)
    one_line_match = re.search(one_line_import_pattern, content)

    if import_match or module_import_match or one_line_match:
        # Module already imported, nothing to do
        new_content = content
    else:
        # If there's no existing module import, add a new one
        import_statement = f"from constants.dist import {distribution_list}\n\n"

        # Check for sys.path.append(root_directory) or sys_path.append(root_directory) line
        path_append_match = re.search(r"sys[_.]path\.append\(root_directory\)", content)

        if path_append_match:
            # Add import after sys.path.append
            path_append_pos = path_append_match.end()
            new_content = (
                content[:path_append_pos]
                + "\n\n"
                + import_statement
                + content[path_append_pos:].lstrip("\n")
            )
        else:
            # If sys.path.append doesn't exist, add root_directory setup and then import
            ops_dir_setup = "\n# Add root directory to PATH in order to import project modules\nroot_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\nsys.path.append(root_directory)\n\n"

            # Find the last import statement
            last_import = re.search(r"^.*import .*$", content, re.MULTILINE)
            if last_import:
                last_import_pos = last_import.end()
                new_content = (
                    content[:last_import_pos]
                    + "\n\n"
                    + ops_dir_setup
                    + import_statement
                    + content[last_import_pos:].lstrip("\n")
                )
            else:
                # If no imports found, add at the beginning of the file
                new_content = ops_dir_setup + import_statement + content

    with open(file_path, "w") as f:
        f.write(new_content)


def check_constants_in_file(constants_dict, constants_file):
    """Check if the values from a constants dict are present in the specified file."""
    constants_file_path = os.path.join(
        root_directory, "constants", "dist", constants_file
    )

    # Read the constants file content
    with open(constants_file_path) as f:
        content = f.read()

    # Check each constant value
    missing_constants = {}
    for email, constant_name in constants_dict.items():
        # Check if the constant is properly defined (name = "value")
        pattern = rf"{re.escape(constant_name)}\s*=\s*['\"].*['\"]"
        if not re.search(pattern, content):
            missing_constants[email] = constant_name

    return missing_constants
