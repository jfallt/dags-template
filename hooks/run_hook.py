import argparse
import importlib.util
import os
import sys


def import_hook_from_file(module_name, hook_name):
    """Dynamically import a hook function from a Python file or module."""
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), module_name)

    try:
        # Handle the case when file_path is a directory (module)
        if os.path.isdir(file_path):
            # Get the module name from directory path
            module_name = os.path.basename(os.path.normpath(file_path))
            # Add the parent directory to sys.path
            parent_dir = os.path.dirname(file_path)
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            # Import the module
            hook_module = importlib.import_module(module_name)
        else:
            # Import from specific file
            spec = importlib.util.spec_from_file_location("hook_module", file_path)
            hook_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(hook_module)

        if hasattr(hook_module, hook_name):
            return getattr(hook_module, hook_name)
        else:
            print(f"Error: Function '{hook_name}' not found in {file_path}")
            return None
    except Exception as e:
        print(f"Error importing hook from {file_path}: {e!s}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Run a hook from a specified Python file"
    )
    parser.add_argument(
        "--hook",
        required=True,
        help="Name of the hook file or directory containing the hook",
    )
    parser.add_argument(
        "--function",
        required=False,
        help="Name of the hook function in the file",
        default="main",
    )
    parser.add_argument("filenames", nargs="*", help="Files to process with the hook")

    # Add any additional arguments that might be needed by specific hooks
    parser.add_argument(
        "--extra-args",
        nargs=argparse.REMAINDER,
        help="Additional arguments to pass to the hook",
    )

    args = parser.parse_args()

    hook_callable = import_hook_from_file(args.hook, args.function)

    if hook_callable is None:
        return 1

    if len(args.filenames) > 0:
        # Pass any extra arguments to the hook if it supports them
        if args.extra_args:
            output = hook_callable(args.filenames, *args.extra_args)
        else:
            output = hook_callable(args.filenames)

        if output and len(output) > 0:
            print(output)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
