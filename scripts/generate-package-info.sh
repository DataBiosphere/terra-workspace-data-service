#!/bin/bash

# Initialize variables
START_DIR=""
FORCE=0
VERBOSE=0

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -f|--force) FORCE=1; shift ;;
        -v|--verbose) VERBOSE=1; shift ;;
        *) if [[ -z "$START_DIR" ]]; then START_DIR="$1"; else echo "Unknown parameter passed: $1"; exit 1; fi; shift ;;
    esac
done

# Check if the starting directory is provided
if [[ -z "$START_DIR" ]]; then
    echo "Usage: $0 <starting-directory> [-f|--force] [-v|--verbose]"
    exit 1
fi

# Convert absolute path to relative path based on starting directory
cd "$START_DIR" || exit
START_DIR_ABS=$(pwd)


# Function to generate package-info.java
generate_package_info() {
    local target_dir=$1
    local package_name=$2
    local force=$3
    local verbose=$4

    # Ensure target directory exists (sanity check, though not strictly necessary)
    mkdir -p "$target_dir"

    # Define the path for the package-info.java file
    local package_info_path="${target_dir}/package-info.java"

    # Check if package-info.java exists and if force is not enabled, skip
    if [[ -f "$package_info_path" && $force -eq 0 ]]; then
      if [[ $verbose -eq 1 ]]; then
          echo "package-info.java already exists in $target_dir, skipping..."
      fi
      return
    fi

    # Generate package-info.java content
    {
      echo "@NonNullApi"
      echo "@NonNullFields"
      echo "package $package_name;"
      echo ""
      echo "import org.springframework.lang.NonNullApi;"
      echo "import org.springframework.lang.NonNullFields;"
    } > "$package_info_path"
    echo "Generated package-info.java for package $package_name in $target_dir"
}

# Find directories containing .java files, generate package-info.java
find . -type f -name "*.java" | sed 's|/[^/]*$||' | sort -u | while read -r dir; do
    # Skip if no directory (current directory case)
    [[ -z "$dir" || "$dir" == "." ]] && continue

    # Remove leading './' using Bash parameter expansion
    clean_dir=${dir#./}

    # Convert directory to package name using Bash parameter expansion
    package_name=${clean_dir//\//.}

    # Skip directories whose package name includes 'generated'
    if [[ $package_name == *"generated"* ]]; then
        [[ $VERBOSE -eq 1 ]] && echo "Skipping generated package: $package_name"
        continue
    fi

    # Call function to generate package-info.java
    generate_package_info "$START_DIR_ABS/$clean_dir" "$package_name" "$FORCE" "$VERBOSE"
done

echo "Completed generating package-info.java files."
