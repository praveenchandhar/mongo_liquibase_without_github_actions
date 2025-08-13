#!/bin/bash

# MongoDB Atlas connection base (without database)
MONGO_CONNECTION_BASE="mongodb+srv://praveenchandharts:kixIUsDWGd3n6w5S@praveen-mongodb-github.lhhwdqa.mongodb.net"

# Define an associative array for databases and their corresponding contexts
declare -A DATABASE_CONTEXTS=(
    ["liquibase_test"]="liquibase_test"
    ["sample_mflix"]="sample_mflix"
)

# Validate input arguments
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <command> <database1,database2,...> [version]"
    echo ""
    echo "Examples:"
    echo "  $0 update liquibase_test                    # Uses latest changeset"
    echo "  $0 update liquibase_test version_4          # Uses specific version"
    echo "  $0 status liquibase_test                    # Status with latest"
    echo "  $0 status liquibase_test version_3          # Status with specific version"
    exit 1
fi

# Read command (status/update), database input, and optional version
command="$1"
raw_databases="$2"
specific_version="$3"  # Optional third parameter

# Validate the Liquibase command
if [[ "$command" != "status" && "$command" != "update" ]]; then
    echo "❌ Invalid command: $command."
    echo "Only 'status' or 'update' commands are allowed."
    exit 1
fi

# Setup CLASSPATH for Liquibase dependencies
CLASSPATH=$(find "$HOME/liquibase-jars" -name "*.jar" | tr '\n' ':')
export CLASSPATH

# Debug information
echo "🚀 Running Liquibase runner script..."
echo "📋 Command: $command"
echo "🗄️  Databases (raw input): $raw_databases"

if [ -n "$specific_version" ]; then
    echo "🎯 Specific version requested: $specific_version"
else
    echo "⏰ Using latest changeset (no version specified)"
fi

# Find and validate the changeset file
CHANGESETS_DIR="json_changesets"

echo ""
echo "=== Changeset File Discovery ==="

# Check if changesets directory exists
if [ ! -d "$CHANGESETS_DIR" ]; then
    echo "❌ Error: Directory '$CHANGESETS_DIR' does not exist!"
    echo "Please ensure changesets have been generated first."
    exit 1
fi

# List available files for debugging
echo "📁 Available changeset files:"
ls -la "$CHANGESETS_DIR"/*.xml 2>/dev/null || echo "No XML files found in $CHANGESETS_DIR"

# Determine which changeset file to use
if [ -n "$specific_version" ]; then
    # Use specific version if provided
    CHANGESET_FILE="$CHANGESETS_DIR/${specific_version}.xml"
    
    if [ ! -f "$CHANGESET_FILE" ]; then
        echo "❌ Error: Specific changeset file '$CHANGESET_FILE' not found!"
        echo ""
        echo "📋 Available versions:"
        find "$CHANGESETS_DIR" -name "*.xml" -type f -exec basename {} .xml \; 2>/dev/null | sort
        exit 1
    fi
    
    echo "🎯 Using specific changeset: $CHANGESET_FILE"
else
    # Get the latest XML file based on modification time
    CHANGESET_FILE=$(find "$CHANGESETS_DIR" -name "*.xml" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
    
    if [ -z "$CHANGESET_FILE" ]; then
        echo "❌ Error: No XML files found in '$CHANGESETS_DIR' directory!"
        echo ""
        echo "💡 Make sure you have:"
        echo "   1. Generated changesets using the GitHub workflow"
        echo "   2. Merged PRs with JS files in db_queries/"
        echo "   3. XML files are present in json_changesets/"
        exit 1
    fi
    
    echo "⏰ Using latest changeset: $CHANGESET_FILE"
fi

# Show file details
echo ""
echo "📄 Changeset file details:"
ls -la "$CHANGESET_FILE"

# Verify the changeset file is readable
if [ ! -r "$CHANGESET_FILE" ]; then
    echo "❌ Error: Cannot read changeset file '$CHANGESET_FILE'"
    exit 1
fi

# Show a preview of the changeset content
echo ""
echo "=== Changeset Preview ==="
echo "First 15 lines of the changeset:"
head -15 "$CHANGESET_FILE"
echo "========================="
echo ""

# Parse and validate databases
echo "=== Database Validation ==="

# Split and clean the database input
IFS=',' read -r -a database_array <<< "$raw_databases"
sanitized_databases=()
for db in "${database_array[@]}"; do
    db=$(echo "$db" | xargs) # Trim leading/trailing spaces
    if [[ -n "$db" ]]; then
        sanitized_databases+=("$db")
    fi
done

echo "🔍 Sanitized databases: ${sanitized_databases[*]}"

# Validate databases against the associative array
valid_databases=()
for db in "${sanitized_databases[@]}"; do
    if [[ -n "${DATABASE_CONTEXTS[$db]}" ]]; then
        valid_databases+=("$db")
        echo "✅ Valid database: '$db' → context: '${DATABASE_CONTEXTS[$db]}'"
    else
        echo "⚠️  Skipping invalid or unknown database: '$db'"
        echo "   Valid databases: ${!DATABASE_CONTEXTS[*]}"
    fi
done

# Ensure there are valid databases
if [[ ${#valid_databases[@]} -eq 0 ]]; then
    echo "❌ No valid databases provided. Exiting."
    echo ""
    echo "💡 Valid databases are: ${!DATABASE_CONTEXTS[*]}"
    exit 1
fi

echo ""
echo "=== Executing Liquibase Commands ==="

# Execute the Liquibase command for each valid database and context
for db in "${valid_databases[@]}"; do
    context="${DATABASE_CONTEXTS[$db]}"
    
    echo ""
    echo "🎯 Processing Database: $db"
    echo "   Context: $context"
    echo "   Changeset: $(basename "$CHANGESET_FILE")"
    echo "   Command: $command"
    echo ""
    
    # Run Liquibase command
    java -cp "$CLASSPATH" liquibase.integration.commandline.Main \
        --url="${MONGO_CONNECTION_BASE}/${db}?retryWrites=true&w=majority&tls=true" \
        --changeLogFile="$CHANGESET_FILE" \
        --contexts="$context" \
        --logLevel="info" \
        "$command"

    # Check if the command was successful
    exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        echo ""
        echo "✅ Liquibase '$command' for database '$db' executed successfully."
    else
        echo ""
        echo "❌ Liquibase failed for database '$db' with exit code: $exit_code"
        echo "Please check the logs above for details."
        exit $exit_code
    fi

    echo "------------------------------------------------------------"
done

echo ""
echo "🎉 Liquibase runner script completed successfully!"
echo "📊 Summary:"
echo "   - Command: $command"
echo "   - Databases processed: ${#valid_databases[@]}"
echo "   - Changeset used: $(basename "$CHANGESET_FILE")"
echo "   - All operations: ✅ SUCCESS"

exit 0
