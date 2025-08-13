import os
import re
import argparse
import json
from github import Github
from datetime import datetime

def parse_js_file(js_file_path):
    """Parse MongoDB queries from a .js file."""
    if not os.path.exists(js_file_path):
        raise FileNotFoundError(f"JS file not found: {js_file_path}")
    
    with open(js_file_path, "r", encoding="utf-8") as file:
        content = file.read()
        print(f"DEBUG: JS file content ({len(content)} characters):")
        print("=" * 50)
        print(content)
        print("=" * 50)
        return content

def extract_context_from_content(content):
    """Extract context from the top of the JS file."""
    lines = content.split('\n')[:10]
    first_lines = '\n'.join(lines)
    
    print("DEBUG: Looking for context in first 10 lines:")
    print("=" * 30)
    print(first_lines)
    print("=" * 30)
    
    context_patterns = [
        r'//\s*@?context\s*:?\s*([a-zA-Z0-9_]+)',
        r'/\*\s*@?context\s*:?\s*([a-zA-Z0-9_]+)\s*\*/',
        r'//\s*@?Context\s*:?\s*([a-zA-Z0-9_]+)',
        r'/\*\s*@?Context\s*:?\s*([a-zA-Z0-9_]+)\s*\*/',
        r'//\s*DATABASE\s*:?\s*([a-zA-Z0-9_]+)',
        r'/\*\s*DATABASE\s*:?\s*([a-zA-Z0-9_]+)\s*\*/',
    ]
    
    for pattern in context_patterns:
        match = re.search(pattern, first_lines, re.IGNORECASE)
        if match:
            context = match.group(1)
            print(f"DEBUG: Found context: '{context}' using pattern: {pattern}")
            return context
    
    print("DEBUG: No context found in file, using default 'liquibase_test'")
    return "liquibase_test"

def validate_and_clean_json(json_str):
    """Validate and clean JSON, converting problematic date formats."""
    if not json_str:
        return "{}"
    
    # Clean the JSON string
    cleaned = json_str.strip()
    
    print(f"DEBUG: Original JSON snippet: {cleaned[:100]}...")
    
    # Replace common problematic patterns
    replacements = [
        # Fix new Date() calls to ISODate() - Various formats
        (r'new\s+Date\s*\(\s*"([^"]+)"\s*\)', r'ISODate("\1")'),
        (r'new\s+Date\s*\(\s*\'([^\']+)\'\s*\)', r'ISODate("\1")'),
        (r'new\s+Date\s*\(\s*\)', r'ISODate()'),
        
        # Handle common date format issues
        (r'ISODate\s*\(\s*"(\d{4}-\d{2}-\d{2})"\s*\)', r'ISODate("\1T00:00:00.000Z")'),
        
        # Clean up whitespace and formatting
        (r'\s+', ' '),  # Multiple spaces to single space
        (r'\s*,\s*', ', '),  # Clean comma spacing
        (r'\s*:\s*', ': '),  # Clean colon spacing
        
        # Ensure proper quote usage for MongoDB
        (r"'([^']*)'(\s*:)", r'"\1"\2'),  # Convert single quotes to double quotes for keys
    ]
    
    for pattern, replacement in replacements:
        before = cleaned
        cleaned = re.sub(pattern, replacement, cleaned)
        if before != cleaned:
            print(f"DEBUG: Applied replacement: {pattern[:50]}...")
    
    print(f"DEBUG: Cleaned JSON snippet: {cleaned[:100]}...")
    return cleaned

def validate_query_syntax(operation):
    """Validate individual query syntax and structure."""
    errors = []
    warnings = []
    
    # Check for required fields
    if 'collection' not in operation or not operation['collection']:
        errors.append("Missing collection name")
        return errors, warnings
    
    # Validate collection name format
    collection_name = operation['collection']
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', collection_name):
        errors.append(f"Invalid collection name format: '{collection_name}'. Use alphanumeric and underscore only.")
    
    # Check operation type
    op_type = operation.get('type', '')
    if not op_type:
        errors.append("Missing operation type")
        return errors, warnings
    
    # Validate based on operation type
    if op_type in ['insertMany', 'insertOne', 'insert']:
        if 'documents' not in operation:
            errors.append(f"{op_type} operation missing documents")
        else:
            doc_str = operation['documents']
            
            # Check for problematic date formats
            if 'new Date(' in doc_str and 'ISODate(' not in doc_str:
                warnings.append("Found 'new Date()' - converting to 'ISODate()' format")
            
            # Check for single quotes in JSON (MongoDB prefers double quotes)
            if re.search(r"'[^']*'\s*:", doc_str):
                warnings.append("Found single quotes for object keys - converting to double quotes")
            
    elif op_type in ['updateOne', 'updateMany', 'replaceOne']:
        if 'filter' not in operation:
            errors.append(f"{op_type} operation missing filter")
        if 'update' not in operation:
            errors.append(f"{op_type} operation missing update document")
            
    elif op_type in ['deleteOne', 'deleteMany', 'remove']:
        if 'filter' not in operation:
            errors.append(f"{op_type} operation missing filter")
    
    # Check for potentially unsafe operations
    for field in ['documents', 'filter', 'update']:
        if field in operation:
            content = operation[field]
            
            # Check for JavaScript functions (not supported in Liquibase)
            if re.search(r'function\s*\(', content):
                errors.append(f"JavaScript functions not supported in {field}")
            
            # Check for eval or other dangerous operations
            if 'eval(' in content or '$where' in content:
                warnings.append(f"Potentially unsafe operation found in {field}")
    
    return errors, warnings

def validate_file_header(content):
    """Validate that the file follows the template structure."""
    lines = content.split('\n')[:15]  # Check first 15 lines
    header_content = '\n'.join(lines)
    
    errors = []
    warnings = []
    
    # Check for required header fields
    required_patterns = {
        'context': r'//\s*@?context\s*:',
        'author': r'//\s*@?author\s*:',
        'description': r'//\s*@?description\s*:',
        'version': r'//\s*@?version\s*:'
    }
    
    for field, pattern in required_patterns.items():
        if not re.search(pattern, header_content, re.IGNORECASE):
            warnings.append(f"Missing recommended header field: @{field}")
    
    return errors, warnings

def extract_mongodb_operations_robust(content):
    """Enhanced operation extraction with comprehensive validation."""
    operations = []
    all_errors = []
    all_warnings = []
    
    print("DEBUG: Starting robust MongoDB operation extraction...")
    
    # Validate file header
    header_errors, header_warnings = validate_file_header(content)
    all_errors.extend([f"Header: {e}" for e in header_errors])
    all_warnings.extend([f"Header: {w}" for w in header_warnings])
    
    # Remove comments first
    content_no_comments = re.sub(r'//.*?$', '', content, flags=re.MULTILINE)
    content_no_comments = re.sub(r'/\*.*?\*/', '', content_no_comments, flags=re.DOTALL)
    
    patterns = {
        # Insert operations
        'insertMany': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.insertMany\s*\(\s*(\[.*?\])\s*\)\s*;?',
        'insertOne': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.insertOne\s*\(\s*(\{.*?\})\s*\)\s*;?',
        'insert': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.insert\s*\(\s*(\{.*?\}|\[.*?\])\s*\)\s*;?',
        
        # Update operations  
        'updateOne': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.updateOne\s*\(\s*(\{.*?\})\s*,\s*(\{.*?\})\s*(?:,\s*(\{.*?\}))?\s*\)\s*;?',
        'updateMany': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.updateMany\s*\(\s*(\{.*?\})\s*,\s*(\{.*?\})\s*(?:,\s*(\{.*?\}))?\s*\)\s*;?',
        'replaceOne': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.replaceOne\s*\(\s*(\{.*?\})\s*,\s*(\{.*?\})\s*(?:,\s*(\{.*?\}))?\s*\)\s*;?',
        
        # Delete operations
        'deleteOne': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.deleteOne\s*\(\s*(\{.*?\})\s*(?:,\s*(\{.*?\}))?\s*\)\s*;?',
        'deleteMany': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.deleteMany\s*\(\s*(\{.*?\})\s*(?:,\s*(\{.*?\}))?\s*\)\s*;?',
        'remove': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.remove\s*\(\s*(\{.*?\})\s*(?:,\s*(\{.*?\}))?\s*\)\s*;?',
        
        # Index operations
        'createIndex': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.createIndex\s*\(\s*(\{.*?\})\s*(?:,\s*(\{.*?\}))?\s*\)\s*;?',
        'dropIndex': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.dropIndex\s*\(\s*(["\'][^"\']*["\']|\{.*?\})\s*\)\s*;?',
        
        # Collection operations
        'createCollection': r'db\.createCollection\s*\(\s*["\']([^"\']+)["\']\s*(?:,\s*(\{.*?\}))?\s*\)\s*;?',
        'dropCollection_direct': r'db\.dropCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*;?',
        'dropCollection_getCollection': r'db\.getCollection\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\.drop\s*\(\s*\)\s*;?',
        'dropCollection_dot': r'db\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\.drop\s*\(\s*\)\s*;?',
    }
    
    # Check for unsupported patterns
    unsupported_patterns = [
        (r'db\.[a-zA-Z_][a-zA-Z0-9_]*\.(?!drop\(\))', "Use db.getCollection('name') instead of db.collection"),
        (r'\.find\s*\(', "find() operations not supported in Liquibase"),
        (r'\.aggregate\s*\(', "aggregate() operations not supported in Liquibase"),
        (r'\.mapReduce\s*\(', "mapReduce() operations not supported in Liquibase"),
        (r'\.distinct\s*\(', "distinct() operations not supported in Liquibase"),
    ]
    
    for pattern, message in unsupported_patterns:
        if re.search(pattern, content_no_comments):
            all_errors.append(f"Unsupported operation: {message}")
    
    # Extract operations
    for operation_type, pattern in patterns.items():
        for match in re.finditer(pattern, content_no_comments, re.DOTALL):
            groups = match.groups()
            
            operation = {
                'type': operation_type,
                'collection': groups[0],
                'raw_match': match.group(0),
                'line_number': content[:match.start()].count('\n') + 1
            }
            
            # Handle different parameter structures based on operation type
            if operation_type in ['insertMany', 'insertOne', 'insert']:
                operation['documents'] = groups[1]
            elif operation_type in ['updateOne', 'updateMany', 'replaceOne']:
                operation['filter'] = groups[1]
                operation['update'] = groups[2]
                operation['options'] = groups[3] if len(groups) > 3 and groups[3] else None
            elif operation_type in ['deleteOne', 'deleteMany', 'remove']:
                operation['filter'] = groups[1]
                operation['options'] = groups[2] if len(groups) > 2 and groups[2] else None
            elif operation_type == 'createIndex':
                operation['index_key'] = groups[1]
                operation['options'] = groups[2] if len(groups) > 2 and groups[2] else None
            elif operation_type == 'dropIndex':
                operation['index_spec'] = groups[1]
            elif operation_type == 'createCollection':
                operation['options'] = groups[1] if len(groups) > 1 and groups[1] else None
            elif operation_type in ['dropCollection_direct', 'dropCollection_getCollection', 'dropCollection_dot']:
                operation['type'] = 'dropCollection'
                operation['collection'] = groups[0]
            
            # Validate and clean the operation
            op_errors, op_warnings = validate_query_syntax(operation)
            
            if op_errors:
                all_errors.extend([f"Operation {len(operations)+1} (line {operation['line_number']}): {e}" for e in op_errors])
                print(f"DEBUG: Skipping invalid operation due to errors: {op_errors}")
                continue
            
            if op_warnings:
                all_warnings.extend([f"Operation {len(operations)+1} (line {operation['line_number']}): {w}" for w in op_warnings])
            
            # Clean JSON content
            if 'documents' in operation:
                operation['documents'] = validate_and_clean_json(operation['documents'])
            if 'filter' in operation:
                operation['filter'] = validate_and_clean_json(operation['filter'])
            if 'update' in operation:
                operation['update'] = validate_and_clean_json(operation['update'])
            
            operations.append(operation)
            print(f"DEBUG: Found {operation['type']} operation on collection '{operation['collection']}' at line {operation['line_number']}")
    
    print(f"DEBUG: Total operations found: {len(operations)}")
    print(f"DEBUG: Total errors: {len(all_errors)}")
    print(f"DEBUG: Total warnings: {len(all_warnings)}")
    
    if all_errors:
        print("ERRORS:")
        for error in all_errors:
            print(f"  ❌ {error}")
    
    if all_warnings:
        print("WARNINGS:")
        for warning in all_warnings:
            print(f"  ⚠️ {warning}")
    
    return operations, all_errors, all_warnings

def clean_json_for_xml(json_str):
    """Clean and format JSON for XML inclusion."""
    if not json_str:
        return "{}"
    return json_str.strip()

def extract_version_number(version_string):
    """Extract numeric part from version string."""
    match = re.search(r'(\d+)', version_string)
    if match:
        return match.group(1)
    return "1"

def extract_index_name(options_str):
    """Extract index name from options string."""
    if not options_str:
        return None
    name_match = re.search(r'["\']?name["\']?\s*:\s*["\']([^"\']+)["\']', options_str)
    return name_match.group(1) if name_match else None

def generate_validation_report(errors, warnings):
    """Generate a human-readable validation report."""
    report = []
    
    if errors:
        report.append("🚨 CRITICAL ERRORS FOUND:")
        report.append("=" * 50)
        for i, error in enumerate(errors, 1):
            report.append(f"{i}. {error}")
        report.append("")
        report.append("❌ Liquibase XML generation FAILED due to above errors.")
        report.append("Please fix these issues and try again.")
        report.append("")
    
    if warnings:
        report.append("⚠️ WARNINGS:")
        report.append("=" * 50)
        for i, warning in enumerate(warnings, 1):
            report.append(f"{i}. {warning}")
        report.append("")
        report.append("✅ Liquibase XML generated successfully, but please review warnings above.")
        report.append("")
    
    if not errors and not warnings:
        report.append("✅ ALL VALIDATIONS PASSED!")
        report.append("Your MongoDB queries follow best practices.")
        report.append("")
    
    return "\n".join(report)

def generate_liquibase_xml_robust(version, operations, author_name, context, errors, warnings):
    """Generate Liquibase XML with enhanced error handling and validation report."""
    
    base_version_num = extract_version_number(version)
    
    xml_lines = []
    xml_lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    xml_lines.append('<databaseChangeLog')
    xml_lines.append('    xmlns="http://www.liquibase.org/xml/ns/dbchangelog"')
    xml_lines.append('    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"')
    xml_lines.append('    xmlns:mongodb="http://www.liquibase.org/xml/ns/dbchangelog-ext"')
    xml_lines.append('    xsi:schemaLocation="')
    xml_lines.append('        http://www.liquibase.org/xml/ns/dbchangelog')
    xml_lines.append('        http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-4.5.xsd')
    xml_lines.append('        http://www.liquibase.org/xml/ns/dbchangelog-ext')
    xml_lines.append('        http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-ext.xsd">')
    
    # Add validation report as comments
    if errors or warnings:
        xml_lines.append('    <!-- VALIDATION REPORT -->')
        validation_report = generate_validation_report(errors, warnings)
        for line in validation_report.split('\n'):
            if line.strip():
                xml_lines.append(f'    <!-- {line} -->')
        xml_lines.append('    <!-- END VALIDATION REPORT -->')
        xml_lines.append('')

    if not operations:
        xml_lines.append(f'    <changeSet id="{base_version_num}" author="{author_name}" context="{context}">')
        xml_lines.append('        <!-- No valid MongoDB operations found in the JS file -->')
        xml_lines.append('    </changeSet>')
    else:
        # Create separate changeSet for each operation
        for i, operation in enumerate(operations):
            op_type = operation['type']
            collection = operation['collection']
            
            changeset_id = base_version_num if len(operations) == 1 else f"{base_version_num}.{i+1}"
            
            xml_lines.append(f'    <changeSet id="{changeset_id}" author="{author_name}" context="{context}">')
            xml_lines.append(f'        <!-- {op_type.upper()} operation on {collection} (from line {operation.get("line_number", "unknown")}) -->')
            
            try:
                if op_type == 'createCollection':
                    xml_lines.append(f'        <mongodb:createCollection collectionName="{collection}" />')
                    
                elif op_type == 'createIndex':
                    index_key = clean_json_for_xml(operation['index_key'])
                    index_name = extract_index_name(operation.get('options', '')) or f"{collection}_index_{i+1}"
                    
                    xml_lines.append('        <mongodb:runCommand>')
                    xml_lines.append('            <mongodb:command><![CDATA[')
                    xml_lines.append('            {')
                    xml_lines.append(f'                "createIndexes": "{collection}",')
                    xml_lines.append('                "indexes": [')
                    xml_lines.append('                    {')
                    xml_lines.append(f'                        "key": {index_key},')
                    xml_lines.append(f'                        "name": "{index_name}"')
                    xml_lines.append('                    }')
                    xml_lines.append('                ]')
                    xml_lines.append('            }')
                    xml_lines.append('            ]]></mongodb:command>')
                    xml_lines.append('        </mongodb:runCommand>')
                    
                elif op_type == 'insertOne':
                    doc_content = clean_json_for_xml(operation['documents'])
                    xml_lines.append(f'        <mongodb:insertOne collectionName="{collection}">')
                    xml_lines.append('            <mongodb:document><![CDATA[')
                    xml_lines.append(f'            {doc_content}')
                    xml_lines.append('            ]]></mongodb:document>')
                    xml_lines.append('        </mongodb:insertOne>')
                    
                elif op_type in ['insertMany', 'insert']:
                    docs_content = clean_json_for_xml(operation['documents'])
                    if not docs_content.strip().startswith('['):
                        docs_content = f"[{docs_content}]"
                    
                    xml_lines.append(f'        <mongodb:insertMany collectionName="{collection}">')
                    xml_lines.append('            <mongodb:documents><![CDATA[')
                    xml_lines.append(f'            {docs_content}')
                    xml_lines.append('            ]]></mongodb:documents>')
                    xml_lines.append('        </mongodb:insertMany>')
                    
                elif op_type in ['updateOne', 'updateMany']:
                    filter_json = clean_json_for_xml(operation['filter'])
                    update_json = clean_json_for_xml(operation['update'])
                    multi = "true" if op_type == "updateMany" else "false"
                    
                    xml_lines.append('        <mongodb:runCommand>')
                    xml_lines.append('            <mongodb:command><![CDATA[')
                    xml_lines.append('            {')
                    xml_lines.append(f'                "update": "{collection}",')
                    xml_lines.append('                "updates": [')
                    xml_lines.append('                    {')
                    xml_lines.append(f'                        "q": {filter_json},')
                    xml_lines.append(f'                        "u": {update_json},')
                    xml_lines.append(f'                        "multi": {multi}')
                    xml_lines.append('                    }')
                    xml_lines.append('                ]')
                    xml_lines.append('            }')
                    xml_lines.append('            ]]></mongodb:command>')
                    xml_lines.append('        </mongodb:runCommand>')
                    
                elif op_type == 'replaceOne':
                    filter_json = clean_json_for_xml(operation['filter'])
                    replacement_json = clean_json_for_xml(operation['update'])
                    
                    xml_lines.append('        <mongodb:runCommand>')
                    xml_lines.append('            <mongodb:command><![CDATA[')
                    xml_lines.append('            {')
                    xml_lines.append(f'                "findAndModify": "{collection}",')
                    xml_lines.append(f'                "query": {filter_json},')
                    xml_lines.append(f'                "update": {replacement_json},')
                    xml_lines.append('                "new": true')
                    xml_lines.append('            }')
                    xml_lines.append('            ]]></mongodb:command>')
                    xml_lines.append('        </mongodb:runCommand>')
                    
                elif op_type in ['deleteOne', 'deleteMany', 'remove']:
                    filter_json = clean_json_for_xml(operation['filter'])
                    limit = 1 if op_type == "deleteOne" else 0
                    
                    xml_lines.append('        <mongodb:runCommand>')
                    xml_lines.append('            <mongodb:command><![CDATA[')
                    xml_lines.append('            {')
                    xml_lines.append(f'                "delete": "{collection}",')
                    xml_lines.append('                "deletes": [')
                    xml_lines.append('                    {')
                    xml_lines.append(f'                        "q": {filter_json},')
                    xml_lines.append(f'                        "limit": {limit}')
                    xml_lines.append('                    }')
                    xml_lines.append('                ]')
                    xml_lines.append('            }')
                    xml_lines.append('            ]]></mongodb:command>')
                    xml_lines.append('        </mongodb:runCommand>')
                    
                elif op_type == 'dropIndex':
                    index_spec = operation['index_spec']
                    if index_spec.startswith('"') or index_spec.startswith("'"):
                        index_name = index_spec.strip('"\'')
                        xml_lines.append(f'        <mongodb:dropIndex collectionName="{collection}" indexName="{index_name}" />')
                    else:
                        xml_lines.append(f'        <mongodb:dropIndex collectionName="{collection}">')
                        xml_lines.append('            <mongodb:keys><![CDATA[')
                        xml_lines.append(f'            {clean_json_for_xml(index_spec)}')
                        xml_lines.append('            ]]></mongodb:keys>')
                        xml_lines.append('        </mongodb:dropIndex>')
                    
                elif op_type == 'dropCollection':
                    xml_lines.append(f'        <mongodb:dropCollection collectionName="{collection}" />')
                    
            except Exception as e:
                print(f"DEBUG: Error processing operation {i+1}: {str(e)}")
                xml_lines.append(f'        <!-- Failed to process {op_type} operation: {str(e)} -->')
                xml_lines.append(f'        <!-- Raw operation: {operation.get("raw_match", "")[:100]}... -->')
            
            xml_lines.append('    </changeSet>')

    xml_lines.append('</databaseChangeLog>')
    return '\n'.join(xml_lines)

def write_to_file(xml_content, output_file_path):
    """Write XML content to a file."""
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    
    with open(output_file_path, "w", encoding="utf-8") as file:
        file.write(xml_content)

def create_pull_request(repo_name, branch_name, changeset_file_path, js_file_path, github_token):
    """Create a GitHub Pull Request with the newly generated XML file."""
    try:
        g = Github(github_token)
        repo = g.get_repo(repo_name)

        with open(changeset_file_path, "r", encoding="utf-8") as file:
            changeset_content = file.read()

        # Check if branch already exists
        try:
            existing_branch = repo.get_branch(branch_name)
            print(f"Branch {branch_name} already exists, deleting it first...")
            ref = repo.get_git_ref(f"heads/{branch_name}")
            ref.delete()
        except:
            pass

        # Create a new branch
        main_branch = repo.get_branch("main")
        ref = repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=main_branch.commit.sha)

        # Add / commit the file to the branch
        file_path_in_repo = f"json_changesets/{os.path.basename(changeset_file_path)}"
        
        try:
            existing_file = repo.get_contents(file_path_in_repo, ref=branch_name)
            repo.update_file(
                path=file_path_in_repo,
                message=f"Updated {os.path.basename(changeset_file_path)} for {os.path.basename(js_file_path)}",
                content=changeset_content,
                sha=existing_file.sha,
                branch=branch_name
            )
        except:
            repo.create_file(
                path=file_path_in_repo,
                message=f"Generated {os.path.basename(changeset_file_path)} for {os.path.basename(js_file_path)}",
                content=changeset_content,
                branch=branch_name
            )

        # Create PR
        pr = repo.create_pull(
            title=f"[Auto-Generated] XML Changeset for {os.path.basename(js_file_path)}",
            body=(
                f"This PR was auto-generated from `{os.path.basename(js_file_path)}`.\n\n"
                f"- Generated XML: `{file_path_in_repo}`\n"
                f"- Source JS: `{js_file_path}`\n\n"
                f"Please review the generated changeset and merge if correct.\n\n"
                f"### Generated XML Preview:\n"
                f"```xml\n{changeset_content}\n```"
            ),
            head=branch_name,
            base="main"
        )

        return pr
    
    except Exception as e:
        print(f"Error creating pull request: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Liquibase XML with enhanced validation and error handling.")
    parser.add_argument("--js_file", required=True, help="Path to the .js file.")
    parser.add_argument("--version", required=True, help="Version for the XML changeset.")
    parser.add_argument("--author", required=True, help="Author for the changeset.")
    parser.add_argument("--repo", required=True, help="GitHub repository (e.g., 'owner/repo').")
    parser.add_argument("--branch", required=True, help="Target branch for the PR.")
    parser.add_argument("--token", required=True, help="GitHub token for authentication.")
    parser.add_argument("--fail-on-warnings", action="store_true", help="Fail if warnings are found.")
    parser.add_argument("--skip-pr", action="store_true", help="Skip creating PR, just generate XML.")
    args = parser.parse_args()

    try:
        js_file_path = args.js_file
        version = args.version
        author = args.author
        repo_name = args.repo
        branch_name = args.branch
        github_token = args.token

        print(f"🔍 Processing JS file: {js_file_path}")
        print("=" * 60)
        
        content = parse_js_file(js_file_path)
        
        print(f"📋 Extracting context from file...")
        context = extract_context_from_content(content)
        print(f"✅ Using context: '{context}'")
        
        print(f"🔎 Extracting and validating MongoDB operations...")
        operations, errors, warnings = extract_mongodb_operations_robust(content)
        
        print("\n" + "=" * 60)
        print("📊 VALIDATION SUMMARY")
        print("=" * 60)
        
        validation_report = generate_validation_report(errors, warnings)
        print(validation_report)
        
        # Stop if there are critical errors
        if errors:
            print("💥 GENERATION FAILED: Critical errors must be fixed before proceeding.")
            print("\n📖 Please refer to the MongoDB Query Guidelines and fix the issues above.")
            exit(1)
        
        # Stop if fail-on-warnings is enabled and there are warnings
        if args.fail_on_warnings and warnings:
            print("⚠️ GENERATION STOPPED: Warnings found and --fail-on-warnings is enabled.")
            exit(1)
        
        print(f"🏗️ Generating Liquibase XML for version: {version}")
        xml_content = generate_liquibase_xml_robust(version, operations, author, context, errors, warnings)
        
        changeset_file_path = f"json_changesets/{version}.xml"
        print(f"💾 Writing XML to: {changeset_file_path}")
        write_to_file(xml_content, changeset_file_path)
        print(f"✅ XML file created successfully!")
        
        # 🔄 UPDATED: Check skip-pr flag
        if args.skip_pr:
            print("⏭️ Skipping PR creation as requested (--skip-pr flag).")
            print(f"📄 XML file saved to: {changeset_file_path}")
        else:
            print(f"🚀 Creating pull request...")
            pr = create_pull_request(repo_name, branch_name, changeset_file_path, js_file_path, github_token)
            print(f"🎉 Pull Request created successfully: {pr.html_url}")
        
        print("\n" + "=" * 60)
        print("✅ PROCESS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    except Exception as e:
        print(f"💥 ERROR: {str(e)}")
        exit(1)
