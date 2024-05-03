from configparser import ConfigParser

def config(filename='//home//vishnud6//vgsales_project//database.ini', section='postgresql'):
    # Create a parser
    parser = ConfigParser()
    # Read config file
    parser.read(filename)

    # Get section, ensure case-sensitivity is handled
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db

# Usage example
try:
    db_params = config(section='postgresql')
    print("Database parameters:", db_params)
except Exception as e:
    print("Error:", e)
