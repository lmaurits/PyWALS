def create_languages_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS languages(
                wals_code TEXT PRIMARY KEY,
                name TEXT,
                latitude REAL,
                longitude REAL,
                genus TEXT,
                family TEXT,
                subfamily TEXT,
                iso_codes TEXT)''')

def create_features_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS features(
                id TEXT PRIMARY KEY,
                name TEXT)''')

def create_values_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS values_(
                feature_id TEXT,
                value_id INTEGER,
                short_desc TEXT,
                long_desc TEXT)''')

def create_data_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS data_points(
                wals_code TEXT,
                feature_id TEXT,
                value_id INTEGER,
                FOREIGN KEY(wals_code) REFERENCES languages(wals_code),
                FOREIGN KEY(feature_id) REFERENCES features(id))''')

def create_tables(cursor):
    create_languages_table(cursor)
    create_features_table(cursor)
    create_values_table(cursor)
    create_data_table(cursor)

def empty_tables(cursor):
    cursor.execute("""DELETE FROM data_points""")
    cursor.execute("""DELETE FROM langs_per_feature_counts""")
    cursor.execute("""DELETE FROM features_per_lang_counts""")
    cursor.execute("""DELETE FROM languages""")
    cursor.execute("""DELETE FROM features""")
    cursor.execute("""DELETE FROM values_""")

def create_indices(cursor):
    cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS lang_name ON languages(name)""");
    cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS lang_family ON languages(family)""");
    cursor.execute("""CREATE INDEX IF NOT EXISTS data_wals ON data_points(wals_code)""");
