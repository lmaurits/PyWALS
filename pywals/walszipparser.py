import csv
import zipfile

import pywals.sqlmodel as sqlmodel

def populate_db(cursor, filename):
    zf = zipfile.ZipFile(filename)
    wals_data = parse_wals_data(zf)
    conn = None
    sqlmodel.create_tables(conn, cursor)
    populate_languages_table(wals_data["languages"], conn, cursor)
    populate_features_table(wals_data["features"], conn, cursor)
    populate_values_table(wals_data["values"], conn, cursor)
    populate_data_table(wals_data["datapoints"], conn, cursor)    
    sqlmodel.create_indices(conn, cursor)
    
def parse_wals_data(zipfile_handle):
    # Build a data structure representing all the WALS data
    # This structure is a dictionary with one key/value pair per file of WALS data
    # The dictionary keys are the filenames (e.g. "languages", "features", etc.)
    # The dictionary values are lists of dictionaries
    # Each dictionary in a main dictionary value list represents one line from the file
    # The keys are the column headers, the values are the column contents

    print zipfile_handle.namelist()
    if len(zipfile_handle.namelist()) == 2:
        return parse_new_wals_data(zipfile_handle)
    elif len(zipfile_handle.namelist()) == 5:
        return parse_old_wals_data(zipfile_handle)

def parse_new_wals_data(zipfile_handle):
    data = {}
    data["languages"] = []
    data["features"] = []
    data["values"] = []
    data["datapoints"] = []
    fp = zipfile_handle.open("language.csv", "r")
    reader = csv.reader(fp)
    fieldnames = reader.next()
    features = fieldnames[8:]
    data["features"] = [dict(zip(["id", "name"], [feature.split(" ", 1)[0], feature.split(" ", 1)[1]])) for feature in features]
    feature_codes = [feature["id"] for feature in data["features"]]
    for line in reader:
#        print line
        lang_details = line[0:8]
        feature_details = line[8:]
        wals_code, iso_code, glottocode, name, latitude, longitude, genus, family = lang_details
        data["languages"].append(dict(zip(["wals code", "name", "latitude", "longitude", "genus", "family", "subfamily", "iso codes"], [wals_code, name, latitude, longitude, genus, family, "", iso_code])))
        datapoint = {}
        datapoint["wals_code"] = wals_code
        for feature_code, value in zip(feature_codes, feature_details):
            if value:
                value_id, long_desc = value.split(" ", 1)
                value_id = int(value_id)
                datapoint[feature_code] = value_id
                valdict = dict(zip(["feature_id", "value_id", "description", "long description"], [feature_code, value_id, "", long_desc]))
                if valdict not in data["values"]:
                    data["values"].append(valdict)
        data["datapoints"].append(datapoint)
#        print data["datapoints"]
            
    fp.close()
    return data

def parse_old_wals_data(zipfile_handle):
    data = {}
    filenames = "languages features values datapoints".split()
    for filename in filenames:
        x = []
        fp = zipfile_handle.open(filename+".csv", "r")
        reader = csv.DictReader(fp)
        for line in reader:
            x.append(line)
        fp.close()
        data[filename] = x
    return data

def populate_languages_table(data, conn, cursor):
    for datum in data:
        cursor.execute("""INSERT INTO languages VALUES (?, ?, ?, ?, ?, ?, ?,?)""", (unicode(datum["wals code"],"utf8"), unicode(datum["name"],"utf8"), float(datum["latitude"]) if datum["latitude"] else None, float(datum["longitude"]) if datum["longitude"] else None, unicode(datum["genus"],"utf8"), unicode(datum["family"],"utf8"), unicode(datum["subfamily"],"utf8"), unicode(datum["iso codes"],"utf8")))

def populate_features_table(data, conn, cursor):
    for datum in data:
        cursor.execute("""INSERT INTO features VALUES (?, ?)""", (datum["id"], unicode(datum["name"],"utf8")))

def populate_values_table(data, conn, cursor):
    for datum in data:
        cursor.execute("""INSERT INTO values_ VALUES (?, ?, ?, ?)""", (datum["feature_id"], int(datum["value_id"]), unicode(datum["description"],"utf8"), unicode(datum["long description"],"utf8")))

def populate_data_table(data, conn, cursor):
    for datum in data:
        keys = datum.keys()
        keys.remove("wals_code")
        for key in keys:
            wals_code = unicode(datum["wals_code"],"utf8")
            feature_id = key
            if datum[key] == "":
                value_id = None
            else:
                value_id = datum[key]
            cursor.execute("""INSERT INTO data_points VALUES (?, ?, ?)""", (wals_code, feature_id, value_id))

def insert_data(wals_dir, conn, cursor):
    wals_data = parse_wals_data(wals_dir)
    empty_tables(conn, cursor)
    populate_languages_table(wals_data["languages"],conn, cursor)
    populate_features_table(wals_data["features"],conn, cursor)
    populate_values_table(wals_data["values"],conn, cursor)
    populate_data_table(wals_data["datapoints"],conn, cursor)    
