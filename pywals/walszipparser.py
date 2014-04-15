import csv
import zipfile

import pywals.sqlmodel as sqlmodel

def populate_db(cursor, filename):
    zf = zipfile.ZipFile(filename)
    wals_data = parse_wals_data(zf)
    sqlmodel.create_tables(cursor)
    populate_languages_table(wals_data["languages"], cursor)
    populate_features_table(wals_data["features"], cursor)
    populate_values_table(wals_data["values"], cursor)
    populate_data_table(wals_data["datapoints"], cursor)    
    sqlmodel.create_indices(cursor)
    
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
    features = reader.next()[8:]
    data["features"] = [dict(zip(["id", "name"], [feature.split(" ", 1)[0], feature.split(" ", 1)[1]])) for feature in features]
    feature_codes = [feature["id"] for feature in data["features"]]
    for line in reader:
        lang_details, feature_details = line[0:8], line[8:]
        data["languages"].append(dict(zip(["wals code", "iso codes", "glottocode", "name", "latitude", "longitude", "genus", "family"], lang_details)))
        datapoint = {}
        datapoint["wals_code"] = lang_details[0]
        for feature_code, value in zip(feature_codes, feature_details):
            if value:
                value_id, long_desc = value.split(" ", 1)
                value_id = int(value_id)
                datapoint[feature_code] = value_id
                valdict = dict(zip(["feature_id", "value_id", "description", "long description"], [feature_code, value_id, "", long_desc]))
                if valdict not in data["values"]:
                    data["values"].append(valdict)
        data["datapoints"].append(datapoint)
            
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

def populate_languages_table(data, cursor):
    for datum in data:
        cursor.execute("""INSERT INTO languages VALUES (?, ?, ?, ?, ?, ?, ?,?)""", (unicode(datum["wals code"],"utf8"), unicode(datum["name"],"utf8"), float(datum["latitude"]) if datum["latitude"] else None, float(datum["longitude"]) if datum["longitude"] else None, unicode(datum["genus"],"utf8"), unicode(datum["family"],"utf8"), unicode(datum["subfamily"],"utf8"), unicode(datum["iso codes"],"utf8")))

def populate_features_table(data, cursor):
    for datum in data:
        cursor.execute("""INSERT INTO features VALUES (?, ?)""", (datum["id"], unicode(datum["name"],"utf8")))

def populate_values_table(data, cursor):
    for datum in data:
        cursor.execute("""INSERT INTO values_ VALUES (?, ?, ?, ?)""", (datum["feature_id"], int(datum["value_id"]), unicode(datum["description"],"utf8"), unicode(datum["long description"],"utf8")))

def populate_data_table(data, cursor):
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
