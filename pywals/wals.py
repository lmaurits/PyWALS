import os
import sqlite3
import tempfile
import urllib2

from pywals.language import Language
import pywals.walszipparser as walszipparser

class WALS:

    def __init__(self, dbfile=None, walsfile=None, url=None):

        if not dbfile and not walsfile and not url:
            # We've been given no explicit location for WALS data
            # Is there already a database in the home directory?
            home = os.path.expanduser("~")
            standard_filename = os.path.join(home, ".pywals","wals.db")
            if os.path.exists(standard_filename):
                # Yes, use it
                dbfile = standard_filename
            else:
                # No, try to download from wals.info
                url = "http://wals.info/static/download/wals-language.csv.zip"

        if dbfile:
            self._conn = sqlite3.connect(dbfile)
            self._cur = self._conn.cursor()
            self._preprocess()
            return

        if url:
            # Download WALS zip file to temporary location
            tempdir = tempfile.mkdtemp()
            tmpfile = os.path.join(tempdir, "wals.zip")
            remote = urllib2.urlopen(url)
            local = open(tmpfile, "w")
            local.write(remote.read())
            remote.close()
            local.close()
            walsfile = tmpfile

        if walsfile:
            # Create a new database in the standard location and populate it
            # from the indicated zip file
            home = os.path.expanduser("~")
            standard_filename = os.path.join(home, ".pywals","wals.db")
            if not os.path.exists(os.path.dirname(standard_filename)):
                os.makedirs(os.path.dirname(standard_filename))
            self._conn = sqlite3.connect(standard_filename)
            self._cur = self._conn.cursor()
            walszipparser.populate_db(self._cur, walsfile)
            self._conn.commit()
        else:
            # Couldn't get data from anywhere!
            raise Exception("No data")

        self.language_count = 0
        self.feature_count = 0
        self._feature_id_to_name = {}
        self._feature_name_to_id = {}
        self._value_id_to_name = {}
        self._value_name_to_id = {}
        self._preprocess()

    def _preprocess(self):
        self._build_translations()
        self._update_counts()

    def _build_translations(self):
        self._cur.execute('''SELECT id, name FROM features''')
        for id_, name in self._cur.fetchall():
            self._feature_id_to_name[id_] = name
            self._feature_name_to_id[name] = id_
        self._cur.execute('''SELECT feature_id, value_id, long_desc FROM values_''')
        for feature_id, value_id, name in self._cur.fetchall():
            if feature_id not in self._value_id_to_name:
                self._value_id_to_name[feature_id] = {}
                self._value_name_to_id[feature_id] = {}
            self._value_id_to_name[feature_id][value_id] = name
            self._value_name_to_id[feature_id][name] = value_id

    def _update_counts(self):

        self._cur.execute('''SELECT COUNT(wals_code) FROM languages''')
        self.language_count = self._cur.fetchone()[0]
        self._cur.execute('''SELECT COUNT(id) FROM features''')
        self.feature_count = self._cur.fetchone()[0]

    def _lang_from_code(self, code):
        lang = Language(self)
        self._cur.execute('''SELECT * FROM languages WHERE wals_code=?''',(code,))
        results = self._cur.fetchone()
        lang.code = results[0]
        lang.name = results[1].replace(" ","_").replace("(","").replace(")","")
        lang.location = (float(results[2]) if results[2] else None, float(results[3]) if results[3] else None)
        lang.genus = results[4]
        lang.family = results[5]
        lang.subfamily = results[6]
        lang.iso_codes = results[7]

        lang.features = {}
        self._cur.execute('''SELECT feature_id, value_id FROM data_points WHERE wals_code=?''',(code,))
        datapoints = self._cur.fetchall()
        for feature_id, value_id in datapoints:
            feature_name = self._feature_id_to_name[feature_id]
            value_name = self._value_id_to_name[feature_id][value_id]
            lang.features[feature_name] = value_name

        return lang

    def get_language_by_name(self, name):
        self._cur.execute("""SELECT wals_code FROM languages WHERE name=?""", (name,))
        code = self._cur.fetchone()[0]
        language = self._lang_from_code(code)
        return language

    def get_languages_by_family(self, family):
        self._cur.execute("""SELECT wals_code FROM languages WHERE family=?""", (family,))
        codes = [code[0] for code in self._cur.fetchall()]
        languages = [self._lang_from_code(code) for code in codes]
        return languages

    def get_languages_by_feature_value(self, feature, value):
        feature_id = feature
        value_id = value
        self._cur.execute("""SELECT wals_code FROM data_points WHERE feature_id=? AND value_id=?""", (feature_id, value_id))
        codes = [code[0] for code in self._cur.fetchall()]
        languages = [self._lang_from_code(code) for code in codes]
        return languages
