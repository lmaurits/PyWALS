import os
import sqlite3
import shutil
import tempfile
import urllib2

from pywals.language import Language
import pywals.walszipparser as walszipparser

class WALS:

    """Object providing an interface to the WALS database, from which
    objects corresponding to languages can be extracted."""

    def __init__(self, dbfile=None, walsfile=None, url=None):

        cleanup_needed = False
        home = os.path.expanduser("~")
        standard_filename = os.path.join(home, ".pywals","wals.db")

        if not dbfile and not walsfile and not url:
            # We've been given no explicit location for WALS data
            # Is there already a database in the home directory?
            if os.path.exists(standard_filename):
                # Yes, use it
                dbfile = standard_filename
            else:
                # No, try to download from wals.info
                url = "http://wals.info/static/download/wals-language.csv.zip"

        # If we've been given an explicit SQLite file, use that.
        if dbfile:
            self._conn = sqlite3.connect(dbfile)
            self._cur = self._conn.cursor()

        # If we've been given a URL, download the file to a temporary
        # location and then pretend the user passed in that location
        # as an explicit filename.
        if not dbfile and url:
            cleanup_needed = True
            tempdir = tempfile.mkdtemp()
            tmp_filename = os.path.join(tempdir, "wals.zip")
            remote = urllib2.urlopen(url)
            local = open(tmp_filename, "w")
            local.write(remote.read())
            remote.close()
            local.close()
            walsfile = tmp_filename

        # If we've been given an explicit path to a WALS .zip file, use that.
        if not dbfile and walsfile:
            # Create a new database in the standard location
            if not os.path.exists(os.path.dirname(standard_filename)):
                os.makedirs(os.path.dirname(standard_filename))
            self._conn = sqlite3.connect(standard_filename)
            self._cur = self._conn.cursor()
            walszipparser.populate_db(self._cur, walsfile)
            self._conn.commit()

        # Set default values of all attributes, then call _preprocess
        # to set them appropriately
        self.language_count = 0
        self.feature_count = 0
        self._feature_id_to_name = {}
        self._feature_name_to_id = {}
        self._value_id_to_name = {}
        self._value_name_to_id = {}
        self._preprocess()

        # Cleanup downloaded files
        if cleanup_needed:
            shutil.rmtree(tempdir)

    def _preprocess(self):

        """Compute various functions of the database."""

        self._build_translations()
        self._update_counts()

    def _build_translations(self):

        """Build dictionaries which translate back and forward between
        codes and human-readable names for features and feature values."""

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

        """Count number of languages, features etc. in database."""

        self._cur.execute('''SELECT COUNT(wals_code) FROM languages''')
        self.language_count = self._cur.fetchone()[0]
        self._cur.execute('''SELECT COUNT(id) FROM features''')
        self.feature_count = self._cur.fetchone()[0]

    # LANGUAGE RELATED STUFF
    ##############################

    def _lang_from_code(self, code):

        """Return a Language object corresponding to the language with the
        provided WALS code."""

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
        lang.glottocode = results[8]

        lang.features = {}
        self._cur.execute('''SELECT feature_id, value_id FROM data_points WHERE wals_code=?''',(code,))
        datapoints = self._cur.fetchall()
        for feature_id, value_id in datapoints:
            feature_name = self._feature_id_to_name[feature_id]
            value_name = self._value_id_to_name[feature_id][value_id]
            lang.features[feature_name] = value_name

        return lang

    def get_all_languages(self):

        """Return a list of Language objects corresponding to all the
        languages in WALS."""

        self._cur.execute("""SELECT wals_code FROM languages""")
        codes = [code[0] for code in self._cur.fetchall()]
        languages = [self._lang_from_code(code) for code in codes]
        return languages

    def get_language_by_name(self, name):

        """Return a Language object corresponding to the language with the
        provided name."""

        self._cur.execute("""SELECT wals_code FROM languages WHERE name=?""", (name,))
        code = self._cur.fetchone()[0]
        language = self._lang_from_code(code)
        return language

    def get_languages_by_family(self, family):

        """Return a list of Language objects corresponding to the languages
        in the provided language family."""

        self._cur.execute("""SELECT wals_code FROM languages WHERE family=?""", (family,))
        codes = [code[0] for code in self._cur.fetchall()]
        languages = [self._lang_from_code(code) for code in codes]
        return languages

    def get_languages_by_feature_value(self, feature, value):

        """Return a list of Language objects corresponding to the languages
        with the given value for the given feature."""

        feature_id = feature
        value_id = value
        self._cur.execute("""SELECT wals_code FROM data_points WHERE feature_id=? AND value_id=?""", (feature_id, value_id))
        codes = [code[0] for code in self._cur.fetchall()]
        languages = [self._lang_from_code(code) for code in codes]
        return languages

    # FEATURE RELATED STUFF
    ##############################

    def get_all_features(self):

        """Return a list of all feature names in the database."""

        self._cur.execute("""SELECT name FROM features""")
        return [f[0] for f in self._cur.fetchall()]

    def get_feature_distribution(self, feature, family=None):

        """Return a tuple structure representing the distribution over
        values for the given feature.  If a family is provided, return the
        distribution for languages in that family only."""
       
        if family:
            self._cur.execute("""SELECT value_id, COUNT(value_id) as count FROM data_points WHERE feature_id=? AND wals_code IN (SELECT wals_code FROM languages WHERE family=?)GROUP BY value_id ORDER BY count DESC""", (feature, family))
        else:
            self._cur.execute("""SELECT value_id, COUNT(value_id) as count FROM data_points WHERE feature_id=? GROUP BY value_id ORDER BY count DESC""", (feature,))
        dist = self._cur.fetchall()
        dist = [(self._value_id_to_name[feature][value], count) for value, count in dist]
        return dist

    def get_feature_language_count(self, feature):

        """Return the number of languages for which there is data bout
        the given feature."""

        feature_id = self._feature_name_to_id[feature]
        self._cur.execute("""SELECT COUNT(wals_code) FROM data_points WHERE feature_id=?""", (feature_id,))
        return self._cur.fetchone()[0]
