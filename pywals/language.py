class Language:

    """Object representing a single language from the WALS database,
    providing access to all language and feature data, as well as methods
    to grab objects representing related languages."""

    def __init__(self, parent_WALS):

        self.parent = parent_WALS
        self._cur = self.parent._cur
        self.code = None
        self.iso_codes = None
        self.glottocode = None
        self.name = "Unknown"
        self.location = (0.0, 0.0)
        self.family = "Unknown"
        self.subfamily = "Unknown"
        self.genus = "Unknown"
        self.features = {}

    def get_family_members(self):

        """Return a list of Language objects corresponding to other languages
        in the same family as this language."""

        return self.parent.get_languages_by_family(self.family)
