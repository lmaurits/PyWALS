class Language:

    def __init__(self, parent_WALS):

        self.parent = parent_WALS
        self._cur = self.parent._cur
        self.code = None
        self.iso_codes = None
        self.name = "Unknown"
        self.location = (0.0, 0.0)
        self.family = "Unknown"
        self.subfamily = "Unknown"
        self.genus = "Unknown"
        self.features = {}

    def get_family_members(self):

        return self.parent.get_languages_by_family(self.family)
