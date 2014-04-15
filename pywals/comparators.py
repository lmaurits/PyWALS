from math import cos, sin, atan, sqrt, radians

def distance(language1, language2):

    """Return the distance, in kilometers, between the WALS location for two
    languages, calculated using the Haversine function, i.e. "as the crow
    flies" and ignoring terrain."""

    R = 6371.0
    lat1, lon1 = language1.location
    lat2, lon2 = language2.location
    dlat = radians(lat2-lat1)
    dlon = radians(lon2-lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    c = 2*atan(sqrt(a)/sqrt(1-a))
    d = R*c
    d = 2*d
    return d

def genetic(language1, language2):

    """Return 0.0 is the two languages are in the same family and same genus,
    return 0.5 if the languages are in the same family but different genera,
    and return 1.0 if the languages are in different families."""

    if language1.family != language2.family:
        return 1.0
    elif language1.genus != language2.genus:
        return 0.5
    else:
        return 0.0

def hamming_featural(language1, language2):

    """Return the mean Hamming distance between two languages, averaged
    over all features for which there is data for both languages.  This is
    the proportion of features for which the two languages have the same
    value."""

    d = 0
    n = 0
    for feat in language1.features:
        if feat in language2.features:
            if language1.features[feat] != language2.features[feat]:
                d += 1.0
            n += 1.0
    return d/n
