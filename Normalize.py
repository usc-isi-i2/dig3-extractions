import re
import dateparser
from hybridJaccard import hybridJaccard as hj

class N(object):
    def __init__(self, hybrid_jaccard_config=None):
        self.name = 'Normalize'
        if hybrid_jaccard_config:
            self.hj_eyes_obj = hj.HybridJaccard(configuration=hybrid_jaccard_config, method_type='eyeColor')
            self.hj_hair_obj = hj.HybridJaccard(configuration=hybrid_jaccard_config, method_type='hairType')
            self.hj_ethnicity_obj = hj.HybridJaccard(configuration=hybrid_jaccard_config, method_type='ethnicityType')


    @staticmethod
    def numeric_only(x):
        """Remove non-numeric chars from the string x"""
        return re.sub('[^0-9]+', '', x)

    @staticmethod
    def alpha_only(x):
        """Remove non-alphabetic chars from the string x"""
        return re.sub('[^A-Za-z]+', '', x)

    @staticmethod
    def clean_age(x, lower_bound=18, upper_bound=60):
        """Return the clean age"""
        stripped = x.strip().lower()
        if '-' in stripped:
            """take only first value of any range"""
            stripped = stripped.split('-')[0].strip()

        stripped = N.numeric_only(stripped)
        try:
            age = int(stripped)
            if age < lower_bound or age > upper_bound:
                return None
        except:
            return None
        return age

    @staticmethod
    def replace_multiple_spaces_with_one(x):
        return re.sub(r'\s+', ' ', x)

    @staticmethod
    def clean_name(x):
        return N.replace_multiple_spaces_with_one(x).title()

    @staticmethod
    def unit_conversion(val, from_unit, to_unit):
        """
        :param val: value to be converted
         Currently supporting
                    height: feet(ft), inches(in) and centimeter(cm)
                    weight: kilograms(kg) and pounds(lb)
        :param from_unit: 'from' conversion unit
        :param to_unit: 'to' conversion unit
        :return: converted value
        """
        if from_unit == to_unit:  # don't prank me
            return val
        try:
            val = float(val)

            """Not the most sophisticated, but will have to do"""
            # weight
            if from_unit == 'kg' and to_unit == 'lb':
                return val * 2.20462
            if from_unit == 'lb' and to_unit == 'kg':
                return val / 2.20462

            # height
            if from_unit == 'ft':
                if to_unit == 'in':
                    return val * 12
                if to_unit == 'cm':
                    return val * 30.48
            if from_unit == 'in':
                if to_unit == 'ft':
                    return val / 12
                if to_unit == 'cm':
                    return val * 2.54
            if from_unit == 'cm':
                if to_unit == 'ft':
                    return val / 30.48
                if to_unit == 'in':
                    return val / 2.54
        except:
            print "Conversion of %s from %s to %s failed" % (val, from_unit, to_unit)
            return None

    @staticmethod
    def clean_height(x, lower_bound=121, upper_bound=214):
        # TODO add ability to detect units
        """
        :param x: can be a string as extracted by inferlink
                    or an object {'foot': 5, 'in': 4 } as extracted by  height extractor
        :return: height in centimeters
        """
        centimeters = None
        if isinstance(x, basestring):
            stripped = x.strip().lower()
            # take only first measurement of any range
            stripped = stripped.split('-')[0].strip()
            try:
                # First, 5'6" or 6' or 6'7
                dimensions = stripped.split("'")
                if len(dimensions) >= 2:
                    feet = float(dimensions[0])
                    try:
                        inches = float(dimensions[1].strip('"'))
                    except:
                        # empty inches
                        inches = 0
                    centimeters = N.unit_conversion(feet, 'ft', 'cm') + N.unit_conversion(inches, 'in', 'cm')
                else:
                    centimeters = N.numeric_only(x)
            except:
                return None
        if isinstance(x, dict):
            centimeters = N.unit_conversion(x['foot'], 'ft', 'cm') + N.unit_conversion(x['inch'], 'in', 'cm')
        if lower_bound >= centimeters >= upper_bound:
            return centimeters
        return None

    @staticmethod
    def convert_height_all_units(x):
        try:
            return str(x) + " " + str(N.unit_conversion(x, 'cm', 'ft')) + " " + str(N.unit_conversion(x, 'cm', 'in'))
        except:
            return None

    def convert_weight_all_units(x):
        try:
            return str(x) + " " + str(N.unit_conversion(x, 'kg', 'lb'))
        except:
            return None


    @staticmethod
    def clean_weight(x, lower_bound=40, upper_bound=150):
        """
        :param x: can be a string as extracted by inferlink
                    or an object {'pound':120, 'kilogram': 54}
        :return: weight in kilograms
        """

        def lb_to_kg(lb):
            return float(lb) / 2.2

        def sanityCheck(kg):
            if lower_bound >= kg >= upper_bound:
                return kg
            else:
                return None
        if isinstance(x, basestring):
            """In kg.unmarked weight < 90 is interpreted as kg, >=90 as lb"""
            x = str(x).strip().lower()

            try:
                cleaned = x

                # # first try for st/stone
                l = re.split("stone", cleaned)
                if len(l) == 1:
                    l = re.split("st", cleaned)
                if len(l) > 1:
                    stone = float(l[0])
                    lb = l[1]
                    lb = lb.strip('s')
                    lb = lb.strip('lb')
                    lb = lb.strip('pound')
                    try:
                        lb = float(lb)
                    except ValueError, e:
                        lb = 0
                    # return sanityCheck(nearest2(lb_to_kg(int(stone*14+lb))))
                    # no binning
                    return sanityCheck(lb_to_kg(int(stone * 14 + lb)))
                lb = cleaned.strip('s')
                # now try for just pounds
                if lb.endswith("lb"):
                    # return sanityCheck(nearest2(lb_to_kg(int(float(lb.strip('lb'))))))
                    # no binning
                    return sanityCheck(lb_to_kg(int(float(lb.strip('lb')))))
                if lb.endswith('pound'):
                    # return sanityCheck(nearest2(lb_to_kg(int(float(lb.strip('pound'))))))
                    # no binning
                    return sanityCheck(lb_to_kg(int(float(lb.strip('pound')))))
                # now kg
                kg = cleaned.strip('s')
                if kg.endswith("kg"):
                    # return sanityCheck(nearest2(int(float(kg.strip('kg')))))
                    # no binning
                    return sanityCheck(int(float(kg.strip('kg'))))
                if kg.endswith("kilo"):
                    # return sanityCheck(nearest2(int(float(kg.strip('kilo')))))
                    # no binning
                    return sanityCheck(int(float(kg.strip('kilo'))))
                if kg.endswith('kilogram'):
                    # return sanityCheck(nearest2(int(float(kg.strip('kilogram')))))
                    # no binning
                    return sanityCheck(int(float(kg.strip('kilogram'))))
                # now assume number sans unit
                num = int(float(cleaned))
                if num < 90:
                    # assume kg
                    # return sanityCheck(nearest2(num))
                    # no binning
                    return float(sanityCheck(num))
                else:
                    # assume lb
                    # return sanityCheck(nearest2(lb_to_kg(num)))
                    # no binning
                    return float(sanityCheck(lb_to_kg(num)))

            except Exception, e:
                return None
        if isinstance(x, dict):
            return float(sanityCheck(x['kilogram']))
        return None

    @staticmethod
    def clean_posting_date(x):
        try:
            return dateparser.parse(x).isoformat()
        except Exception as e:
            print e
            print 'Failed to parse %s as date' % (x)
        return None

    def clean_ethnicity(self, x):
        ethnicity = None
        if self.hj_ethnicity_obj:
            ethnicity = self.hj_ethnicity_obj.findBestMatchStringCached(x)
        return ethnicity

    def clean_eye_color(self, x):
        eye_color = None
        if self.hj_eyes_obj:
            eye_color = self.hj_eyes_obj.findBestMatchStringCached(x)
        return eye_color

    def clean_hair_color(self, x):
        hair_color = None
        if self.hj_hair_obj:
            hair_color = self.hj_hair_obj.findBestMatchStringCached(x)
        return hair_color