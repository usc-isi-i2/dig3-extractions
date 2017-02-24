import re
import dateparser
import sys
sys.path.append('/Users/amandeep/Github/dig-crf/src/applyCrf')
from hybridJaccard import hybridJaccard as hj
import numbers


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
    def clean_age(x, conf):
        x = x['value']
        stripped = x.strip().lower()
        if '-' in stripped:
            """take only first value of any range"""
            stripped = stripped.split('-')[0].strip()

        stripped = N.numeric_only(stripped)
        try:
            age = N.sanity_check_values(int(stripped), conf)
            if not age:
                return None
        except:
            return None
        o = dict()
        o['name'] = age
        o['key'] = age
        return o

    @staticmethod
    def replace_multiple_spaces_with_one(x):
        return re.sub(r'\s+', ' ', x)

    @staticmethod
    def clean_name(x, conf):
        x = x['value']
        name = N.replace_multiple_spaces_with_one(x).title()
        if N.sanity_check_values(name, conf) is not None:
            o = dict()
            o['name'] = name
            o['key'] = name
            return o
        return None

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
    def clean_height(x, conf):
        # TODO add ability to detect units
        """
        :param x: can be a string as extracted by inferlink
                    or an object {'foot': 5, 'in': 4 } as extracted by  height extractor
        :return: height in centimeters
        """
        centimeters = None
        x = x['value']
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
                if N.sanity_check_values(centimeters, conf) is not None:
                    o = dict()
                    o['name'] = centimeters
                    o['key'] = centimeters
                    o['search'] = N.convert_height_all_units(centimeters)
                    return o
            except:
                return None
        if isinstance(x, dict):
            print x
            # centimeters = N.unit_conversion(x['foot'], 'ft', 'cm') + N.unit_conversion(x['inch'], 'in', 'cm')
            if 'centimeter' in x:
                centimeters_list = x['centimeter']
                o_list = list()
                for centimeters in centimeters_list:
                    if N.sanity_check_values(centimeters, conf) is not None:
                        o = dict()
                        o['name'] = centimeters
                        o['key'] = centimeters
                        o['search'] = N.convert_height_all_units(centimeters)
                        o_list.append(o)
                        if len(o_list) > 0:
                            return o_list
            else:

                print 'YOU DONE MESSED UP:', x
        return None

    @staticmethod
    def convert_height_all_units(x):
        try:
            return str(x) + " " + str(N.unit_conversion(x, 'cm', 'ft')) + " " + str(N.unit_conversion(x, 'cm', 'in'))
        except:
            return None

    @staticmethod
    def convert_weight_all_units(x):
        try:
            return str(x) + " " + str(N.unit_conversion(x, 'kg', 'lb'))
        except:
            return None


    @staticmethod
    def clean_weight(x, conf):
        """
        :param x: can be a string as extracted by inferlink
                    or an object {'pound':120, 'kilogram': 54}
        :return: weight in kilograms
        """

        def lb_to_kg(lb):
            return float(lb) / 2.2
        x = x['value']
        if isinstance(x, basestring):
            """In kg.unmarked weight < 90 is interpreted as kg, >=90 as lb"""
            x = str(x).strip().lower()
            clean_weight_value = None

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
                    # no binning
                        clean_weight_value = lb_to_kg(int(stone * 14 + lb))
                lb = cleaned.strip('s')
                # now try for just pounds
                if lb.endswith("lb"):
                    # no binning
                    clean_weight_value = lb_to_kg(int(float(lb.strip('lb'))))
                elif lb.endswith('pound'):
                    # no binning
                    clean_weight_value = lb_to_kg(int(float(lb.strip('pound'))))
                # now kg
                elif lb.endswith("kg"):
                    # no binning
                    clean_weight_value = int(float(lb.strip('kg')))
                elif lb.endswith("kilo"):
                    # no binning
                    clean_weight_value = int(float(lb.strip('kilo')))
                elif lb.endswith('kilogram'):
                    # no binning
                    clean_weight_value = int(float(lb.strip('kilogram')))
                else:
                    # now assume number sans unit
                    num = int(float(cleaned))
                    if num < 90:
                        # assume kg
                        # no binning
                        clean_weight_value = float(num)
                    else:
                        # assume lb
                        # no binning
                        clean_weight_value = float(lb_to_kg(num))
                if N.sanity_check_values(clean_weight_value, conf) is not None:
                    o = dict()
                    o['name'] = clean_weight_value
                    o['key'] = clean_weight_value
                    o['search'] = N.convert_weight_all_units(clean_weight_value)
                    return o
            except Exception, e:
                return None

        if isinstance(x, dict):
            o_list = list()
            weight_list = x['kilogram']
            for clean_weight_value in weight_list:
                clean_weight_value = float(clean_weight_value)
                if N.sanity_check_values(clean_weight_value, conf) is not None:
                    o = dict()
                    o['name'] = clean_weight_value
                    o['key'] = clean_weight_value
                    o['search'] = N.convert_weight_all_units(clean_weight_value)
                    o_list.append(o)
                    if len(o_list) > 0:
                        return o_list
        return None

    @staticmethod
    def clean_posting_date(x, conf):
        x = x['value']
        try:
            d = dateparser.parse(x).isoformat()
            o = dict()
            o['name'] = d
            o['key'] = d
            return o
        except Exception as e:
            print e
            print 'Failed to parse %s as date' % (x)
        return None

    def clean_ethnicity(self, x, conf):
        x = x['value']
        ethnicity = None
        if self.hj_ethnicity_obj:
            ethnicity = N.sanity_check_values(self.hj_ethnicity_obj.findBestMatchStringCached(x), conf)
        if ethnicity:
            o = dict()
            o['name'] = ethnicity
            o['key'] = ethnicity
            return o
        return None

    def clean_eye_color(self, x, conf):
        x = x['value']
        eye_color = None
        if self.hj_eyes_obj:
            eye_color = N.sanity_check_values(self.hj_eyes_obj.findBestMatchStringCached(x), conf)
        if eye_color:
            o = dict()
            o['name'] = eye_color
            o['key'] = eye_color
            return o
        return None

    def clean_hair_color(self, x, conf):
        x = x['value']
        hair_color = None
        if self.hj_hair_obj:
            hair_color = N.sanity_check_values(self.hj_hair_obj.findBestMatchStringCached(x), conf)
        if hair_color:
            o = dict()
            o['name'] = hair_color
            o['key'] = hair_color
            return o
        return None

    @staticmethod
    def clean_city(x, conf=None):
        o = dict()
        if isinstance(x, dict):
            if 'value' in x:
                if 'state' in x and 'country' in x and 'longitude' in x and 'latitude' in x:
                    o['key'] = x['value'] + ":" + x['state'] + ":" + x['country'] + ":" + str(x['longitude']) + ":" + \
                           str(x['latitude'])
                    o['name'] = x['value']
                else:
                    o['key'] = x['value']
                    o['name'] = x['value']
        else:
            o['key'] = x
            o['name'] = x
        return o

    @staticmethod
    def sanity_check_values(x, conf):
        if not x:
            return x
        if 'range' in conf:
            r = conf['range']
            if not r['lower_bound'] <= int(x) <= r['upper_bound']:
                print '%s should be in between %s and %s' % (str(x), r['lower_bound'], r['upper_bound'])
                return None
        if not isinstance(x, eval(conf['data_type'])):
            print '%s expected to be of type %s, but is of type %s' %(str(x), conf['data_type'], type(x))
            return None
        return x

    @staticmethod
    def identity(x, conf=None):
        x = x['value']
        o = dict()
        o['name'] = x
        return o
