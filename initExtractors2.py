"""
To make sure an INFERLINK field is processed, it should be added to following global variables
    1. inferlink_data_fields
    2. inverse_inferlink_data_fields
    3. inferlink_type_to_extractor_map - to run relevant extractor on aa inferlink fields

To ensure a data extractor runs,
    1. Add it to data_extractors list in this file
    2. Add it to data _extractors in the main of new_extractor_run2.py

"""
import json
import time
from tldextract import tldextract
import codecs
import pprint
import re
from digReadabilityExtractor.readability_extractor import ReadabilityExtractor
from jsonpath_rw import parse, jsonpath
from digRegexExtractor.regex_extractor import RegexExtractor
from digPhoneExtractor.phone_extractor import PhoneExtractor
from digAgeRegexExtractor.age_regex_helper import get_age_regex_extractor
from digDictionaryExtractor.populate_trie import populate_trie
from digDictionaryExtractor.dictionary_extractor import DictionaryExtractor
from digExtractor.extractor import Extractor as SuperExtractor
from digExtractor.extractor_processor import ExtractorProcessor
from digTokenizerExtractor.tokenizer_extractor import TokenizerExtractor
from digLandmarkExtractor.get_landmark_extractor_processors import get_multiplexing_landmark_extractor_processor
from landmark_extractor.extraction.Landmark import RuleSet
from digHeightExtractor.height_extractor import HeightExtractor
from digWeightExtractor.weight_extractor import WeightExtractor
from digServiceExtractor.names_helper import get_service_extractor
from digReviewIDExtractor.review_id_extractor import ReviewIDExtractor
from digPriceExtractor.price_extractor import PriceExtractor
from digSocialMediaIdExtractor.socialmedia_id_extractor import SocialMediaIdExtractor
from digAddressExtractor.address_extractor import AddressExtractor

"""This is just for reference
inferlink_field_names = [
                         {'name': ['inferlink_name']},
                         {'posting_date': ['inferlink_posting-date', 'inferlink_posting-date-2',
                                           'inferlink_posting-date-1']},
                         {'location': ['inferlink_city', 'inferlink_state','inferlink_country', 'inferlink_location',
                                       'inferlink_location-1', 'inferlink_location-2','inferlink_location-3']},
                         {'phone': ['inferlink_phone', 'inferlink_local-phone', 'inferlink_phone-1']},
                         {'description': ['inferlink_description', 'inferlink_description-1','inferlink_description-2',
                                          'inferlink_description-3','inferlink_description-4']},
                         {'age': ['inferlink_age', 'inferlink_age-1', 'inferlink_age-2']},
                         {'ethnicity': ['inferlink_ethnicity']},
                         {'hair_color': ['inferlink_hair-color']},
                         {'weight': ['inferlink_weight']},
                         {'price': ['inferlink_price','inferlink_price-1', 'inferlink_price-2', 'inferlink_price-3']},
                         {'height': ['inferlink_height']},
                         {'eye_color': ['inferlink_eye-color']},
                         {'gender': ['inferlink_gender']}
                         ]
"""
inferlink_data_fields = {
    'name': ['inferlink_name'],
    'posting_date': ['inferlink_posting-date', 'inferlink_posting-date-2',
                     'inferlink_posting-date-1'],
    'location': ['inferlink_location', 'inferlink_location-1',
                 'inferlink_location-2','inferlink_location-3'],
    'city': ['inferlink_city'],
    'state': ['inferlink_state'],
    'country': ['inferlink_country'],
    'phone': ['inferlink_phone'], #, 'inferlink_local-phone', 'inferlink_phone-1'],
    'age': ['inferlink_age', 'inferlink_age-1', 'inferlink_age-2'],
    'ethnicity': ['inferlink_ethnicity'],
    'hair_color': ['inferlink_hair-color'],
    'weight': ['inferlink_weight'],
    'price': ['inferlink_price','inferlink_price-1', 'inferlink_price-2', 'inferlink_price-3'],
    'height': ['inferlink_height'],
    'eye_color': ['inferlink_eye-color'],
    'gender': ['inferlink_gender'],
    'service': ['inferlink_service'],
    'review_id': ['inferlink_review-id']
}

inverse_inferlink_data_fields = {'inferlink_price': 'price',
                                 'inferlink_posting-date': 'posting_date',
                                 'inferlink_posting-date-2': 'posting_date',
                                 'inferlink_location': 'location',
                                 'inferlink_age-2': 'age',
                                 'inferlink_height': 'height',
                                 'inferlink_country': 'country',
                                 'inferlink_name': 'name',
                                 'inferlink_city': 'city',
                                 'inferlink_gender': 'gender',
                                 'inferlink_age': 'age',
                                 'inferlink_ethnicity': 'ethnicity',
                                 'inferlink_phone': 'phone',
                                 'inferlink_state': 'state',
                                 'inferlink_location-1': 'location',
                                 'inferlink_hair-color': 'hair_color',
                                 'inferlink_location-3': 'location',
                                 'inferlink_location-2': 'location',
                                 'inferlink_age-1': 'age',
                                 'inferlink_posting-date-1': 'posting_date',
                                 'inferlink_weight': 'weight',
                                 'inferlink_price-1': 'price',
                                 'inferlink_price-2': 'price',
                                 'inferlink_price-3': 'price',
                                 'inferlink_eye-color': 'eye_color',
                                 'inferlink_service' : 'service',
                                 'inferlink_review-id': 'review_id'
                                 }


fields_to_remove = ["crawl_data", "extracted_metadata"]
my_name_is_name_regex = re.compile('(?:my[\s]+name[\s]+is[\s]+([-a-z0-9@$!]+))', re.IGNORECASE)
name_filter_regex = re.compile('[a-z].*[a-z]')
# Initialize root extractors
readability_extractor_init = ReadabilityExtractor().set_metadata({'type': 'readability_high_recall'})
readability_extractor_rc_init = ReadabilityExtractor().set_recall_priority(False).set_metadata({'type': 'readability_low_recall'})


tokenizer_extractor = TokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True).set_metadata({'extractor': 'crf_tokenizer'})
# init sub root extractors
phone_extractor_init = PhoneExtractor().set_metadata({'extractor': 'phone', 'semantic_type': 'phone', 'input_type': ['tokens']}).set_source_type('text')
age_extracor_init = get_age_regex_extractor().set_metadata({'semantic_type': 'age', 'input_type': ['text']}).set_include_context(True)

"""setup a title extractor to get title from html"""
html_title_regex = re.compile(r'<title>(.*?)</title>', flags=re.IGNORECASE)
title_regex_extractor = RegexExtractor() \
    .set_regex(html_title_regex) \
    .set_metadata({'extractor': 'title_regex', 'type': 'title', 'input_type': ['text'], 'name': 'title'}) \
    .set_include_context(True)

city_dictionary_extractor_init = DictionaryExtractor() \
    .set_ngrams(3) \
    .set_pre_filter(lambda x: name_filter_regex.match(x)) \
    .set_pre_process(lambda x: x.lower()) \
    .set_metadata({
    'extractor': 'dig_cities_dictionary_extractor',
    'semantic_type': 'city',
    'input_type': ['tokens'],
    'type': 'dictionary',  # !Important
    'properties_key': 'cities',  # !Important
}) \
    .set_include_context(True)

name_regex_extractor_init = RegexExtractor() \
    .set_regex(my_name_is_name_regex) \
    .set_metadata({
    'extractor': 'name_regex',
    'semantic_type': 'name',
    'input_type': ['text']
}) \
    .set_include_context(True)

ethnicities_dictionary_extractor_init = DictionaryExtractor() \
    .set_pre_filter(lambda x: name_filter_regex.match(x)) \
    .set_pre_process(lambda x: x.lower()) \
    .set_metadata({
    'extractor': 'dig_ethnicities_dictionary_extractor',
    'semantic_type': 'ethnicity',
    'input_type': ['tokens'],
    'type': 'dictionary',  # !Important
    'properties_key': 'ethnicities',  # !Important
}) \
    .set_include_context(True)

hair_color_dictionary_extractor_init = DictionaryExtractor() \
    .set_pre_filter(lambda x: name_filter_regex.match(x)) \
    .set_pre_process(lambda x: x.lower()) \
    .set_metadata({
    'extractor': 'dig_haircolor_dictionary_extractor',
    'semantic_type': 'hair_color',
    'input_type': ['tokens'],
    'type': 'dictionary',  # !Important
    'properties_key': 'haircolor',  # !Important
}) \
    .set_include_context(True)

eye_color_dictionary_extractor_init = DictionaryExtractor() \
    .set_pre_filter(lambda x: name_filter_regex.match(x)) \
    .set_pre_process(lambda x: x.lower()) \
    .set_metadata({
    'extractor': 'dig_eyecolor_dictionary_extractor',
    'semantic_type': 'eye_color',
    'input_type': ['tokens'],
    'type': 'dictionary',  # !Important
    'properties_key': 'eyecolor',  # !Important
}) \
    .set_include_context(True)

height_extractor_init = HeightExtractor()\
            .set_metadata({'extractor': 'height',
                           'semantic_type': 'height',
                           'input_type': ['text'],
                           'type': 'height'})

weight_extractor_init = WeightExtractor()\
            .set_metadata({'extractor': 'weight',
                           'semantic_type': 'weight',
                           'input_type': ['text'],
                           'type': 'weight'})

state_dictionary_extractor = DictionaryExtractor() \
    .set_pre_filter(lambda x: name_filter_regex.match(x)) \
    .set_pre_process(lambda x: x.lower()) \
    .set_metadata({
        'extractor': 'dig_state_dictionary_extractor',
        'semantic_type': 'state',
        'input_type': ['tokens'],
        'type': 'dictionary',  # !Important
        'properties_key': 'states'  # !Important
}).set_include_context(True)

service_extractor = get_service_extractor().set_metadata({
        'extractor': 'dig_service_dictionary_extractor',
        'semantic_type': 'service',
        'input_type': ['tokens']
}).set_include_context(True)

reviewid_extractor = ReviewIDExtractor().set_metadata({
        'extractor': 'dig_review_id_regex_extractor',
        'semantic_type': 'review_id',
        'input_type': ['text']
})

price_extractor = PriceExtractor().set_metadata({
        'extractor': 'dig_price_regex_extractor',
        'semantic_type': 'price',
        'input_type': ['text']
})

french_english_words = json.load(codecs.open('french_english_words.json', 'r'))
socialmedia_id_extractor = SocialMediaIdExtractor(french_english_words['english'],
                                                  french_english_words['french']).set_metadata({
                                                        'extractor': 'dig_social_media_extractor',
                                                        'semantic_type': 'social_media_id',
                                                        'input_type': ['tokens']
                                                })

address_extractor = AddressExtractor() \
            .set_include_context(True) \
            .set_metadata({
                'extractor': 'dig_address_regex_extractor',
                'semantic_type': 'address',
                'input_type': ['text']
        })

names_dictionary_extractor = DictionaryExtractor() \
    .set_pre_filter(lambda x: name_filter_regex.match(x)) \
    .set_pre_process(lambda x: x.lower()) \
    .set_metadata({
    'extractor': 'dig_name_dictionary_extractor',
    'semantic_type': 'name',
    'input_type': ['tokens'],
    'type': 'dictionary',  # !Important
    'properties_key': 'names_high_precision',  # !Important
}) \
    .set_include_context(True)

data_extractors = [
            phone_extractor_init,
            age_extracor_init,
            city_dictionary_extractor_init,
            hair_color_dictionary_extractor_init,
            eye_color_dictionary_extractor_init,
            name_regex_extractor_init,
            ethnicities_dictionary_extractor_init,
            height_extractor_init,
            weight_extractor_init,
            state_dictionary_extractor,
            service_extractor,
            reviewid_extractor,
            price_extractor,
            socialmedia_id_extractor,
            address_extractor,
            names_dictionary_extractor
        ]
inferlink_type_to_extractor_map = {
    'name': [name_regex_extractor_init],
    'posting_date': [],
    'location': [city_dictionary_extractor_init],
    'city': [city_dictionary_extractor_init],
    'state': [state_dictionary_extractor],
    'country': [],
    'phone': [phone_extractor_init],
    'age': [],
    'ethnicity': [ethnicities_dictionary_extractor_init],
    'hair_color': [hair_color_dictionary_extractor_init],
    'weight': [],
    'price': [],
    'height': [],
    'eye_color': [eye_color_dictionary_extractor_init],
    'gender': [],
    'nothing': [],
    'service': [service_extractor],
    'review_id': []
}

class LambdaExtractor(SuperExtractor):

    def __init__(self, x):
        super(LambdaExtractor, self).__init__()
        self.renamed_input_fields = x + '_x'
        self.metadata = {}

    def extract(self, doc):
        return self.f(doc[self.renamed_input_fields])

    def get_metadata(self):
        return self.metadata

    def set_metadata(self, metadata):
        self.metadata = metadata
        return self

    def get_renamed_input_fields(self):
        return self.renamed_input_fields

    def set_extract_function(self, f):
        self.f = f
        return self


pp = pprint.PrettyPrinter(indent=4)


class Extractor(object):
    """A simple data structure for initializing the extractor and helper functions for processing
     processing the document """
    def __init__(self, class_type, input_field, output_field, semantic_type=None, metadata=None):
        self.type = class_type
        self.input = input_field
        self.output = output_field
        self.semantic_type = semantic_type
        self.metadata = metadata

    @staticmethod
    def remove_fields(doc, fields):
        for field in fields:
            doc.pop(field, None)
            doc[field] = "..."
        return doc

    @staticmethod
    def add_tld(x):
        if 'tld' not in x:
            if 'url' in x:
                url = x['url']
                tld = tldextract.extract(url).domain + '.' + tldextract.extract(url).suffix
                x['tld'] = tld
        return x

    @staticmethod
    def rename_key(old_key, new_key, doc):
        if old_key in doc:
            val = doc.pop(old_key, None)
            doc[new_key] = val
        return doc

    @staticmethod
    def load_trie(file_name):
        values = json.load(codecs.open(file_name, 'r', 'utf-8'))
        trie = populate_trie(map(lambda x: x.lower(), values))
        return trie

    @staticmethod
    def extract_title(doc, title_extractor):
        title_regex_processor = ExtractorProcessor() \
            .set_input_fields('raw_content') \
            .set_output_field('extractors.title.text') \
            .set_extractor(title_extractor)
        return title_regex_processor.extract(doc)

    @staticmethod
    def execute_extractor(extractor, doc):

        output = ""
        try:
            start_time = time.time()
            output = extractor.extract(doc)
            metadata =  extractor.get_metadata()
            if 'semantic_type' in metadata and metadata['semantic_type'] == 'social_media_id':
                print output
            time_taken = time.time() - start_time
            if time_taken > 5.0:
                print "Extractor %s took %s seconds for %s" % (extractor.get_name(), str(time_taken), doc['url'])
        except Exception as e:
            print e
            print "Extractor %s crashed." % extractor
        # print "Document url %s" % doc['url']
        return output

"""  ************  IMPORTANT  *************   """
""" Initialize the content and Data extractors here """
content_extractors = {
    'READABILITY_HIGH_RECALL': Extractor(readability_extractor_init, 'raw_content', 'content_relaxed'),
    'READABILITY_LOW_RECALL': Extractor(readability_extractor_rc_init, 'raw_content', 'content_strict'),
    'TITLE': Extractor(title_regex_extractor, 'raw_content', 'title')
}

""" ************** END INTIALIZATION ******************  """


class ProcessExtractor(Extractor):
    """ Class to process the document - Extend functions from Extractor class """
    def __init__(self, content_extractors, data_extractors, properties=None, landmark_rules=None):
        self.landmark_rules = landmark_rules
        self.landmark = None
        if self.landmark_rules:
            rule_sets = dict()
            for key, value in self.landmark_rules.iteritems():
                rule_sets[key] = RuleSet(value)
            landmark_extractor_init = get_multiplexing_landmark_extractor_processor(rule_sets,
                                                                                    ['raw_content', 'tld'],
                                                                                    lambda tld: tld,
                                                                                    None,
                                                                                    True,
                                                                                    metadata={
                                                                                        'extractor': 'landmark',
                                                                                        'semantic_type': 'landmark'
                                                                                    })
            if landmark_extractor_init:
                # print "i L:", landmark_extractor_init
                self.landmark = landmark_extractor_init
        self.content_extractors = self.__initialize(content_extractors)
        self.data_extractors = self.__get_data_extractor(data_extractors, properties)

    def __initialize(self, extractors_selection, type_filter=None):
        """ Initialize content extractors """
        dictionary, result = {}, {}
        dictionary = content_extractors
        for extractor in extractors_selection:
            if extractor in dictionary:
                result[extractor] = dictionary[extractor]
            else:
                print extractor + ' - Not found'
        return result

    def __get_data_extractor(self, sub, properties):
        """ Initialize all data extractors and return only extractors that are included
            in the execution request chain
        """
        res = []
        for extractor in data_extractors:
            metadata = extractor.get_metadata()
            # !Important : If metadata has type dictionary than load properties into extractor
            if 'type' in metadata.keys() and 'properties_key' in metadata.keys():
                if metadata['type'] == 'dictionary':
                    extractor.set_trie(Extractor.load_trie(properties[metadata['properties_key']]))
            elif 'type' in metadata.keys():
                if metadata['type'] == 'dictionary' and 'properties_key' not in metadata.keys():
                    print 'No properties key mentioned in metadata avoiding - ', metadata['semantic_type']
                    continue

            if metadata['semantic_type'] in sub:
                res.append(extractor)
        return res

    def buildTreeFromHtml(self, doc, inputs, levelKey=None, jsonPath=False):
        # Initialize levelkey if doesn't exist!
        if levelKey:
            doc[levelKey] = {}
        # if self.landmark:
        #   doc = self.landmark.extract(doc)

        for input_key in inputs:
            extract_key = input_key
            for key, extractor in self.content_extractors.iteritems():
                start_time = time.time()
                if levelKey:
                    doc['html'] = doc[extract_key]
                    output = Extractor.execute_extractor(extractor.type, doc)
                    if not output:
                      continue
                    result = {'value': output}
                    metadata = extractor.type.get_metadata()
                    metadata['result'] = result
                    metadata['source'] = input_key
                    doc[levelKey][extractor.output] = {'text': [metadata]}
                time_taken = time.time() - start_time
                print "Time for " + key + " : ", time_taken

        if title_regex_extractor:
            try:
                doc = Extractor.extract_title(doc, title_regex_extractor)
                # print pprint.pprint(doc['extractors']['title'], indent=4)
            except:
                print 'No Title found: ', doc['url']
        return doc

    def addTokenizedData(self, doc, matches, index, data):
        crf_tokens = []
        r = data['result']
        # print "R:", r
        if isinstance(r, list):
            # Currently for title, as it uses extractor processor
            # TODO fix this, should be generic
            r = r[0]
        if r['value']:
            temp = {'text': r['value']}
            # print "Rvalue:", r['value']
            tokens = Extractor.execute_extractor(tokenizer_extractor, temp)
            if tokens:
                crf_tokens = tokens[0]
                result = [{'result': {'value': crf_tokens[0]}}]
                doc = self.update_json(doc, matches, 'crf_tokens', result, index, parent=True)
        return doc, crf_tokens

    def addSimpleTokenizedData(self, doc, matches, index, crf_tokens):
        new_simple_tokens = [tk['value'] for tk in crf_tokens]
        result = [{'result': {'value': new_simple_tokens}}]
        doc = self.update_json(doc, matches, 'tokens', result, index, parent=True)
        return doc, new_simple_tokens

    def addDataExtractorValues(self, doc, matches, index, text, simple_tokens):
        extractions = {}

        if 'source' in text and text['source'] == 'landmark':
            if text['type'] in inferlink_type_to_extractor_map and inferlink_type_to_extractor_map[text['type']]:
                data_extractors = inferlink_type_to_extractor_map[text['type']]
            else:
                data_extractors = []
        else:
            data_extractors = self.data_extractors

        for extractor in data_extractors:
            metadata = extractor.get_metadata()
            inputs = metadata['input_type']
            key = metadata['semantic_type']

            if 'tokens' in inputs:
                if 'extractor' in metadata and metadata['extractor'] == 'phone':
                    temp = {'input': simple_tokens}
                else:
                    temp = {'tokens': simple_tokens}

                extraction = Extractor.execute_extractor(extractor, temp)
            elif 'text' in inputs:
                r = text['result']
                if isinstance(r, list):
                    # TODO fix this, should be generic
                    r = r[0]

                temp = {'text': r['value']}
                extraction = Extractor.execute_extractor(extractor, temp)
            else:
                continue
            if not extraction:
                continue
            # print extraction
            # result = {'value': extraction}
            metadata = extractor.get_metadata()
            if 'extractor' in metadata and metadata['extractor'] == 'phone':
                extraction = ProcessExtractor.format_phone_output(extraction)
            # else:
            #     print 'something else'
            #     print extraction
            metadata['result'] = extraction
            metadata['source'] = 'tokens'
            extractions[key] = [metadata]
        if extractions:
            doc = self.update_json(doc, matches, 'data_extractors', extractions, index, parent=True)
        return doc, extractions

    @staticmethod
    def format_phone_output(phones):
        # TODO this should be done by extractor in future
        out_phones = list()
        if isinstance(phones, list):
            for phone in phones:
                if isinstance(phone, basestring):
                    out = dict()
                    out['value'] = phone
                    out_phones.append(out)
        if len(out_phones) > 0:
            return out_phones
        return phones

    def annotateTokenToExtractions(self, tokens, extractions):
        for extractor, extractions in extractions.iteritems():
            if extractor in ['phone', 'social_media_id']:
                " ignoring phone annotation.."
                continue
            for extraction in extractions:
                input_type = extraction['input_type']
                if 'text' in input_type:
                    # build text tokens
                    pass

                if 'tokens' not in input_type:
                    print "ignoring ", extractor, " as tokens not dependant.."
                    continue
                data = extraction['result']
                for values in data:
                    start = values['context']['start']
                    end = values['context']['end']
                    offset = 0
                    for i in range(start, end):
                        if 'semantic_type' not in tokens[i].keys():
                            tokens[i]['semantic_type'] = []
                        temp = {}
                        temp['type'] = extractor
                        temp['offset'] = offset
                        if offset == 0:
                            temp['length'] = end - start
                        tokens[i]['semantic_type'].append(temp)
                        offset += 1
        return tokens

    def addTokensWithAnnotation(self, doc, matches, index, crf_tokens, content):
        crf_tokens = self.annotateTokenToExtractions(crf_tokens, content)
        doc = self.update_json(doc, matches, 'crf_tokens', crf_tokens, index, parent=True)
        return doc, crf_tokens

    def buildTokensAndDataExtractors(self, doc):
        jsonpath_expr = parse('*.*.text')
        matches = jsonpath_expr.find(doc)
        index = 0
        for index in range(len(matches)):
            values = matches[index].value
            # print "VAL:", values
            data, e_type = values[0], values[0]['type']
            if e_type == 'table':
                # search internal text values
                table_expr = parse('extractors.tables.text.[*].result.value.tables[*].rows[*].cells[*].text')
                table_matches = table_expr.find(doc)
                # print str(len(table_matches)) + " : tables cell count"
                for tIndex in range(len(table_matches)):
                    tValues = table_matches[tIndex].value
                    data = tValues[0]
                    # step2 - build crf tokens
                    doc, crf_tokens = self.addTokenizedData(doc, table_matches, tIndex, data)
                    # step3 - build normal tokens
                    if crf_tokens:
                        doc, simple_tokens = self.addSimpleTokenizedData(doc, table_matches, tIndex, crf_tokens)
                        # step4 - build data extractors
                        doc, content = self.addDataExtractorValues(doc, table_matches, tIndex, data, simple_tokens)
                        # step5 - annotate the data extractors to tokens
                        doc, crf_tokens = self.addTokensWithAnnotation(doc, table_matches, tIndex, crf_tokens, content)
            else:
                # step2 - build crf tokens
                doc, crf_tokens = self.addTokenizedData(doc, matches, index, data)
                # step3 - build normal tokens
                if crf_tokens:
                    doc, simple_tokens = self.addSimpleTokenizedData(doc, matches, index, crf_tokens)
                    # step4 - build data extractors
                    doc, content = self.addDataExtractorValues(doc, matches, index, data, simple_tokens)
                    # step5 - annotate the data extractors to tokens
                    doc, crf_tokens = self.addTokensWithAnnotation(doc, matches, index, crf_tokens, content)
        return doc

    def process_inferlink_fields(self, doc):
        if self.landmark:
            self.landmark.set_output_field('landmarks')
            self.landmark.output_fields = None
            doc = Extractor.add_tld(Extractor.rename_key('_id', 'cdr_id', Extractor.remove_fields(doc, fields_to_remove)))
            doc = self.landmark.extract(doc)

            # Not the best to handle this, but here goes nothing

            if 'landmarks' in doc and doc['landmarks']:
                # print pprint.pprint(doc['landmarks'], indent=4)
                landmark = dict()
                landmark_extractions = doc['landmarks']
                for le in landmark_extractions:
                    l_result = le['result']
                    for key in l_result.keys():
                        if key in inverse_inferlink_data_fields:
                            semantic_type = inverse_inferlink_data_fields[key]
                        else:
                            semantic_type = 'nothing'
                        landmark[key] = dict()
                        landmark[key]['text'] = list()
                        e_value = l_result[key]
                        if isinstance(e_value, dict):
                            e_value = [e_value]
                        for value in e_value:
                            r_obj = dict()
                            r_obj['result'] = dict()
                            r_obj['result']['value'] = value['value']
                            r_obj['type'] = semantic_type
                            r_obj['source'] = 'landmark'
                            landmark[key]['text'].append(r_obj)
                            # print pprint.pprint(doc['landmark'][key]['text'], indent=4)

                doc['landmark'] = landmark
        doc.pop('landmarks', None)
        # pprint.pprint(doc['landmark'], indent=2)
        return doc

    def string_to_json(self, source):
        try:
            load_input_json = json.loads(source)
        except ValueError, e:
            raise Exception("Could not parse '%s' as JSON: %s" % (source, e))
        return load_input_json

    def _json_path_search(self, json, expr):
        path = parse(expr)
        return path.find(json)

    def update_json(self, doc, matches, name, value, index=0, parent=False):
        load_input_json = doc
        datum_object = matches[int(index)]
        if not isinstance(datum_object, jsonpath.DatumInContext):
            raise Exception("Nothing found by the given json-path")
        path = datum_object.path
        if isinstance(path, jsonpath.Index):
            datum_object.context.value[datum_object.path.index][name] = value
        elif isinstance(path, jsonpath.Fields):
            datum_object.context.value[name] = value
        return load_input_json
