import json
import time
from tldextract import tldextract
import codecs
import pprint
import re
from digReadabilityExtractor.readability_extractor import ReadabilityExtractor
from jsonpath_rw import parse, jsonpath
from digPhoneExtractor.phone_extractor import PhoneExtractor
from digAgeRegexExtractor.age_regex_helper import get_age_regex_extractor
from digDictionaryExtractor.populate_trie import populate_trie
from digDictionaryExtractor.dictionary_extractor import DictionaryExtractor
from digTokenizerExtractor.tokenizer_extractor import TokenizerExtractor
from digTableExtractor.table_extractor import TableExtractor
from digExtractor.extractor_processor import ExtractorProcessor
from digLandmarkExtractor.get_landmark_extractor_processors import get_multiplexing_landmark_extractor_processor
from landmark_extractor.extraction.Landmark import RuleSet
from digRegexExtractor.regex_extractor import RegexExtractor
from digExtractor.extractor import Extractor as SuperExtractor
from digHeightWeightExtractor.height_weight_extractor import HeightWeightExtractor


"""This is just for reference
inferlink_field_names = [
                         {'name': ['inferlink_name']},
                         {'posting-date': ['inferlink_posting-date', 'inferlink_posting-date-2',
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
    'posting-date': ['inferlink_posting-date', 'inferlink_posting-date-2',
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
    'gender': ['inferlink_gender']
}


fields_to_remove = ["crawl_data", "extracted_metadata"]
my_name_is_name_regex = re.compile('(?:my[\s]+name[\s]+is[\s]+([-a-z0-9@$!]+))', re.IGNORECASE)
name_filter_regex = re.compile('[a-z].*[a-z]')
# Initialize root extractors
readability_extractor_init = ReadabilityExtractor().set_metadata({'type': 'readability_high_recall'})
readability_extractor_rc_init = ReadabilityExtractor().set_recall_priority(False).set_metadata({'type': 'readability_low_recall'})
table_extractor_init = TableExtractor().set_metadata({'type': 'table'})
tokenizer_extractor = TokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True).set_metadata({'extractor': 'crf_tokenizer'})
# init sub root extractors
phone_extractor_init = PhoneExtractor().set_metadata({'extractor': 'phone', 'semantic_type': 'phone', 'input_type': ['tokens']}).set_source_type('text')
age_extracor_init = get_age_regex_extractor().set_metadata({'semantic_type': 'age', 'input_type': ['text']}).set_include_context(True)
"""setup a title extractor to get title from html"""
html_title_regex = re.compile(r'<title>(.*?)</title>', flags=re.IGNORECASE)

title_regex_extractor = RegexExtractor() \
    .set_regex(html_title_regex) \
    .set_metadata({'extractor': 'title_regex', 'type': 'title'}) \
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

name_dictionary_extractor_init = DictionaryExtractor() \
    .set_pre_filter(lambda x: name_filter_regex.match(x)) \
    .set_pre_process(lambda x: x.lower()) \
    .set_metadata({
    'extractor': 'dig_name_dictionary_extractor',
    'semantic_type': 'name',
    'input_type': ['tokens'],
    'type': 'dictionary',
    'properties_key': 'names_low_precision'
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

height_weight_extractor_init = HeightWeightExtractor()\
            .set_metadata({'extractor': 'height_weight',
                           'semantic_type': 'height_weight',
                           'input_type': ['text'],
                           'type': 'height_weight'})


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
    def execute_processor_chain(doc, extractor_processors):
        """Applies a sequence of ExtractorProcessors which wrap Extractors
        to a doc which will then contain all the extracted values"""
        for ep in extractor_processors:
            try:
                start_time = time.time()
                doc = ep.extract(Extractor.add_tld(Extractor.rename_key('_id', 'cdr_id', Extractor.remove_fields(doc, fields_to_remove))))
                time_taken = time.time() - start_time
                if time_taken > 5.0:
                    print "Extractor %s took %s seconds for %s" % (ep.get_name(), str(time_taken), doc['url'])

            except Exception as e:
                print e
                print "Extractor %s crashed." % ep.get_name()
                print "Document url %s" % doc['url']
        return doc

"""  ************  IMPORTANT  *************   """
""" Initialize the content and Data extractors here """
content_extractors = {
    'READABILITY_HIGH_RECALL': Extractor(readability_extractor_init, 'raw_content', 'extractors.content_relaxed.text'),
    'READABILITY_LOW_RECALL': Extractor(readability_extractor_rc_init, 'raw_content', 'extractors.content_strict.text'),
    # 'TABLE': Extractor(table_extractor_init, 'raw_content', 'extractors.tables.text'),
    'TITLE': Extractor(title_regex_extractor, 'raw_content', 'extractors.content_strict.title')
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
        data_extractors = [
            phone_extractor_init,
            age_extracor_init,
            city_dictionary_extractor_init,
            hair_color_dictionary_extractor_init,
            name_dictionary_extractor_init,
            ethnicities_dictionary_extractor_init,
            height_weight_extractor_init
        ]

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

    def buildTreeFromHtml(self, tokenizer=False):
        ep = []
        for key, extractor in self.content_extractors.iteritems():
            processor = ExtractorProcessor() \
                .set_input_fields(extractor.input) \
                .set_output_field(extractor.output) \
                .set_extractor(extractor.type) \
                .set_name(key)
            ep.append(processor)
        return ep

    def buildTokens(self, doc):
        ep = []
        jsonpath_expr = parse('extractors.*.text')
        values = [(str(match.full_path), match.value) for match in jsonpath_expr.find(doc)]
        for value in values:
            path = value[0]
            data = value[1][0]
            ep_type = data['type']

            # special for table to find internal tokens
            if ep_type == 'table':
                table_expr = parse('result.value.tables[*].rows[*].cells[*].text')
                table_values = [(str(match.full_path), match.value) for match in table_expr.find(data)]
                for table_value in table_values:
                    table_path = table_value[0]
                    ep_table_path = path + '[0].' + table_path
                    op = ep_table_path[:-5]
                    token_ep = ExtractorProcessor() \
                        .set_name('tokens') \
                        .set_input_fields(ep_table_path + '[0].result.value') \
                        .set_output_field(op + '.crf_tokens') \
                        .set_extractor(tokenizer_extractor)
                    ep.append(token_ep)
            else:
                op = '.'.join(path.split('.')[:-1])
                token_ep = ExtractorProcessor() \
                    .set_name('tokens') \
                    .set_input_fields(path + '[0].result.value') \
                    .set_output_field(op + '.crf_tokens') \
                    .set_extractor(tokenizer_extractor)
                ep.append(token_ep)
        return ep

    @staticmethod
    def process_inferlink_fields(doc):
        path = 'extractors.content_strict.data_extractors'
        for key in inferlink_data_fields.keys():
            inferlink_fields = inferlink_data_fields[key]
            metadata = {'extractor': 'inferlink', 'input_type': ['text']}
            for field in inferlink_fields:
                if field in doc:
                    print "Adding %s to %s" % (field, key)
                    doc = ProcessExtractor.run_identity_extractor(doc, field + "[*].result.value", path + "." + key, metadata, field )
                    # identity_extractor = LambdaExtractor(field).set_extract_function(lambda x: x) \
                    #     .set_metadata()
                    # identity_processor = ExtractorProcessor() \
                    #     .set_input_fields(field + "[*].result.value") \
                    #     .set_output_fields(path + "." + key).set_extractor(identity_extractor)
                    # doc = identity_processor.extract(doc)
        return doc

    @staticmethod
    def process_height_weight(doc):
        """
        Need to process height weight separately as there is one extractor to return both
        :param doc: with height_weight present in the path extractors.*.data_extractors.height_weight
        :return doc: with height_weight present in the path
                     extractors.*.data_extractors.height and extractors.*.data_extractors.height
        """
        """
        [
  {
    "extractor": "height_weight",
    "name": "height_weight",
    "input_type": [
      "text"
    ],
    "source": "extractors.content_strict.text[0].result.value",
    "result": {
      "value": {
        "height": {
          "foot": [
            "5'9\""
          ],
          "raw": [
            {
              "foot": 5,
              "inch": 9
            }
          ],
          "centimeter": [
            175
          ]
        }
      }
    },
    "type": "height_weight",
    "semantic_type": "height_weight"
  }
]
        """
        path = 'extractors.*.data_extractors.height_weight'
        jp_expr = parse(path)
        for match in jp_expr.find(doc):
            values = match.value
            source_path = str(match.full_path)
            op = '.'.join(source_path.split('.')[:-1])
            for value in values:
                val = value['result']['value']
                metadata = {'extractor': 'height_weight', 'input_type': ['text']}
                if 'height' not in val and 'weight' not in val:
                    print 'GO BACKKKKK'
                    return doc
                if 'height' in val:
                    field = 'height'
                    print "Adding %s to %s" % ('hw_height', field)
                    doc['t_height'] = val[field]
                    input_path = 't_height'
                    output_path = op + "." + field
                    doc = ProcessExtractor.run_identity_extractor(doc, input_path, output_path, metadata, 't_height')

                if 'weight' in val:
                    field = 'weight'
                    print "Adding %s to %s" % ('hw_weight', field)
                    doc['t_weight'] = val[field]
                    input_path = 't_weight'
                    output_path = op + "." + field
                    doc = ProcessExtractor.run_identity_extractor(doc, input_path, output_path, metadata, 't_weight')
        doc.pop('t_height', None)
        doc.pop('t_weight', None)
        return doc

    @staticmethod
    def run_identity_extractor(doc, input_field, output_field, metadata, semantic_type):
        identity_extractor = LambdaExtractor(semantic_type).set_extract_function(lambda x: x) \
            .set_metadata(metadata)
        identity_processor = ExtractorProcessor() \
            .set_input_fields(input_field) \
            .set_output_fields(output_field).set_extractor(identity_extractor)
        return identity_processor.extract(doc)

    def buildDataExtractors(self, doc):
        ep = []
        if self.landmark:
            ep.append(self.landmark)
        for extractor in self.data_extractors:
            metadata = extractor.get_metadata()
            inputs = metadata['input_type']
            output = metadata['semantic_type']

            for inp in inputs:
                print "building..", output, " extractor"
                input_suffix, expression = '', ''
                if inp == 'tokens':
                    input_suffix = '[0].result[0].value'
                    expression = 'extractors.*.tokens'
                elif inp == "text":
                    input_suffix = '[0].result.value'
                    expression = 'extractors.*.text'

                jsonpath_expr = parse(expression)
                values = [str(match.full_path) for match in jsonpath_expr.find(doc)]
                for value in values:
                    op = '.'.join(value.split('.')[:-1])
                    processor = ExtractorProcessor() \
                        .set_input_fields(value + input_suffix) \
                        .set_output_field(op + '.data_extractors.' + output) \
                        .set_extractor(extractor) \
                        .set_name(output)
                    ep.append(processor)
        return ep

    def buildDataExtractorsForTable(self, doc):
        ep = []
        for extractor in self.data_extractors:
            metadata = extractor.get_metadata()
            inputs = metadata['input_type']
            output = metadata['semantic_type']

            for inp in inputs:
                print "building..", output, " extractor for tables"
                input_suffix, expression = '', ''
                if inp == 'tokens':
                    input_suffix = '[0].result[0].value'
                    expression = 'extractors.tables.text.[*].result.value.tables[*].rows[*].cells[*].tokens'
                elif inp == "text":
                    input_suffix = '[0].result.value'
                    expression = 'extractors.tables.text.[*].result.value.tables[*].rows[*].cells[*].text'

                jsonpath_expr = parse(expression)
                values = [str(match.full_path) for match in jsonpath_expr.find(doc)]
                for value in values:
                    op = '.'.join(value.split('.')[:-1])
                    processor = ExtractorProcessor() \
                        .set_input_fields(value + input_suffix) \
                        .set_output_field(op + '.data_extractors.' + output) \
                        .set_extractor(extractor) \
                        .set_name(output)
                    ep.append(processor)
        return ep

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

    def buildSimpleTokensFromStructured(self, doc):
        expression = 'extractors.*.crf_tokens'
        jsonpath_expr = parse(expression)
        matches = jsonpath_expr.find(doc)
        i = 0
        for match in matches:
            val = match.value
            tokens = val[0]['result'][0]['value']
            new_simple_tokens = [tk['value'] for tk in tokens]
            data = [{"result": [{"value": new_simple_tokens}]}]
            doc = self.update_json(doc, matches, 'tokens', data, i, parent=True)
            i += 1
        # for tables
        table_expr = parse('extractors.tables.text.[*].result.value.tables[*].rows[*].cells[*].crf_tokens')
        table_matches = table_expr.find(doc)
        i = 0
        # table_values = [(str(match.full_path), match.value) for match in table_expr.find(doc)]
        for match in table_matches:
            val = match.value
            tokens = val[0]['result'][0]['value']
            new_simple_tokens = [tk['value'] for tk in tokens]
            data = [{"result": [{"value": new_simple_tokens}]}]
            doc = self.update_json(doc, table_matches, 'tokens', data, i, parent=True)
            i += 1
        return doc

    def annotateTokenToExtractions(self, tokens, extractions):
        for extractor, extraction in extractions.iteritems():
            if extractor in ['phone']:
                " ignoring phone annotation.."
                continue
            input_type = extraction[0]['input_type']
            if 'tokens' not in input_type:
                print "ignoring ", extractor, " as tokens not dependant.."
                continue
            data = extraction[0]['result']
            for extraction in data:
                start = extraction['context']['start']
                end = extraction['context']['end']
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

    def anotateDocTokens(self, doc, type=None):
        if type == 'Table':
            expression = 'extractors.tables.text.[*].result.value.tables[*].rows[*].cells[*].crf_tokens'
        else:
            expression = 'extractors.*.crf_tokens'
        jsonpath_expr = parse(expression)
        matches = jsonpath_expr.find(doc)
        i = 0
        for match in matches:
            val, path = match.value, str(match.full_path)
            tokens = val[0]['result'][0]['value']
            # find data extractors
            data_expression = '.'.join(path.split('.')[:-1]) + '.data_extractors'
            data_jsonpath_expr = parse(data_expression)

            results_expr = data_jsonpath_expr.find(doc)
            if len(results_expr) == 0:
                i += 1
                continue
            data_extractors_val = results_expr[0].value

            annotated_tokens = self.annotateTokenToExtractions(tokens, data_extractors_val)
            val[0]['result'][0]['value'] = annotated_tokens

            doc = self.update_json(doc, matches, 'crf_tokens', val, i, parent=True)
            i += 1
        return doc
