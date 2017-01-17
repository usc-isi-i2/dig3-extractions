import json
import time
from tldextract import tldextract
from digReadabilityExtractor.readability_extractor import ReadabilityExtractor
from digExtractor.extractor_processor import ExtractorProcessor
from digTokenizerExtractor.tokenizer_extractor import TokenizerExtractor
from jsonpath_rw import parse, jsonpath
# from parse import DatumInContext, Index, Fields
from digPhoneExtractor.phone_extractor import PhoneExtractor
from digAgeRegexExtractor.age_regex_helper import get_age_regex_extractor
# from objectpath import *

fields_to_remove = ["crawl_data", "extracted_metadata"]
# Initialize root extractors
readability_extractor_init = ReadabilityExtractor()
readability_extractor_rc_init = ReadabilityExtractor().set_recall_priority(False)
tokenizer_extractor = TokenizerExtractor(recognize_linebreaks=True, create_structured_tokens=True).set_metadata({'extractor': 'crf_tokenizer'})
# init sub root extractors
phone_extractor_init = PhoneExtractor().set_metadata({'extractor': 'phone', 'semantic_type': 'phone', 'input_type': ['tokens']}).set_source_type('text')
age_extracor_init = get_age_regex_extractor().set_metadata({'semantic_type': 'age', 'input_type': ['text']}).set_include_context(True)
# city extractor


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
    'READABILITY_LOW_RECALL': Extractor(readability_extractor_rc_init, 'raw_content', 'extractors.content_strict.text')
}

data_extractors = [
    phone_extractor_init,
    age_extracor_init
]

""" ************** END INTIALIZATION ******************  """


class ProcessExtractor(Extractor):
  """ Class to process the document - Extend functions from Extractor class """
  def __init__(self, content_extractors, data_extractors):
    self.content_extractors = self.__initialize(content_extractors)
    self.data_extractors = self.__get_data_extractor(data_extractors)

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

  def __get_data_extractor(self, sub):
    """ Initialize all data extractors and return only extractors that are included
        in the execution request chain
    """
    res = []
    for extractor in data_extractors:
      metadata = extractor.get_metadata()
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
    values = [str(match.full_path) for match in jsonpath_expr.find(doc)]
    for value in values:
      op = '.'.join(value.split('.')[:-1])
      token_ep = ExtractorProcessor() \
          .set_name('tokens') \
          .set_input_fields(value + '[0].result.value') \
          .set_output_field(op + '.crf_tokens') \
          .set_extractor(tokenizer_extractor)
      ep.append(token_ep)
    return ep

  def buildDataExtractors(self, doc):
    ep = []
    for extractor in self.data_extractors:
      metadata = extractor.get_metadata()
      inputs = metadata['input_type']
      output = metadata['semantic_type']

      for inp in inputs:
        print inp, output
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
              .set_output_field(op + '.' + output) \
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
    # matches = self._json_path_search(load_input_json, expr)
    datum_object = matches[int(index)]
    if not isinstance(datum_object, jsonpath.DatumInContext):
      raise Exception("Nothing found by the given json-path")
    path = datum_object.path
    if isinstance(path, jsonpath.Index):
        # datum_object.context.value[datum_object.path.index] = value
        datum_object.context.value[name] = value
    elif isinstance(path, jsonpath.Fields):
        # datum_object.context.value[datum_object.path.fields[0]] = value
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
      self.update_json(doc, matches, 'tokens', data, i, parent=True)
      i += 1
    return doc
