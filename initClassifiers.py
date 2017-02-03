import json
import codecs
import re
from jsonpath_rw import parse, jsonpath
import os

from digExtractionsClassifier import dig_extractions_classifier
import digExtractionsClassifier.utility.functions as utility_functions
from sklearn.externals import joblib

class ProcessClassifier():
  """ Class to process the classifiers """
  def __init__(self, extraction_classifiers):
    self.extraction_classifiers = extraction_classifiers
    self.embeddings_file = 'unigram-part-00000-v2.json'
    self.__initialize()

  def __initialize(self):
    """ Initialize classifiers """
    self.embeddings = utility_functions.load_embeddings(self.embeddings_file)
    self.extractors = []

    for classifier in self.extraction_classifiers:
      print "Setting up - "+classifier
      extractor = self.setup_classifier(classifier)
      self.extractors.append(extractor)

  def classify_extractions(self, doc):
    expression = 'extractors.*.crf_tokens'
    jsonpath_expr = parse(expression)
    matches = jsonpath_expr.find(doc)
    for match in matches:
      val, path = match.value, str(match.full_path)
      
      for extractor in self.extractors:

        tokens = val[0]['result'][0]['value']

        updated_tokens = extractor.classify(tokens)

        val[0]['result'][0]['value'] = updated_tokens

    return doc

  def setup_classifier(self, classification_field):
    directory = os.path.dirname(os.path.abspath(__file__))
    MODEL_FILE = classification_field+'.pkl'
    model_file_path = os.path.join(directory, 'resources', MODEL_FILE)

    model = self.load_model([model_file_path])

    return dig_extractions_classifier.DigExtractionsClassifier(model, classification_field, self.embeddings)

  def load_model(self, filenames):
    print filenames
    classifier = dict()
    # try:
    filename = utility_functions.get_full_filename(__file__, filenames[0])
    print filename
    classifier['model'] = joblib.load(filename)
    # except:
        # raise Exception('Model file not present')
    try:
      classifier['scaler'] = joblib.load(utility_functions.get_full_filename(__file__, filenames[1]))
      classifier['normalizer'] = joblib.load(utility_functions.get_full_filename(__file__, filenames[2]))
      classifier['k_best'] = joblib.load(utility_functions.get_full_filename(__file__, filenames[3]))
    except:
      pass
    return classifier

