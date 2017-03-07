#!/usr/bin/env python
import json
import codecs
import sys
import os
import time
import fnmatch
from optparse import OptionParser
from initExtractors2 import ProcessExtractor
from initClassifiers import ProcessClassifier
from initILP import ProcessILP
from hadoop.io import SequenceFile


def load_json_file(file_name):
    rules = json.load(codecs.open(file_name, 'r', 'utf-8'))
    return rules


def jl_file_iterator(file):
    with codecs.open(file, 'r', 'utf-8') as f:
        for line in f:
            document = json.loads(line)
            yield document


def jl_path_iterator(file_path):
    abs_file_path = os.path.abspath(file_path)
    if os.path.isdir(abs_file_path):
        for file in os.listdir(abs_file_path):
            if fnmatch.fnmatch(file, '*.jl'):
                yield os.path.join(abs_file_path, file)

    else:
        yield abs_file_path

#
# todo force flag to check path exists or not

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-l", "--landmarkRules", action="store", type="string", dest="landmarkRules")
    parser.add_option("-f", "--frenchEnglishWords", action="store", type="string", dest="frenchEnglishWords")
    (c_options, args) = parser.parse_args()

    try:
        input_path = args[0]
        output_file = args[1]
        properties_file = args[2]
    except Exception as e:
        print "Usage error: python run.py <input> <output> <properties>"
        sys.exit()

    french_english_words_file = c_options.frenchEnglishWords
    french_english_words = None
    if french_english_words_file:
        french_english_words = json.load(codecs.open(french_english_words_file, 'r'))

    landmark_rules_file = c_options.landmarkRules
    landmark_rules = None
    if landmark_rules_file:
        landmark_rules = json.load(codecs.open(landmark_rules_file, 'r', 'utf-8'))
        # print landmark_rules['eroticmugshots.com']

    # Init the extractors
    content_extractors = ['READABILITY_HIGH_RECALL', 'READABILITY_LOW_RECALL', 'TABLE', 'TITLE']
    data_extractors = ['age', 'phone', 'city', 'ethnicity', 'hair_color', 'eye_color', 'name', 'landmark', 'height',
                       'weight', 'state', 'service', 'review_id', 'price', 'social_media_id', 'address']
    extraction_classifiers = ['city', 'ethnicity', 'hair_color', 'name', 'eye_color']
    properties = load_json_file(properties_file)

    # Initialize only requires extractors
    pe = ProcessExtractor(content_extractors, data_extractors, properties, landmark_rules=landmark_rules, french_english_words=french_english_words)

    # Initialize the classifiers
    classifier_processor = ProcessClassifier(extraction_classifiers)

    # Initialize the ILP engine
    ilp_processor = ProcessILP(properties)

    o = codecs.open(output_file, 'w', 'utf-8')
    i = 1
    reader = SequenceFile.Reader(input_path)

    key_class = reader.getKeyClass()
    value_class = reader.getValueClass()

    key = key_class()
    value = value_class()

    position = reader.getPosition()
    while reader.next(key, value):

        jl = json.loads(value.toString())
        print '*' * 20, "Processing file, ", i, '*' * 20
        start_time = time.time()
        print "Building and running content extractors..."

        result_doc = ''
        # step 1
        tree_inputs = {'raw_content': jl['raw_content']}
        result_doc = pe.buildTreeFromHtml(jl, tree_inputs, levelKey='extractors', jsonPath=False)
        time_taken = time.time() - start_time
        print "Total time for content(Readability + table): ", time_taken

        result_doc = pe.process_inferlink_fields(result_doc)

        start_time_mid = time.time()
        result_doc = pe.buildTokensAndDataExtractors(result_doc)
        time_taken = time.time() - start_time_mid
        print "Total time for tokenizing, data extractors and annotation: ", time_taken

        # Classifying the extractions using their context and appending the probabilities
        print "Classifying the extractions..."
        result_doc = classifier_processor.classify_extractions(result_doc)
        #
        # # Formulating and Solving the ILP for the extractions
        print "Formulating and Solving the ILP"
        result_doc = ilp_processor.run_ilp(result_doc)

        time_taken = time.time() - start_time
        print "Total Time: ", time_taken
        print "Done.."
        print '*' * 20, " End ", '*' * 20
        position = reader.getPosition()
        o.write(json.dumps(result_doc) + '\n')

        i += 1
    o.close()
    reader.close()
