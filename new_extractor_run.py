#!/usr/bin/env python
import json
import codecs
import sys
import os
import fnmatch
from optparse import OptionParser
from initExtractors import ProcessExtractor
from initClassifiers import ProcessClassifier
from initILP import ProcessILP


def load_json_file(file_name):
    rules = json.load(codecs.open(file_name, 'r', 'utf-8'))
    return rules


def jl_file_iterator(file):
    with codecs.open(file, 'r') as f:
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
    (c_options, args) = parser.parse_args()

    try:
        input_path = args[0]
        output_file = args[1]
        properties_file = args[2]
    except Exception as e:
        print "Usage error: python run.py <input> <output> <properties>"
        sys.exit()

    landmark_rules_file = c_options.landmarkRules
    landmark_rules = None
    if landmark_rules_file:
        landmark_rules = json.load(codecs.open(landmark_rules_file, 'r', 'utf-8'))

    # Init the extractors
    # content_extractors = ['READABILITY_HIGH_RECALL', 'READABILITY_LOW_RECALL', 'TABLE', 'TITLE']
    content_extractors = ['READABILITY_HIGH_RECALL', 'READABILITY_LOW_RECALL', 'TITLE']
    data_extractors = ['age', 'phone', 'city', 'ethnicity', 'hair_color', 'landmark', 'name', 'height_weight']
    extraction_classifiers = ['city', 'ethnicity', 'hair_color', 'name', 'eye_color']
    properties = load_json_file(properties_file)

    # Initialize only requires extractors
    pe = ProcessExtractor(content_extractors, data_extractors, properties, landmark_rules=landmark_rules)

    # Initialize the classifiers
    # classifier_processor = ProcessClassifier(extraction_classifiers)
    #
    # # Initialize the ILP engine
    # ilp_processor = ProcessILP(properties)

    # Build tree from raw content
    # get all processors for root extractors
    tree_eps = pe.buildTreeFromHtml()

    o = codecs.open(output_file, 'w', 'utf-8')
    i = 1
    for jl in jl_file_iterator(input_path):
        print '*' * 20, "Processing file, ", i, '*' * 20
        print "Building and running content extractors..."

        result_doc = ''
        result_doc = pe.execute_processor_chain(jl, tree_eps)

        # Build tokens for root extractors
        # print "Building and running tokenizer extractors..."
        eps = pe.buildTokens(result_doc)
        result_doc = pe.execute_processor_chain(result_doc, eps)
        # print "Storing simple tokens from crf tokens..."
        result_doc = pe.buildSimpleTokensFromStructured(pe.add_tld(result_doc))

        # print "Building data extractors..."
        eps = pe.buildDataExtractors(result_doc)
        # print "Running data extractors..."
        result_doc = pe.execute_processor_chain(result_doc, eps)
        # print "Done"

        # print "Building data extractors for tables..."
        eps = pe.buildDataExtractorsForTable(result_doc)
        # print "Running data extractors..."
        result_doc = pe.execute_processor_chain(result_doc, eps)
        # print "Done"

        result_doc = pe.process_inferlink_fields(result_doc)
        result_doc = pe.process_height_weight(result_doc)
        # annotate
        # print "Annotating tokens and data extractors..."
        # result_doc = pe.anotateDocTokens(result_doc)
        #
        # print "Annotating tokens and data extractors for table..."
        # result_doc = pe.anotateDocTokens(result_doc, type='Table')
        #
        # # Classifying the extractions using their context and appending the probabilities
        # print "Classifying the extractions..."
        #
        # result_doc = classifier_processor.classify_extractions(result_doc)
        #
        # # Formulating and Solving the ILP for the extractions
        # print "Formulating and Solving the ILP"
        # result_doc = ilp_processor.run_ilp(result_doc)

        print "Done.."
        print '*' * 20, " End ", '*' * 20
        o.write(json.dumps(result_doc) + '\n')
        i += 1
    o.close()

