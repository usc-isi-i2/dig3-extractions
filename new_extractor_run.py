#!/usr/bin/env python
import json
import codecs
import sys
from jsonpath_rw import jsonpath, parse
import os
import fnmatch
# import time
# from optparse import OptionParser
from initExtractors import ProcessExtractor


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

    input_path = sys.argv[1]
    output_file = sys.argv[2]

    # Init the extractors
    content_extractors = ['READABILITY_HIGH_RECALL', 'READABILITY_LOW_RECALL']
    data_extractors = ['age', 'phone']
    # Initialize only requires extractors
    pe = ProcessExtractor(content_extractors, data_extractors)
    # Build tree from raw content
    # get all processors for root extractors
    eps = pe.buildTreeFromHtml()

    o = codecs.open(output_file, 'w', 'utf-8')
    for jl in jl_file_iterator(input_path):
        result_doc = pe.execute_processor_chain(jl, eps)
        result_doc['raw_content'] = "..."

        # Build tokens for root extractors
        eps = pe.buildTokens(result_doc)
        result_doc = pe.execute_processor_chain(result_doc, eps)
        result_doc = pe.buildSimpleTokensFromStructured(result_doc)

        eps = pe.buildDataExtractors(result_doc)
        result_doc = pe.execute_processor_chain(result_doc, eps)

        # annotate
        # ranking

        o.write(json.dumps(result_doc) + '\n')
    o.close()

    # parser = OptionParser()
    # parser.add_option("-l", "--landmarkRules", action="store", type="string", dest="landmarkRules")
    # (c_options, args) = parser.parse_args()

    # input_path = args[0]
    # output_file = args[1]
    # properties_file = args[2]
    # landmark_rules_file = c_options.landmarkRules

    # properties = load_json_file(properties_file)
    # landmark_rules = None
    # if landmark_rules_file:
    #     landmark_rules = load_json_file(landmark_rules_file)