from pyspark import SparkContext
import json
import codecs
import sys
import os
import time
import fnmatch
from optparse import OptionParser
from initExtractors2 import ProcessExtractor
# from initClassifiers import ProcessClassifier
# from initILP import ProcessILP

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

if __name__ == "__main__":
    compression = "org.apache.hadoop.io.compress.GzipCodec"
    sc = SparkContext(appName="Memex Eval 2017 Workflow")

    parser = OptionParser()
    parser.add_option("-l", "--landmarkRules", action="store", type="string", dest="landmarkRules")
    parser.add_option("-f", "--frenchEnglishWords", action="store", type="string", dest="frenchEnglishWords")
    parser.add_option("-p", "--numPartitions", dest="numPartitions", type="int", default=1000)
    (c_options, args) = parser.parse_args()

    try:
        input_path = args[0]
        output_file = args[1]
        properties_file = args[2]
        phase = args[3]
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
                       'weight', 'state', 'service', 'review_id', 'price', 'social_media_id', 'address', 'email',
                       'posting_date']
    extraction_classifiers = ['city', 'ethnicity', 'hair_color', 'name', 'eye_color']
    properties = load_json_file(properties_file)

    # Initialize only requires extractors
    pe = ProcessExtractor(content_extractors, data_extractors, properties, landmark_rules=landmark_rules,
                          french_english_words=french_english_words)

    # # Initialize the classifiers
    # classifier_processor = ProcessClassifier(extraction_classifiers)
    #
    # # Initialize the ILP engine
    # ilp_processor = ProcessILP(properties)
    input_rdd = sc.sequenceFile(input_path).mapValues(json.loads)
    print input_rdd.count()
    print input_rdd.filter(lambda x: 'landmark' in x[1]).count()
    if phase == '1':
        processed_rdd = input_rdd.mapValues(lambda x: pe.buildTreeFromHtml(x, {'raw_content': x['raw_content']}, levelKey='extractors', jsonPath=False)).mapValues(pe.process_inferlink_fields)
    if phase == '2':
        processed_rdd = input_rdd.mapValues(pe.process_ist_extractions).mapValues(pe.buildTokensAndDataExtractors)

    processed_rdd.mapValues(json.dumps).saveAsSequenceFile(output_file, compressionCodecClass=compression)
    #processed_rdd.mapValues(pe.buildTokensAndDataExtractors).mapValues(json.dumps).map(lambda x: x[1]).saveAsTextFile(output_file)
    # # Classifying the extractions using their context and appending the probabilities
    # print "Classifying the extractions..."
    # result_doc = classifier_processor.classify_extractions(result_doc)
    # #
    # # # Formulating and Solving the ILP for the extractions
    # print "Formulating and Solving the ILP"
    # result_doc = ilp_processor.run_ilp(result_doc
