import json
import codecs
import sys
from optparse import OptionParser
from initClassifiers import ProcessClassifier
from initILP import ProcessILP
from hadoop.io import SequenceFile

def load_json_file(file_name):
    rules = json.load(codecs.open(file_name, 'r'))
    return rules


if __name__ == "__main__":
    parser = OptionParser()
    (c_options, args) = parser.parse_args()

    try:
        input_path = args[0]
        output_file = args[1]
        properties_file = args[2]
    except Exception as e:
        print "Usage error: python run_ilp_only.py <input> <output> <properties>"
        sys.exit()

    properties = load_json_file(properties_file)
    # Initialize the classifiers
    extraction_classifiers = ['city', 'ethnicity', 'hair_color', 'name', 'eye_color']
    classifier_processor = ProcessClassifier(extraction_classifiers)

    # Initialize the ILP engine
    ilp_processor = ProcessILP(properties)

    reader = SequenceFile.Reader(input_path)

    key_class = reader.getKeyClass()
    value_class = reader.getValueClass()

    key = key_class()
    value = value_class()

    position = reader.getPosition()
    o = codecs.open(output_file, 'w')
    while reader.next(key, value):
        jl = json.loads(value.toString())
        result_doc = classifier_processor.classify_extractions(jl)
        result_doc = ilp_processor.run_ilp(result_doc)
        o.write(json.dumps(result_doc) + '\n')
    o.close()
