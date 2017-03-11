import json
from optparse import OptionParser
import codecs
from Normalize import N as NO
from pyspark import SparkContext
import restructure_format as rf


if __name__ == "__main__":
    compression = "org.apache.hadoop.io.compress.GzipCodec"
    sc = SparkContext(appName="Memex Eval 2017 REFORMAT Workflow")

    parser = OptionParser()
    parser.add_option("-r", "--giantOakRisk", action="store", type="string", dest="giantOakRisk")
    (c_options, args) = parser.parse_args()
    input_file = args[0]
    output_file = args[1]
    normalize_conf_file = args[2]
    hybrid_jaccard_conf_file = args[3]
    gurobi_o = args[4]

    gurobi = False
    if gurobi_o == 'yes':
        gurobi = True


    giant_oak_risk_assessment = None
    giant_oak_file = c_options.giantOakRisk
    if giant_oak_file:
        giant_oak_risk_assessment = json.load(codecs.open(giant_oak_file, 'r'))

    hybrid_jaccard_config = json.load(codecs.open(hybrid_jaccard_conf_file, 'r'))
    # print hybrid_jaccard_config
    N_O = NO(hybrid_jaccard_config=hybrid_jaccard_config)
    normalize_conf = json.load(codecs.open(normalize_conf_file, 'r', 'utf-8'))

    input_rdd = sc.sequenceFile(input_file).mapValues(json.loads)
    processed_rdd = input_rdd.mapValues(lambda x: rf.consolidate_semantic_types(x, normalize_conf, N_O, gurobi))
    if giant_oak_risk_assessment:
        processed_rdd.mapValues(lambda x: rf.add_giant_oak_risk(x, giant_oak_risk_assessment))

    processed_rdd.mapValues(json.dumps).saveAsSequenceFile(output_file, compressionCodecClass=compression)
