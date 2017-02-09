# Commenting out the file since gurobi licence required to run the same


import json
import codecs
import re
from jsonpath_rw import parse, jsonpath

from digIlpRankings import ilp_extractions


TITLE_WEIGHT = 1.0
TEXT_WEIGHT = 0.5

class ProcessILP():
  """ Class to process ILP """
  def __init__(self, properties):
    self.ilp_formulation = ilp_extractions.ILPFormulation({
    'city-country':properties['city_country'],
    'city-state':properties['city_state'],
    'state-country': properties['state_country']
    })

  def run_ilp(self, doc):
    expression = 'extractors.*.crf_tokens'
    jsonpath_expr = parse(expression)
    matches = jsonpath_expr.find(doc)

    # TODO: Need to rewrite this so that it handles tokens from multiple sources
    for match in matches:
      val, path = match.value, str(match.full_path)
      
      tokens = val[0]['result'][0]['value']

      tokens_input = [{
        "tokens":tokens,
        "source":"text",
        "weight":TEXT_WEIGHT
      }]

      self.ilp_formulation.formulate_ILP(tokens_input)

      updated_tokens = tokens_input[0]['tokens']

      val[0]['result'][0]['value'] = updated_tokens

    return doc
    