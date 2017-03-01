# Commenting out the file since gurobi licence required to run the same


import json
import codecs
import re
from jsonpath_rw import parse, jsonpath

from digIlpRankings import ilp_extractions

TITLE_WEIGHT = 1.0
RELAXED_TEXT_WEIGHT = 0.4
STRICT_TEXT_WEIGHT = 0.5
LANDMARK_WEIGHT = 1.0
DEFAULT_WEIGHT = 0.5

class ProcessILP():
  """ Class to process ILP """
  def __init__(self, properties):
    self.ilp_formulation = ilp_extractions.ILPFormulation({
    'city-country':properties['city_country'],
    'city-state':properties['city_state'],
    'state-country': properties['state_country'],
    'city_alt': properties['city_alt'],
    'city_all': properties['city_all']
    })

  def _get_weight(self, path):
    if("landmark." in path):
      return LANDMARK_WEIGHT
    elif(".content_relaxed." in path):
      return RELAXED_TEXT_WEIGHT
    elif(".content_strict" in path):
      return STRICT_TEXT_WEIGHT
    elif(".title." in path):
      return TITLE_WEIGHT
    return DEFAULT_WEIGHT

  def run_ilp(self, doc):
    expression = '*.*.crf_tokens'
    jsonpath_expr = parse(expression)
    matches = jsonpath_expr.find(doc)

    tokens_input = []

    for match in matches:
      val, path = match.value, str(match.full_path)

      tokens_input.append({
        "tokens": val,
        "source": path,
        "weight": self._get_weight(path)
        })

    self.ilp_formulation.formulate_ILP(tokens_input)

    return doc
    