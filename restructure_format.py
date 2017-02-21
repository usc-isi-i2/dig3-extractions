import json
from optparse import OptionParser
import codecs

SEM_TYPES = 'semantic_types'
SEM_TYPE = 'semantic_type'
PHONE = 'phone'
AGE = 'age'
HAIR_C = 'hair_color'
ETHNICITY = 'ethnicity'
CITY = 'city'
FIELDS = 'fields'


def update_semantic_types_token(token, offset):
    sem_types = token[SEM_TYPE]
    for sem_t in sem_types:
        if sem_t['offset'] == offset:
            sem_t['read'] = True
    return sem_types


def get_value_sem_type(tokens):
    """
    :param tokens: list of annotated tokens
    :return: an object with found values for all given semantic types
    """
    semantic_type_values = dict()

    for i in range(0, len(tokens)):
        token = tokens[i]
        if SEM_TYPE in token:
            sem_types = token[SEM_TYPE]
            for j in range(0, len(sem_types)):
                sem_t = sem_types[j]
                # only process this token, if its not already read by a previous iteration
                if 'read' not in sem_t:
                    # print token
                    # print sem_t
                    if 'length' in sem_t and 'probability' in sem_t and sem_t['probability'] > 0.2:
                        # this token is the first of 'length' values for this field
                        length = sem_t['length']
                        field = sem_t['type']
                        val = token['value']
                        length -= 1  # read the remaining length - 1 tokens to contruct the full value
                        offset = sem_t['offset']
                        temp_i = i
                        while length > 0 :
                            temp_i += 1
                            offset += 1
                            val += ' ' + tokens[temp_i]['value']
                            updated_sem_types = update_semantic_types_token(tokens[temp_i], offset)
                            tokens[temp_i][SEM_TYPE] = updated_sem_types
                            length -= 1

                        if field not in semantic_type_values:
                            semantic_type_values[field] = list()
                        obj = dict()
                        obj['probability'] = sem_t['probability']
                        obj['value'] = val.strip()
                        semantic_type_values[field].append(obj)
                    tokens[i][SEM_TYPE][j]['read'] = True
    for key in semantic_type_values.keys():
        semantic_type_values[key].sort(key=lambda x: x['probability'], reverse=True)
        values_only = (x['value'] for x in semantic_type_values[key])
        semantic_type_values[key] = list(values_only)
    return semantic_type_values

def create_field_objects(values, field_name):
    if isinstance(values, list):
        out = list()
        for val in values:
            out.append(create_field_object(val, field_name))
        return out
    else:
        return create_field_object(values, field_name)

def create_field_object(value, field_name):
    # TODO  according to field name, create different objects
    out = dict()
    out['key'] = value
    out['name'] = value
    return out

def consolidate_semantic_types(doc):
    strict_semantic_types = None
    relaxed_semantic_types = None
    if 'extractors' in doc and 'content_strict' in doc['extractors'] and 'crf_tokens' in doc['extractors']['content_strict']:
        strict_crf_tokens = doc['extractors']['content_strict']['crf_tokens'][0]['result'][0]['value']
        strict_semantic_types = get_value_sem_type(strict_crf_tokens)
    if 'extractors' in doc and 'content_relaxed' in doc['extractors'] and 'crf_tokens' in doc['extractors']['content_relaxed']:
        relaxed_crf_tokens = doc['extractors']['content_relaxed']['crf_tokens'][0]['result'][0]['value']
        relaxed_semantic_types = get_value_sem_type(relaxed_crf_tokens)

    semantic_type_objs = dict()

    if strict_semantic_types:
        for key in strict_semantic_types.keys():
            field_val = strict_semantic_types[key]
            print field_val
            if key not in semantic_type_objs:
                semantic_type_objs[key] = dict()
            if 'strict' not in semantic_type_objs[key]:
                semantic_type_objs[key]['strict'] = list()
            if 'relaxed' not in semantic_type_objs[key]:
                semantic_type_objs[key]['relaxed'] = list()
            if len(semantic_type_objs[key]['strict']) == 0:
                semantic_type_objs[key]['strict'].append(create_field_objects(field_val[0], key))
            else:
                if len(field_val) > 1:
                    semantic_type_objs[key]['relaxed'].extend(field_val[1:], key)
    if relaxed_semantic_types:
        for key in relaxed_semantic_types.keys():
            field_val = relaxed_semantic_types[key]
            if key not in semantic_type_objs:
                semantic_type_objs[key] = dict()
            if 'relaxed' not in semantic_type_objs[key]:
                semantic_type_objs[key]['relaxed'] = list()
            semantic_type_objs[key]['relaxed'].extend(create_field_objects(field_val, key))
    doc[FIELDS] = semantic_type_objs
    return doc


if __name__ == "__main__":
    parser = OptionParser()
    (c_options, args) = parser.parse_args()
    input_file = args[0]
    output_file = args[1]

    lines = codecs.open(input_file, 'r').readlines()
    o = codecs.open(output_file, 'w')

    for line in lines:
        x = json.loads(line)
        o.write(json.dumps(consolidate_semantic_types(x)))
        o.write('\n')
    o.close()