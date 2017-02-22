import json
from optparse import OptionParser
import codecs
from Normalize import N as NO

SEM_TYPES = 'semantic_types'
SEM_TYPE = 'semantic_type'
PHONE = 'phone'
AGE = 'age'
HAIR_C = 'hair_color'
ETHNICITY = 'ethnicity'
CITY = 'city'
FIELDS = 'fields'
probability_threshold = 0.5

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
                        if 'selected' in sem_t:
                            obj['selected'] = True
                        semantic_type_values[field].append(obj)
                    tokens[i][SEM_TYPE][j]['read'] = True
    for key in semantic_type_values.keys():
        semantic_type_values[key].sort(key=lambda x: x['probability'], reverse=True)
        # values_only = (x['value'] for x in semantic_type_values[key])
        # semantic_type_values[key] = list(values_only)
    return semantic_type_values


def create_field_objects(values, field_name, normalize_conf, N):
    if isinstance(values, list):
        out = list()
        for val in values:
            out.append(create_field_object(val, field_name, normalize_conf, N))
        return out
    else:
        return create_field_object(values, field_name, normalize_conf, N)


def if_exists(values, value_to_add):
    return value_to_add in list(x['key'] for x in values)


def create_field_object(value, field_name, normalize_conf, N):
    print "%s: %s" % (field_name, value)
    out = dict()
    if field_name in normalize_conf:
        f_conf = normalize_conf[field_name]
        func = getattr(N, f_conf['function'])
        if field_name == 'city':
            o = func(value)
        else:
            o = func(value['value'], f_conf)
        if o:
            print "Normalized output:", o
            print "\n"
            return o

    out['key'] = value['value']
    out['name'] = value['value']

    print 'Not Normalized output: ', out
    print "\n"

    return out


def add_objects_to_semantic_types_for_gui(obj_semantic_types, semantic_type, values, value_type, normalize_conf, N):
    if semantic_type not in obj_semantic_types:
        obj_semantic_types[semantic_type] = dict()
    if 'strict' not in obj_semantic_types[semantic_type]:
        obj_semantic_types[semantic_type]['strict'] = list()
    if 'relaxed' not in obj_semantic_types[semantic_type]:
        obj_semantic_types[semantic_type]['relaxed'] = list()

    if value_type == 'strict':
        # print values
        for val in values:
            if 'selected' in val \
                    or ('probability' in val and val['probability'] >= probability_threshold) \
                    or len(obj_semantic_types[semantic_type]['strict']) == 0:
                if not if_exists(obj_semantic_types[semantic_type]['strict'], val['value']):
                    obj_semantic_types[semantic_type]['strict'].append(create_field_objects(val, semantic_type,
                                                                                            normalize_conf, N))
            else:
                # print obj_semantic_types[semantic_type]['relaxed']
                # print val
                if not if_exists(obj_semantic_types[semantic_type]['relaxed'], val['value']):
                    obj_semantic_types[semantic_type]['relaxed'].append(create_field_objects(val, semantic_type,
                                                                                             normalize_conf, N))
    elif value_type == 'relaxed':
        for val in values:
            if not if_exists(obj_semantic_types[semantic_type]['relaxed'], val['value']):
                obj_semantic_types[semantic_type]['relaxed'].extend(create_field_objects(values, semantic_type,
                                                                                         normalize_conf, N))
    return obj_semantic_types


def consolidate_extractor_values(extraction):
    out = list()
    for values in extraction:
        result = values['result']
        if isinstance(result, list):
            out.extend(result)
        else:
            out.append(result)
    return out


def consolidate_semantic_types(doc, normalize_conf, N):
    semantic_type_objs = dict()

    if 'extractors' in doc:
        extractors = doc['extractors']
        if 'content_strict' in extractors:
            content_strict = extractors['content_strict']
            if 'crf_tokens' in content_strict:
                strict_crf_tokens = content_strict['crf_tokens'][0]['result'][0]['value']
                strict_semantic_types = get_value_sem_type(strict_crf_tokens)
                if strict_semantic_types:
                    for key in strict_semantic_types.keys():
                        semantic_type_objs = add_objects_to_semantic_types_for_gui(semantic_type_objs, key,
                                                                                   strict_semantic_types[key],
                                                                                   'strict', normalize_conf, N)
            if 'data_extractors' in content_strict:
                de_strict = content_strict['data_extractors']
                for key in de_strict.keys():
                    extraction = consolidate_extractor_values(de_strict[key])
                    # print key
                    # print doc['doc_id']
                    # print extraction
                    semantic_type_objs = add_objects_to_semantic_types_for_gui(semantic_type_objs, key, extraction,
                                                                               'strict', normalize_conf, N)

            title = None
            if 'title' in content_strict:
                title = content_strict['title'][0]['result'][0]
            elif 'inferlink_title' in doc:
                title = doc['inferlink_title'][0]['result']
            if title:
                # print title
                title = [title]
                semantic_type_objs = add_objects_to_semantic_types_for_gui(semantic_type_objs, 'title', title,
                                                                           'strict', normalize_conf, N)
                semantic_type_objs = add_objects_to_semantic_types_for_gui(semantic_type_objs, 'title', title,
                                                                           'relaxed', normalize_conf, N)
            description = None
            if 'inferlink_description' in doc:
                description = doc['inferlink_description'][0]['result']
            elif 'text' in content_strict:
                description = content_strict['text'][0]['result']
            if description:
                description = [description]
                semantic_type_objs = add_objects_to_semantic_types_for_gui(semantic_type_objs, 'description',
                                                                           description, 'strict', normalize_conf, N)

        if 'content_relaxed' in extractors:
            content_relaxed = extractors['content_relaxed']
            if 'crf_tokens' in content_relaxed:
                relaxed_crf_tokens = content_relaxed['crf_tokens'][0]['result'][0]['value']
                relaxed_semantic_types = get_value_sem_type(relaxed_crf_tokens)
                if relaxed_semantic_types:
                    for key in relaxed_semantic_types.keys():
                        semantic_type_objs = add_objects_to_semantic_types_for_gui(semantic_type_objs, key,
                                                                                   relaxed_semantic_types[key],
                                                                                   'relaxed', normalize_conf, N)
            if 'data_extractors' in content_relaxed:
                de_relaxed = content_relaxed['data_extractors']
                for key in de_relaxed.keys():
                    extraction = consolidate_extractor_values(de_relaxed[key])
                    semantic_type_objs = add_objects_to_semantic_types_for_gui(semantic_type_objs, key,
                                                                               extraction, 'relaxed', normalize_conf, N)

            description = None
            if 'text' in content_relaxed:
                description = content_relaxed['text'][0]['result']
            if description:
                description = [description]
                semantic_type_objs = add_objects_to_semantic_types_for_gui(semantic_type_objs, 'description',
                                                                           description, 'relaxed', normalize_conf, N)

    doc[FIELDS] = semantic_type_objs
    return doc


if __name__ == "__main__":
    parser = OptionParser()
    (c_options, args) = parser.parse_args()
    input_file = args[0]
    output_file = args[1]
    normalize_conf_file = args[2]
    hybrid_jaccard_conf_file = args[3]
    hybrid_jaccard_config = json.load(codecs.open(hybrid_jaccard_conf_file, 'r'))
    N_O = NO(hybrid_jaccard_config)
    normalize_conf = json.load(codecs.open(normalize_conf_file, 'r', 'utf-8'))

    lines = codecs.open(input_file, 'r').readlines()
    o = codecs.open(output_file, 'w')

    for line in lines:
        x = json.loads(line)
        o.write(json.dumps(consolidate_semantic_types(x, normalize_conf, N_O)))
        o.write('\n')
    o.close()