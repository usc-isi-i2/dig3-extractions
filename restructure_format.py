import json
from optparse import OptionParser
import codecs
from Normalize import N as NO
import time
from jsonpath_rw import parse, jsonpath

SEM_TYPES = 'semantic_types'
SEM_TYPE = 'semantic_type'
PHONE = 'phone'
AGE = 'age'
HAIR_C = 'hair_color'
ETHNICITY = 'ethnicity'
CITY = 'city'
FIELDS = 'fields'
probability_threshold = 0.5
# blacklist for inferlink fields on which we do not want to run normalizer
# because it's already run or can't be run
inferlink_blacklist  = ['phone', 'nothing']

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
                        obj['value'] = val.strip()
                        for key in sem_t.keys():
                            if key != 'value':
                                obj[key] = sem_t[key]
                        semantic_type_values[field].append(obj)
                    tokens[i][SEM_TYPE][j]['read'] = True
    for key in semantic_type_values.keys():
        semantic_type_values[key].sort(key=lambda x: x['probability'], reverse=True)
    return semantic_type_values


def create_field_objects(obj_dedup_semantic_types, value_type, values, field_name, normalize_conf, N):
    if isinstance(values, list):
        out = list()
        for val in values:
            o = create_field_object(obj_dedup_semantic_types, value_type, val, field_name, normalize_conf, N)
            if o[0]:
                if isinstance(o[0], list):
                    out.extend(o[0])
                else:
                    out.append(o[0])
        if len(out) > 0:
            return out, obj_dedup_semantic_types
        return None, obj_dedup_semantic_types
    else:
        return create_field_object(obj_dedup_semantic_types, value_type, values, field_name, normalize_conf, N)


def create_field_object(obj_dedup_semantic_types, value_type, value, field_name, normalize_conf, N):

    out = dict()
    debug = False
    if field_name == 'height' or field_name == 'weight' or field_name == 'location':
        debug = True
    if debug:
        print "Input value %s" % (value)
    if field_name in normalize_conf:
        f_conf = normalize_conf[field_name]
        func = getattr(N, f_conf['function'])
        start_time = time.time()
        o = func(value, f_conf)
        end_time = time.time() - start_time
        if end_time > .05:
            print "Time taken to normalize %s : %f" % (value['value'], end_time)
        if o:
            if debug:
                print "Output 1 %s" % (o)
            return check_if_value_exists(o, obj_dedup_semantic_types, field_name, value_type)
        else:
            out['name'] = value['value']
            if debug:
                print "Output 2 %s" % (out)
            return out, obj_dedup_semantic_types

    out['key'] = value['value']
    out['name'] = value['value']
    if debug:
        print "Output 3 %s" % (out)
    return out, obj_dedup_semantic_types


def check_if_value_exists(obj, dedup_list, field_name, value_type):
    out_list = list()
    if isinstance(obj, dict):
        obj = [obj]
    for o in obj:
        if o['name'] not in dedup_list[field_name][value_type]:
            dedup_list[field_name][value_type].append(o['name'])
            out_list.append(o)
    # print json.dumps(dedup_list)
    return (out_list, dedup_list) if len(out_list) > 0 else (None, dedup_list)


def add_objects_to_semantic_types_for_gui(obj_semantic_types, obj_dedup_semantic_types, semantic_type, values,
                                          value_type, normalize_conf, N):

    if semantic_type not in obj_dedup_semantic_types:
        obj_dedup_semantic_types[semantic_type] = dict()
    if 'strict' not in obj_dedup_semantic_types[semantic_type]:
        obj_dedup_semantic_types[semantic_type]['strict'] = list()
    if 'relaxed' not in obj_dedup_semantic_types[semantic_type]:
        obj_dedup_semantic_types[semantic_type]['relaxed'] = list()

    if semantic_type not in obj_semantic_types:
        obj_semantic_types[semantic_type] = dict()
    if 'strict' not in obj_semantic_types[semantic_type]:
        obj_semantic_types[semantic_type]['strict'] = list()
    if 'relaxed' not in obj_semantic_types[semantic_type]:
        obj_semantic_types[semantic_type]['relaxed'] = list()

    if value_type == 'strict':
        for val in values:
            if 'selected' in val \
                    or ('probability' in val and val['probability'] >= probability_threshold) \
                    or len(obj_semantic_types[semantic_type][value_type]) == 0:
                out, obj_dedup_semantic_types = create_field_objects(obj_dedup_semantic_types, value_type, val,
                                                                     semantic_type, normalize_conf, N)

                if out:
                    # print "1. Appending to %s %s %s" % (semantic_type, value_type, out)
                    if isinstance(out, list):
                        obj_semantic_types[semantic_type][value_type].extend(out)
                    else:
                        obj_semantic_types[semantic_type][value_type].append(out)
            else:
                out, obj_dedup_semantic_types = create_field_objects(obj_dedup_semantic_types, 'relaxed', val,
                                                                     semantic_type, normalize_conf, N)
                if out:
                    # print "2. Appending to %s %s %s" % (semantic_type, 'relaxed', out)
                    if isinstance(out, list):
                        obj_semantic_types[semantic_type]['relaxed'].extend(out)
                    else:
                        obj_semantic_types[semantic_type]['relaxed'].append(out)
    elif value_type == 'relaxed':
        out, obj_dedup_semantic_types = create_field_objects(obj_dedup_semantic_types, value_type, values,
                                                             semantic_type, normalize_conf, N)

        if out:
            # print "3. Appending to %s %s %s" % (semantic_type, value_type, out)
            obj_semantic_types[semantic_type][value_type].extend(out)
    return obj_semantic_types, obj_dedup_semantic_types


def consolidate_extractor_values(extraction):
    out = list()
    for values in extraction:
        result = values['result']
        if isinstance(result, list):
            out.extend(result)
        else:
            out.append(result)
    return out


def get_strict_crf_tokens(doc):
    strict_crf_tokens_paths = ['landmark.*.crf_tokens', 'extractors.content_strict.crf_tokens']
    strict_crf_tokens = list()
    for path in strict_crf_tokens_paths:
        expr = parse(path)
        matches = expr.find(doc)
        for match in matches:
            # print match.full_path
            strict_crf_tokens.extend(match.value)
    return strict_crf_tokens


def get_relaxed_crf_tokens(doc):
    relaxed_crf_tokens_paths = ['extractors.content_relaxed.crf_tokens']
    relaxed_crf_tokens = list()
    for path in relaxed_crf_tokens_paths:
        expr = parse(path)
        matches = expr.find(doc)
        for match in matches:
            # print match.full_path
            relaxed_crf_tokens.extend(match.value)
    return relaxed_crf_tokens


def handle_strict_data_extractions(doc, semantic_type_objs, obj_dedup_semantic_types, N):
    strict_de_paths = ['landmark.*.data_extractors', 'extractors.content_strict.data_extractors',
                       'extractors.title.data_extractors']
    for path in strict_de_paths:
        expr = parse(path)
        matches = expr.find(doc)
        for match in matches:
            de = match.value
            for key in de.keys():
                extraction = consolidate_extractor_values(de[key])
                semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                            semantic_type_objs, obj_dedup_semantic_types,
                            key, extraction, 'strict',
                            normalize_conf, N)

    # also process landmark extractions except title and description
    if 'landmark' in doc and doc['landmark']:
        landmark = doc['landmark']
        for l_key in landmark.keys():
            if l_key != 'inferlink_title' and l_key != 'inferlink_description':
                l_extraction_text = landmark[l_key]['text']
                # harcoded because type will be same for all values in this path
                key = l_extraction_text[0]['type']
                # print key
                if key not in inferlink_blacklist:
                    extraction = consolidate_extractor_values(l_extraction_text)
                    semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                        semantic_type_objs, obj_dedup_semantic_types,
                        key, extraction, 'strict',
                        normalize_conf, N)


    return semantic_type_objs, obj_dedup_semantic_types


def handle_relaxed_data_extractions(doc, semantic_type_objs, obj_dedup_semantic_types, N):
    relaxed_de_paths = ['extractors.content_relaxed.data_extractors']
    for path in relaxed_de_paths:
        expr = parse(path)
        matches = expr.find(doc)
        for match in matches:
            # print match.full_path
            de = match.value
            for key in de.keys():
                extraction = consolidate_extractor_values(de[key])
                semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                            semantic_type_objs, obj_dedup_semantic_types,
                            key, extraction, 'relaxed',
                            normalize_conf, N)
    return semantic_type_objs, obj_dedup_semantic_types


def consolidate_semantic_types(doc, normalize_conf, N):
    semantic_type_objs = dict()
    obj_dedup_semantic_types = dict()

    #  handle semantic types from strict crf tokens
    strict_crf_tokens = get_strict_crf_tokens(doc)
    strict_semantic_types = get_value_sem_type(strict_crf_tokens)
    if strict_semantic_types:
        for key in strict_semantic_types.keys():
            semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                semantic_type_objs, obj_dedup_semantic_types, key, strict_semantic_types[key], 'strict',
                normalize_conf, N)

    # handle semantic types from relaxed crf tokens
    relaxed_crf_tokens = get_relaxed_crf_tokens(doc)
    relaxed_semantic_types = get_value_sem_type(relaxed_crf_tokens)
    if relaxed_semantic_types:
        for key in relaxed_semantic_types.keys():
            semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                semantic_type_objs, obj_dedup_semantic_types,
                key, relaxed_semantic_types[key],
                'relaxed', normalize_conf, N)

    # handle strict data extractions
    semantic_type_objs, obj_dedup_semantic_types = handle_strict_data_extractions(doc, semantic_type_objs,
                                                                                  obj_dedup_semantic_types, N)

    # handle relaxed data extractions
    semantic_type_objs, obj_dedup_semantic_types = handle_relaxed_data_extractions(doc, semantic_type_objs,
                                                                                  obj_dedup_semantic_types, N)
    landmark = None
    if 'landmark' in doc:
        landmark = doc['landmark']

    if 'extractors' in doc:
        extractors = doc['extractors']

        # handle title first
        title = None
        if 'title' in extractors:
            title = extractors['title']['text'][0]['result'][0]
        elif landmark and 'inferlink_title' in landmark:
            title = landmark['inferlink_title']['text'][0]['result']
        if title:
            title = [title]
            semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(semantic_type_objs,
                                                                                                 obj_dedup_semantic_types,
                                                                                                 'title', title,
                                                                                                 'strict',
                                                                                                 normalize_conf, N)
            semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(semantic_type_objs,
                                                                                                 obj_dedup_semantic_types,
                                                                                                 'title', title,
                                                                                                 'relaxed',
                                                                                                 normalize_conf, N)
        # now strict semantic types
        if 'content_strict' in extractors:
            content_strict = extractors['content_strict']
            description = None
            if landmark and 'inferlink_description' in landmark:
                description = landmark['inferlink_description']['text'][0]['result']
            elif 'text' in content_strict:
                description = content_strict['text'][0]['result']
            if description:
                description = [description]
                semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(semantic_type_objs,
                                                                                                     obj_dedup_semantic_types,
                                                                                                     'description',
                                                                                                     description,
                                                                                                     'strict',
                                                                                                     normalize_conf, N)

        if 'content_relaxed' in extractors:
            content_relaxed = extractors['content_relaxed']
            description = None
            if 'text' in content_relaxed:
                description = content_relaxed['text'][0]['result']
            if description:
                description = [description]
                semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(semantic_type_objs,
                                                                                                     obj_dedup_semantic_types,
                                                                                                     'description',
                                                                                                     description,
                                                                                                     'relaxed',
                                                                                                     normalize_conf, N)

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
    # print hybrid_jaccard_config
    N_O = NO(hybrid_jaccard_config)
    normalize_conf = json.load(codecs.open(normalize_conf_file, 'r', 'utf-8'))

    lines = codecs.open(input_file, 'r').readlines()
    o = codecs.open(output_file, 'w')
    i = 1
    for line in lines:
        x = json.loads(line)
        s_t = time.time()
        print 'processing line # %d' % (i)
        o.write(json.dumps(consolidate_semantic_types(x, normalize_conf, N_O)))
        e_t = time.time() - s_t
        if e_t > 1.0:
            print "Time taken to normalize %s : %f" % (json.loads(line)['doc_id'], e_t)
        o.write('\n')
        i += 1
    o.close()