import json
from optparse import OptionParser
import codecs
from Normalize import N as NO
import time

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
                        # if field == 'city':
                        #     print token
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
                        # obj['probability'] = sem_t['probability']
                        obj['value'] = val.strip()
                        # if 'selected' in sem_t:
                        #     obj['selected'] = True

                        for key in sem_t.keys():
                            if key != 'value':
                                obj[key] = sem_t[key]
                        semantic_type_values[field].append(obj)
                    tokens[i][SEM_TYPE][j]['read'] = True
    for key in semantic_type_values.keys():
        semantic_type_values[key].sort(key=lambda x: x['probability'], reverse=True)
        # values_only = (x['value'] for x in semantic_type_values[key])
        # semantic_type_values[key] = list(values_only)
    return semantic_type_values


def create_field_objects(obj_dedup_semantic_types, value_type, values, field_name, normalize_conf, N):
    if isinstance(values, list):
        out = list()
        for val in values:
            o = create_field_object(obj_dedup_semantic_types, value_type, val, field_name, normalize_conf, N)
            if o[0]:
                out.append(o[0])
        if len(out) > 0:
            return out, obj_dedup_semantic_types
        return None, obj_dedup_semantic_types
    else:
        return create_field_object(obj_dedup_semantic_types, value_type, values, field_name, normalize_conf, N)


def create_field_object(obj_dedup_semantic_types, value_type, value, field_name, normalize_conf, N):

    out = dict()
    # print "INPUT:", value
    if field_name in normalize_conf:
        f_conf = normalize_conf[field_name]
        func = getattr(N, f_conf['function'])
        start_time = time.time()
        o = func(value, f_conf)
        end_time = time.time() - start_time
        if end_time > .05:
            print "Time taken to normalize %s : %f" % (value['value'], end_time)
        if o:
            # print "Normalized output 1:", o
            if o['name'] not in obj_dedup_semantic_types[field_name][value_type]:
                obj_dedup_semantic_types[field_name][value_type].append(o['name'])
                # print "changing dedup:", json.dumps(obj_dedup_semantic_types)
                return o, obj_dedup_semantic_types
            return None, obj_dedup_semantic_types
        else:
            out['key'] = ''
            out['name'] = value['value']
            # print "Normalized output 2:", out
            return out, obj_dedup_semantic_types

    out['key'] = value['value']
    out['name'] = value['value']

    # print 'Not Normalized output: ', out
    # print "\n"
    # print "Normalized output 3 :", out
    return out, obj_dedup_semantic_types


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
        # print values
        for val in values:
            if 'selected' in val \
                    or ('probability' in val and val['probability'] >= probability_threshold) \
                    or len(obj_semantic_types[semantic_type][value_type]) == 0:
                out, obj_dedup_semantic_types = create_field_objects(obj_dedup_semantic_types, value_type, val,
                                                                     semantic_type, normalize_conf, N)

                if out:
                    print "1. Appending to %s %s %s" % (semantic_type, value_type, out)
                    obj_semantic_types[semantic_type][value_type].append(out)
            else:
                # print obj_semantic_types[semantic_type]['relaxed']
                # print val
                out, obj_dedup_semantic_types = create_field_objects(obj_dedup_semantic_types, 'relaxed', val,
                                                                     semantic_type, normalize_conf, N)
                if out:
                    print "2. Appending to %s %s %s" % (semantic_type, 'relaxed', out)
                    obj_semantic_types[semantic_type]['relaxed'].append(out)
    elif value_type == 'relaxed':
        out, obj_dedup_semantic_types = create_field_objects(obj_dedup_semantic_types, value_type, values,
                                                             semantic_type, normalize_conf, N)

        if out:
            print "3. Appending to %s %s %s" % (semantic_type, value_type, out)
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


def consolidate_semantic_types(doc, normalize_conf, N):
    semantic_type_objs = dict()
    obj_dedup_semantic_types = dict()

    if 'extractors' in doc:
        extractors = doc['extractors']
        if 'content_strict' in extractors:
            content_strict = extractors['content_strict']
            if 'crf_tokens' in content_strict:
                strict_crf_tokens = content_strict['crf_tokens'][0]['result'][0]['value']
                strict_semantic_types = get_value_sem_type(strict_crf_tokens)
                if strict_semantic_types:
                    for key in strict_semantic_types.keys():
                        semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                            semantic_type_objs, obj_dedup_semantic_types, key, strict_semantic_types[key], 'strict',
                            normalize_conf, N)
            if 'data_extractors' in content_strict:
                de_strict = content_strict['data_extractors']
                for key in de_strict.keys():
                    extraction = consolidate_extractor_values(de_strict[key])
                    # print key
                    # print doc['doc_id']
                    # print extraction
                    semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                        semantic_type_objs, obj_dedup_semantic_types,
                        key, extraction, 'strict',
                        normalize_conf, N)

            title = None
            if 'title' in content_strict:
                title = content_strict['title'][0]['result'][0]
            elif 'inferlink_title' in doc:
                title = doc['inferlink_title'][0]['result']
            if title:
                # print title
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
            description = None
            if 'inferlink_description' in doc:
                description = doc['inferlink_description'][0]['result']
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
            if 'crf_tokens' in content_relaxed:
                relaxed_crf_tokens = content_relaxed['crf_tokens'][0]['result'][0]['value']
                relaxed_semantic_types = get_value_sem_type(relaxed_crf_tokens)
                if relaxed_semantic_types:
                    for key in relaxed_semantic_types.keys():
                        semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                            semantic_type_objs, obj_dedup_semantic_types,
                            key, relaxed_semantic_types[key],
                            'relaxed', normalize_conf, N)
            if 'data_extractors' in content_relaxed:
                de_relaxed = content_relaxed['data_extractors']
                for key in de_relaxed.keys():
                    extraction = consolidate_extractor_values(de_relaxed[key])
                    semantic_type_objs, obj_dedup_semantic_types = add_objects_to_semantic_types_for_gui(
                        semantic_type_objs, obj_dedup_semantic_types,
                        key, extraction, 'relaxed',
                        normalize_conf, N)

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

    for line in lines:
        x = json.loads(line)
        s_t = time.time()
        o.write(json.dumps(consolidate_semantic_types(x, normalize_conf, N_O)))
        e_t = time.time() - s_t
        if e_t > 1.0:
            print "Time taken to normalize %s : %f" % (json.loads(line)['doc_id'], e_t)
        o.write('\n')
    o.close()