from optparse import OptionParser
from pyspark import SparkContext
import json


def process_images(x):

    if 'similarImageId' in  x:
        out_similar_images = list()
        similarImageId = x['similarImageId']
        if isinstance(similarImageId, dict):
            similarImageId = [similarImageId]
        for similarImage in similarImageId:
            similarImage.pop('a', None)
            similarImage.pop('uri', None)
            out_similar_images.append(similarImage)
        x['similarImageId'] = out_similar_images

    x.pop('uri', None)
    if 'isImagePartOf' in x:
        out_image_parts = list()
        isImagePartOf = x['isImagePartOf']
        if isinstance(isImagePartOf, dict):
            isImagePartOf = [isImagePartOf]

        for part in isImagePartOf:
            part.pop('a', None)
            part.pop('mainEntity', None)
            part['uri'] = strip_uri(part['uri'])
            if 'mentionsPhone' in part:
                mentionsPhone = part['mentionsPhone']
                part['mentionsPhone'] = list()
                for phone in mentionsPhone:
                    part['mentionsPhone'].append(strip_uri(phone))
            out_image_parts.append(part)
        x['isImagePartOf'] =  out_image_parts
    return x



def strip_uri(uri):
    return uri[uri.rfind('/')+1:]



if __name__ == "__main__":
    compression = "org.apache.hadoop.io.compress.GzipCodec"
    sc = SparkContext(appName="Memex Eval 2017 Images")
    parser = OptionParser()
    (c_options, args) = parser.parse_args()

    input_path = args[0]
    output_file = args[1]
    sc.sequenceFile(input_path).mapValues(json.loads).mapValues(process_images).\
        mapValues(json.dumps).saveAsSequenceFile(output_file, compressionCodecClass=compression)


#     x = """
#
#     {
# "a": "ImageObject",
# "similarImageId": [
# {
# "a": [
# "Role"
# ],
# "uri": "http://dig.isi.edu/ht/data/99EE76C6FEF1265F2ECDFB4CA1E4C5F2EFA1E305/role",
# "similarityScore": [
# "0.141372"
# ],
# "similarImageId": [
# "1C85B281511960206DF8D1B5B9A6A8A20DEC08FE"
# ]
# }
# ],
# "url": "https://s3.amazonaws.com/roxyimages/6a2f270eb07e150ffcf3e3b8f801bf5a03a95223.jpg",
# "uri": "http://dig.isi.edu/ht/data/99EE76C6FEF1265F2ECDFB4CA1E4C5F2EFA1E305",
# "isImagePartOf": [
# {
# "a": "WebPage",
# "mainEntity": {
# "a": "Offer",
# "uri": "http://dig.isi.edu/ht/data/offer/25D457BABF6B7D0D5CC6B22DD442E93A533B616833ADDA6B48FA18078B0FEAB9",
# "itemOffered": {
# "a": "AdultService",
# "uri": "http://dig.isi.edu/ht/data/adultservice/C045DF61D3CD7977EE6B2CD47BA5BB8ACDBC887F"
# }
# },
# "uri": "http://dig.isi.edu/ht/data/webpage/25D457BABF6B7D0D5CC6B22DD442E93A533B616833ADDA6B48FA18078B0FEAB9",
# "mentionsPhone": [
# "http://dig.isi.edu/ht/data/phone/1-5037190325"
# ]
# },
# {
# "a": "WebPage",
# "mainEntity": {
# "a": "Offer",
# "uri": "http://dig.isi.edu/ht/data/offer/35DB452977F83A8E24A1AF1C8D19192C812D105AC629460D5920B207ED641C47",
# "itemOffered": {
# "a": "AdultService",
# "uri": "http://dig.isi.edu/ht/data/adultservice/EAFD64600A6805293A62425408F7237453FC7D95"
# }
# },
# "uri": "http://dig.isi.edu/ht/data/webpage/35DB452977F83A8E24A1AF1C8D19192C812D105AC629460D5920B207ED641C47",
# "mentionsPhone": [
# "http://dig.isi.edu/ht/data/phone/1-5037190325"
# ]
# },
# {
# "a": "WebPage",
# "uri": "http://dig.isi.edu/ht/data/webpage/466111E04413E26EDFB7495D9362B0BDA898EB9D2CD46A770EFB0D3C5071F390"
# },
# {
# "a": "WebPage",
# "mainEntity": {
# "a": "Offer",
# "uri": "http://dig.isi.edu/ht/data/offer/25D457BABF6B7D0D5CC6B22DD442E93A533B616833ADDA6B48FA18078B0FEAB9",
# "itemOffered": {
# "a": "AdultService",
# "uri": "http://dig.isi.edu/ht/data/adultservice/C045DF61D3CD7977EE6B2CD47BA5BB8ACDBC887F"
# }
# },
# "uri": "http://dig.isi.edu/ht/data/webpage/25D457BABF6B7D0D5CC6B22DD442E93A533B616833ADDA6B48FA18078B0FEAB9",
# "mentionsPhone": [
# "http://dig.isi.edu/ht/data/phone/1-5037190325"
# ]
# },
# {
# "a": "WebPage",
# "mainEntity": {
# "a": "Offer",
# "uri": "http://dig.isi.edu/ht/data/offer/35DB452977F83A8E24A1AF1C8D19192C812D105AC629460D5920B207ED641C47",
# "itemOffered": {
# "a": "AdultService",
# "uri": "http://dig.isi.edu/ht/data/adultservice/EAFD64600A6805293A62425408F7237453FC7D95"
# }
# },
# "uri": "http://dig.isi.edu/ht/data/webpage/35DB452977F83A8E24A1AF1C8D19192C812D105AC629460D5920B207ED641C47",
# "mentionsPhone": [
# "http://dig.isi.edu/ht/data/phone/1-5037190325"
# ]
# }
# ],
# "identifier": "99EE76C6FEF1265F2ECDFB4CA1E4C5F2EFA1E305"
# }"""
#
#     print json.dumps(process_images(json.loads(x)))