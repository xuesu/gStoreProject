import codecs

import simplejson as json

input_fname = 'C:\Users\Administrator\Desktop\dbpedia170M.nt'


def read_and_filter_subject(infname=input_fname):
    my_subjects = set()
    with codecs.open(infname, 'r', encoding='utf-8') as fin:
        for line in fin:
            eles = [ele.strip() for ele in line.split('\t')]
            if eles[1] == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' \
                    and eles[2] in ['<http://dbpedia.org/ontology/MusicalWork>.', '<http://dbpedia.org/ontology/musicalArtist>.', '<http://dbpedia.org/ontology/musicalBand>.']:
                my_subjects.add(eles[0])
    return my_subjects


def read_and_extend_subject(subjects, infname=input_fname):
    subjects = set(subjects)
    with codecs.open(infname, 'r', encoding='utf-8') as fin:
        for line in fin:
            eles = [ele.strip() for ele in line.split('\t')]
            if subjects.intersection(eles):
                subjects.add(eles[0])
                subjects.add(eles[1])
    return subjects


def write_filtered_subject(subjects, infname=input_fname, outfname=""):
    with codecs.open(infname, 'r', encoding='utf-8') as fin:
        with codecs.open(outfname, "w", encoding="utf-8") as fout:
            for line in fin:
                eles = line.split('\t')
                if eles[0].strip() in subjects:
                    fout.write(line)


def filter_illgel_character(infname, outfname):
    legal_char = '01234567890()abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,./?;:\' !@#$%^&~`^&*90-_+=|{}[]<>\t\n\r\\'
    with codecs.open(infname, 'r', encoding='utf-8') as fin:
        with codecs.open(outfname, "w", encoding="utf-8") as fout:
            for line in fin:
                eles = [ele.strip() for ele in line.split('\t')]
                if eles[2][0] == '"':
                    eles[2] = eles[2].replace('"', '', 2)
                fl = [c for c in ''.join(eles) if c not in legal_char]
                if not fl:
                    fout.write(line)
                else:
                    print fl


# subject_st = read_and_filter_subject()
# subject_list = list(subject_st)
# with codecs.open("subjects_wab.json", "w", encoding="utf-8") as fout:
#     json.dump(subject_list, fout)
# write_filtered_subject(subject_st, outfname='subjects_wab.nt')
filter_illgel_character('subjects_wab.nt', 'subjects_wab_c.nt')
