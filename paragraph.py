import itertools
import nltk
import numpy
import re
import math
import string
from collections import Counter


# from bigram import Bigram


class Paragraph:
    def __init__(self, doc_id, para=[]):
        """
            Constructor of self class

        """
        """ The meta-data of the document """
        self.doc_id = doc_id
        # self.para_no = para_no
        self.file_path = ""

        """ Unhandled raw self """
        self.tokenized_self = para
        self.flatten_self = self.regroup_tokens_to_self()
        self.sentences = nltk.sent_tokenize(self.regroup_tokens_to_self())

        """ Tagging the part of speech of each word in the self and count the frequency """
        self.tagged_tokens = nltk.pos_tag(self.tokenized_self)
        self.pos_counter = Counter(tag for word, tag in self.tagged_tokens)

        """ Extracting the word in self and count the length """
        self.words_list = [word for word in self.tokenized_self if re.match(r'.*\w', word)]
        self.words_length = [len(word) for word in self.words_list]
        self.low_case_words_list = [word.lower() for word in self.words_list]
        self.char_list = list(itertools.chain(*self.flatten_self))
        self.word_occurrence = self.get_dict_of_word_and_occurrence()

    def get_para_insert_query(self):
        return "INSERT INTO paragraph(doc_id, chapter_id, path) " \
               "VALUES ({}, currval('chapter_chapter_id_seq'), '{}');\n" \
            .format(self.doc_id, "somewhere")

    def regroup_tokens_to_self(self):
        self = "".join([" " + i if not i.startswith("'") and i not in string.punctuation else i for i in
                        self.tokenized_self]).strip()
        return self

    def get_dict_of_word_and_occurrence(self):
        word = []
        no_of_occurrence = []
        for item in sorted(set(self.low_case_words_list)):
            word.append(item)
            no_of_occurrence.append(self.low_case_words_list.count(item))
        return dict(zip(word, no_of_occurrence))

    def get_total_no_of_words(self):
        return len(self.low_case_words_list)

    def get_total_no_of_distinct_words(self):
        return len(set(self.low_case_words_list))

    """
    9 different kinds of vocabulary richness are calculated
    """

    def get_vocabulary_richness(self):
        try:
            return float(self.get_total_no_of_distinct_words()) / float(self.get_total_no_of_words())
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_K_vocabulary_richness(self):
        try:
            total = 0
            for val in self.word_occurrence.values():
                total += val * val
            return float(math.pow(10, 4) * (total - self.get_total_no_of_words())) / float(
                math.pow(self.get_total_no_of_words(), 2))
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_R_vocabulary_richness(self):
        try:
            return float(self.get_total_no_of_distinct_words()) / float(math.sqrt(self.get_total_no_of_words()))
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_C_vocabulary_richness(self):
        try:
            return float(math.log(self.get_total_no_of_distinct_words())) / float(
                math.log(self.get_total_no_of_words()))
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_H_vocabulary_richness(self):
        try:
            return float(100 * math.log(self.get_total_no_of_words())) / float(
                1 - self.word_occurrence.values().count(1) / self.get_total_no_of_distinct_words())
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_S_vocabulary_richness(self):
        try:
            return float(self.word_occurrence.values().count(2)) / float(self.get_total_no_of_distinct_words())
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_k_vocabulary_richness(self):
        try:
            return float(math.log(self.get_total_no_of_distinct_words())) / float(
                math.log(float(math.log(self.get_total_no_of_words()))))
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_LN_vocabulary_richness(self):
        try:
            return float(1 - math.pow(self.get_total_no_of_distinct_words(), 2)) / float(
                math.pow(self.get_total_no_of_distinct_words(), 2) * math.log(self.get_total_no_of_words()))
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_entropy(self):
        try:
            total = 0
            for val in self.word_occurrence.values():
                total += ((float(val) / float(self.get_total_no_of_words())) * math.log(
                    float(val) / float(self.get_total_no_of_words())))
            return -100 * float(total)
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_average_word_length(self):
        try:
            return float(sum(self.words_length)) / float(len(self.words_length))
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_stddev_of_word_length(self):
        mean = self.get_average_word_length()
        if mean == -1:
            return -1
        try:
            count = 0
            for num in self.words_length:
                count = (num - mean) * (num - mean)
            return math.sqrt(float(count) / float(len(self.words_length)))
        except ZeroDivisionError as e:
            return -1
        except ValueError as e:
            return -1

    def get_total_no_of_character(self):
        return len(self.char_list)

    def get_total_no_of_alpha_character(self):
        eng_list = [char for char in self.char_list if char in string.ascii_letters]  # re.match('[a-zA-Z]', char)]
        return len(eng_list)

    def get_total_no_of_uppercase_character(self):
        uppercase_list = [char for char in self.char_list if char in string.ascii_uppercase]  # char.isupper()]
        return len(uppercase_list)

    def get_total_no_of_lowercase_character(self):
        lowercase_list = [char for char in self.char_list if char in string.ascii_lowercase]  # char.islower()]
        return len(lowercase_list)

    def get_total_no_of_special_character(self):
        special_list = [char for char in self.char_list if char in string.punctuation]  # re.match('\W', char)]
        return len(special_list)

    def get_total_no_of_digital_character(self):
        digit_list = [char for char in self.char_list if char in string.digits]  # char.isdigit()]
        return len(digit_list)

    def get_total_no_of_whitespace_character(self):
        whitespace_list = [char for char in self.char_list if char in string.whitespace]
        return len(whitespace_list)

    def get_alpha_chars_ratio(self):
        try:
            return float(self.get_total_no_of_alpha_character()) / float(self.get_total_no_of_character())
        except:
            return 0
    def get_uppercase_chars_ratio(self):
        try:
            return float(self.get_total_no_of_uppercase_character()) / float(self.get_total_no_of_character())
        except:
            return 0
    def get_lowercase_chars_ratio(self):
        try:
            return float(self.get_total_no_of_lowercase_character()) / float(self.get_total_no_of_character())
        except:
            return 0

    def get_special_chars_ratio(self):
        try:
            return float(self.get_total_no_of_special_character()) / float(self.get_total_no_of_character())
        except:
            return 0

    def get_digital_chars_ratio(self):
        try:
            return float(self.get_total_no_of_digital_character()) / float(self.get_total_no_of_character())
        except:
            return 0

    def get_whitespace_chars_ratio(self):
        try:
            return float(self.get_total_no_of_whitespace_character()) / float(self.get_total_no_of_character())
        except:
            return 0

    def get_total_no_of_sentences(self):
        return len(self.sentences)

    def get_average_no_of_words_per_sentence(self):
        return numpy.mean([len(sen) for sen in self.sentences])

    def get_freq_of_nouns(self):
        return self.pos_counter['NN'] + self.pos_counter['NNS']

    def get_freq_of_proper_nouns(self):
        return self.pos_counter['NNP'] + self.pos_counter['NNPS']

    def get_freq_of_pronoun(self):
        return self.pos_counter['PRP'] + self.pos_counter['PRP$']

    def get_freq_of_ordinal_adj(self):
        return self.pos_counter['JJ']

    def get_freq_of_comparative_adj(self):
        return self.pos_counter['JJR']

    def get_freq_of_superlative_adj(self):
        return self.pos_counter['JJS']

    def get_freq_of_ordinal_adv(self):
        return self.pos_counter['RB']

    def get_freq_of_comparative_adv(self):
        return self.pos_counter['RBR']

    def get_freq_of_superlative_adv(self):
        return self.pos_counter['RBS']

    def get_freq_of_modal_auxiliary(self):
        return self.pos_counter['MD']

    def get_freq_of_base_form_verb(self):
        return self.pos_counter['VB'] + self.pos_counter['VBP'] + self.pos_counter['VPZ']

    def get_freq_of_past_verb(self):
        return self.pos_counter['VBD']

    def get_freq_of_presesnt_participle_verb(self):
        return self.pos_counter['VBG']

    def get_freq_of_past_participle_verb(self):
        return self.pos_counter['VBN']

    def get_freq_of_particle(self):
        return self.pos_counter['RP']

    def get_freq_of_wh_words(self):
        return self.pos_counter['WDT'] + self.pos_counter['WP'] + self.pos_counter['WP$'] + self.pos_counter['WRB']

    def get_freq_of_conjunction(self):
        return self.pos_counter['CC']

    def get_freq_of_numerical(self):
        return self.pos_counter['CD']

    def get_freq_of_determiner(self):
        return self.pos_counter['DT'] + self.pos_counter['PDT']

    def get_freq_of_existential_there(self):
        return self.pos_counter['EX']

    def get_freq_of_existential_to(self):
        return self.pos_counter['TO']

    def get_freq_of_preposition(self):
        return self.pos_counter['IN']

    def get_freq_of_genitive_marker(self):
        return self.pos_counter['POS']

    def get_freq_of_quotation(self):
        return self.pos_counter['``']

    def get_freq_of_comma(self):
        return self.pos_counter[',']

    def get_freq_of_sen_terminator(self):
        return self.pos_counter['.']

    def get_freq_of_symbol(self):
        return self.pos_counter['SYM']

    # def get_bigrams(self):
    #    bigram_list = []
    #    for bigram in nltk.bigrams(self.low_case_words_list):
    #     bigram_list.append(Bigram(self.doc_id, bigram))
    #    return bigram_list

    def get_stylo_list(self):
        stylo_list = []
        stylo_list.append(self.get_total_no_of_words())
        stylo_list.append(self.get_total_no_of_distinct_words())
        stylo_list.append(self.get_average_word_length())
        stylo_list.append(self.get_stddev_of_word_length())
        stylo_list.append(self.get_vocabulary_richness())
        stylo_list.append(self.get_K_vocabulary_richness())
        stylo_list.append(self.get_R_vocabulary_richness())
        stylo_list.append(self.get_C_vocabulary_richness())
        stylo_list.append(self.get_H_vocabulary_richness())
        stylo_list.append(self.get_S_vocabulary_richness())
        stylo_list.append(self.get_k_vocabulary_richness())
        stylo_list.append(self.get_LN_vocabulary_richness())
        stylo_list.append(self.get_entropy())
        stylo_list.append(self.get_average_word_length())
        stylo_list.append(self.get_stddev_of_word_length())
        stylo_list.append(self.get_total_no_of_character())
        stylo_list.append(self.get_total_no_of_alpha_character())
        stylo_list.append(self.get_total_no_of_uppercase_character())
        stylo_list.append(self.get_total_no_of_lowercase_character())
        stylo_list.append(self.get_total_no_of_special_character())
        stylo_list.append(self.get_total_no_of_digital_character())
        stylo_list.append(self.get_total_no_of_whitespace_character())
        stylo_list.append(self.get_alpha_chars_ratio())
        stylo_list.append(self.get_uppercase_chars_ratio())
        stylo_list.append(self.get_lowercase_chars_ratio())
        stylo_list.append(self.get_special_chars_ratio())
        stylo_list.append(self.get_digital_chars_ratio())
        stylo_list.append(self.get_whitespace_chars_ratio())
        stylo_list.append(self.get_total_no_of_sentences())
        stylo_list.append(self.get_average_no_of_words_per_sentence())
        stylo_list.append(self.get_freq_of_nouns())
        stylo_list.append(self.get_freq_of_proper_nouns())
        stylo_list.append(self.get_freq_of_pronoun())
        stylo_list.append(self.get_freq_of_ordinal_adj())
        stylo_list.append(self.get_freq_of_comparative_adj())
        stylo_list.append(self.get_freq_of_superlative_adj())
        stylo_list.append(self.get_freq_of_ordinal_adv())
        stylo_list.append(self.get_freq_of_comparative_adv())
        stylo_list.append(self.get_freq_of_superlative_adv())
        stylo_list.append(self.get_freq_of_modal_auxiliary())
        stylo_list.append(self.get_freq_of_base_form_verb())
        stylo_list.append(self.get_freq_of_past_verb())
        stylo_list.append(self.get_freq_of_presesnt_participle_verb())
        stylo_list.append(self.get_freq_of_past_participle_verb())
        stylo_list.append(self.get_freq_of_particle())
        stylo_list.append(self.get_freq_of_wh_words())
        stylo_list.append(self.get_freq_of_conjunction())
        stylo_list.append(self.get_freq_of_numerical())
        stylo_list.append(self.get_freq_of_determiner())
        stylo_list.append(self.get_freq_of_existential_there())
        stylo_list.append(self.get_freq_of_existential_to())
        stylo_list.append(self.get_freq_of_preposition())
        stylo_list.append(self.get_freq_of_genitive_marker())
        stylo_list.append(self.get_freq_of_quotation())
        stylo_list.append(self.get_freq_of_comma())
        stylo_list.append(self.get_freq_of_sen_terminator())
        stylo_list.append(self.get_freq_of_symbol())
        return stylo_list
