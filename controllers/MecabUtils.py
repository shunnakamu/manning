#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MeCab


def wakati_gaki(text, remove_joshi_flg=False, remove_full_space=True, return_type=False, remove_verb=False):
    mecabrc_tagger = MeCab.Tagger("mecabrc")
    if type(text) == unicode:
        encoded_text = text.strip().encode('utf-8')
    else:
        encoded_text = text.strip()
    res = mecabrc_tagger.parseToNode(encoded_text)
    wakati_list = []
    while res:
        if res.surface != "" and \
            (not remove_joshi_flg or res.feature.split(",")[0] != "助詞") and\
            (not remove_verb or res.feature.split(",")[0] != "動詞") and\
            (not remove_full_space or (
                res.feature.split(",")[0] != "記号" and res.feature.split(",")[1] != "空白")
             ):
            if return_type:
                wakati_list.append({
                    "morpheme": res.surface.decode('utf-8'),
                    "type": [x.decode("utf-8") for x in res.feature.split(",")[:6]],
                    "sounds": [x.decode("utf-8") for x in res.feature.split(",")[6:]],
                })
            else:
                wakati_list.append(res.surface.decode('utf-8'))
        res = res.next
    return wakati_list
