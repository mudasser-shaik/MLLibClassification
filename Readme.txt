This is a Spark WordCount Example
Spark MachineLearning Classifictaion model used to address the StumpleUpon Data Challenge

https://www.kaggle.com/c/stumbleupon/data?train.tsv

$ head -1 stumpleUponTrain.tsv
"url"   "urlid" "boilerplate"   "alchemy_category"      "alchemy_category_score"        "avglinksize"   "commonlinkratio_1"     "commonlinkratio_2"     "commonlinkratio_3" "commonlinkratio_4"     "compression_ratio"     "embed_ratio"   "framebased"    "frameTagRatio" "hasDomainLink" "html_ratio"    "image_ratio"       "is_news"       "lengthyLinkDomain"     "linkwordscore" "news_front_page"       "non_markup_alphanum_characters"        "numberOfLinks" "numwords_in_url"   "parametrizedLinkRatio" "spelling_errors_ratio" "label"

$ sed 1d stumpleUponTrain.csv > train_NoHeader
