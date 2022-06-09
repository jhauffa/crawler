# Social Network Crawlers

This is a set of Java programs for retrieving data from online social network
services (Twitter, Facebook, LiveJournal) and for preprocessing existing
datasets (the Enron and HackingTeam e-mail collections).

These programs were used to build datasets that have been featured in several
academic publications. Most of these datasets cannot be made public, as doing so
would be in conflict with indiviual rights to privacy, local data protection
laws, and the terms of service of the platforms from which the data was
acquired. We provide the source code of the crawlers to create transparency
about the process of data acquisition and to make it easier to reproduce the
experiments described in these publications without access to the original data.

Web crawling strongly depends on the structure and layout of the target website
and is therefore very fragile. The two web crawlers in this repository (Facebook
and LiveJournal) were developed years ago and most likely no longer work
correctly. The Twitter crawler accesses tweets via version 1.1 of the official
API and will continue to work until Twitter retires that version. The e-mail
preprocessors operate on the
[EDRM v2](https://archive.org/details/edrm.enron.email.data.set.v2.xml)
distribution of the Enron corpus and the PST files in the HackingTeam data dump,
respectively.

## Contributors

The following people, listed in chronological order, were involved in the
development:

* Benjamin Koster
    - Twitter crawler
* Gregor Semmler ([GitHub](https://github.com/gregorsemmler))
    - Facebook crawler
* Felix Sonntag
    - extraction of the text body of websites mentioned in tweets
* Jan Hauffa
    - LiveJournal crawler, e-mail processing, overall maintenance

## Publications

A number of publications rely on data that was obtained using the crawling and
preprocessing routines in this repository. The following is an incomplete list:

* Jan Hauffa, Georg Groh: "A Comparative Temporal Analysis of User-Content-  
  Interaction in Social Media", 2019, 5th International Workshop on Social  
  Media World Sensors @ HT
* Jan Hauffa, Wolfgang Bräu, Georg Groh: "Detection of Topical Influence in  
  Social Networks via Granger-Causal Inference: A Twitter Case Study", 2019,  
  Workshop on Social Influence @ ASONAM
* Jan Hauffa, Benjamin Koster, Florian Hartl, Valeria Köllhofer, Georg Groh:  
  "Mining Twitter for an Explanatory Model of Social Influence", 2016, 2nd  
  International Workshop on Social Influence Analysis @ IJCAI
* Christoph Fuchs, Jan Hauffa, Georg Groh: "Does Friendship Matter? An  
  Analysis of Social Ties and Content Relevance in Twitter and Facebook",  
  2015, First Karlsruhe Service Summit Research Workshop
