# Databricks notebook source exported at Tue, 16 Feb 2016 15:01:26 UTC
# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Wikipedia: ETL
# MAGIC  
# MAGIC This lab explains the process that was used to obtain Wikipedia data and transform it into a more usable form for analysis in Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL Background
# MAGIC  
# MAGIC Wikipedia data from the [August 5, 2015](https://dumps.wikimedia.org/enwiki/20150805/) enwiki dump on dumps.wikimedia.org.  Using the file: `enwiki-20150805-pages-articles-multistream.xml.bz2`  The file was uncompressed, parsed to pull out the XML `<page>` tags, and parsed again to retrieve several fields as JSON.  The JSON was stored one JSON string per line so that Spark could easily load the JSON and write out a parquet file.  The resulting parquet file was downsampled to keep the dataset small for the labs.

# COMMAND ----------

wikiSample = """<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.10/ http://www.mediawiki.org/xml/export-0.10.xsd" version="0.10" xml:lang="en">
  <siteinfo>
    <sitename>Wikipedia</sitename>
    <dbname>enwiki</dbname>
    <base>https://en.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.26wmf16</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      ...
  <page>
    <title>AccessibleComputing</title>
    <ns>0</ns>
    <id>10</id>
    <redirect title="Computer accessibility" />
    <revision>
      <id>631144794</id>
      <parentid>381202555</parentid>
      <timestamp>2014-10-26T04:50:23Z</timestamp>
      <contributor>
        <username>Paine Ellsworth</username>
        <id>9092818</id>
      </contributor>
      <comment>add [[WP:RCAT|rcat]]s</comment>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text xml:space="preserve">#REDIRECT [[Computer accessibility]]{{Redr|move|from CamelCase|up}}</text>
      <sha1>4ro7vvppa5kmm0o1egfjztzcwd0vabw</sha1>
    </revision>
  </page>
  ...
</mediawiki>"""
import cgi
displayHTML('<pre>{0}</pre>'.format(cgi.escape(wikiSample, True)))

# COMMAND ----------

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError
from pyspark.sql import Row

def parse_xml_to_dict(xmlString):
    data = {"title": None, "redirect_title": None, "timestamp": None, "last_contributor_username": None, "text": None}

    try:
        root = ET.fromstring(xmlString.encode('utf-8'))

        title = root.find("title")
        if title is not None:
            data["title"] = title.text
        redirect = root.find("redirect")
        if redirect is not None:
            data["redirect_title"] = redirect.attrib["title"]
        revision = root.find("revision")
        if revision is not None:
            timestamp = revision.find("timestamp")
            data["timestamp"] = timestamp.text
        contributor = revision.find("contributor")
        if contributor is not None:
            username = contributor.find("username")
        if username is not None:
            data["last_contributor_username"] = username.text
        text = revision.find("text")
        if text is not None and text.text is not None:
            data["text"] = text.text.replace("\\n", " ")
    except ParseError:
        data['title'] = '<PARSE ERROR>'

    return data #Row(**dict)

# COMMAND ----------

import codecs
import json
from xml.etree.ElementTree import ParseError

wikiData = codecs.open('/mnt/data_quick/wiki/extract/enwiki-20150805-pages-articles-multistream.xml', 'r', 'utf-8')
jsonData = codecs.open('/mnt/data_quick/wiki/allpages.json', 'w', 'utf-8')

pageData = []
articleCount = 0
pageCount = 0
pageStart = 0

for i, line in enumerate(wikiData):
    #if i > 10000:
    #    break

    if '<page>' in line:
        pageCount += 1

        if pageCount > 1:
            print 'unexpected to have pageCount > 1'
        else:
            articleCount += 1
            pageStart = line.index('<page>')

    if pageCount > 0:
        pageData.append(line[pageStart:])

    if '</page>' in line:
        pageCount -= 1

        if pageCount == 0:
            try:
                fromxml = parse_xml_to_row(u'\n'.join(pageData))
            except ParseError:
                print u'\n'.join(pageData)
                break

            json.dump(fromxml, jsonData)
            jsonData.write('\n')
            pageData = []

jsonData.close()
wikiData.close()

# COMMAND ----------

# MAGIC %md
# MAGIC Read in a json file and use Spark to output a Parquet file.
# MAGIC  
# MAGIC Parquet files store data by column in a compressed and efficient manner.  More details can be found at [parquet.apache.org](https://parquet.apache.org/documentation/latest/).

# COMMAND ----------

df = sqlContext.read.json("/mnt/data_quick/wiki/allpages.json")
df.write.parquet("/mnt/data_quick/wiki/allpages.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Read back in the data as `df2` so that the more efficient Parquet format will be our starting point for the `DataFrame`.

# COMMAND ----------

df2 = sqlContext.read.parquet("/mnt/data_quick/wiki/allpages.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Save a 1% sample of the data as another Parquet file.

# COMMAND ----------

dfOne = df2.sample(False, .01, 2718).coalesce(24)
dfOne.write.parquet('/mnt/data_quick/wiki/onepercent.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC Save an even smaller sample.

# COMMAND ----------

dfSmall = df2.sample(False, .0005, 2718).coalesce(8)
dfSmall.write.parquet("/mnt/data_quick/wiki/smallwiki.parquet")
