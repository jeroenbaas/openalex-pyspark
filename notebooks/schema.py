# Databricks notebook source
from pyspark.sql.types import StructField, StructType , LongType, StringType, FloatType, TimestampType, BooleanType, ArrayType, MapType
openalex_import_tables={}

# COMMAND ----------

# MAGIC %md # Schemas
# MAGIC Specify the schema of each flat file for import

# COMMAND ----------

# MAGIC %md ## Affiliations
# MAGIC Base table for affiliations/institutions (mag/Affiliations.txt)
# MAGIC 
# MAGIC | |Field Name|Data Type|Description|
# MAGIC |-|----------|---------|-----------|
# MAGIC ||AffiliationId|bigint|PRIMARY KEY|
# MAGIC |📦️|Rank|integer|ARCHIVAL; no new ranks will be added after Jan 3.|
# MAGIC ||NormalizedName|varchar|UPDATED; slightly different normalization algorithm|
# MAGIC ||DisplayName|varchar||
# MAGIC |📦️|GridId|varchar|ARCHIVAL; RorId is the new standard identifier for organizations|
# MAGIC |🔥|RorId|varchar|NEW; ROR for this organization, see https://ror.org, https://ror.org/:RorId|
# MAGIC ||OfficialPage|varchar||
# MAGIC ||WikiPage|varchar||
# MAGIC ||PaperCount|bigint||
# MAGIC |📦️|PaperFamilyCount|bigint|ARCHIVAL; same value as PaperCount after Jan 3|
# MAGIC ||CitationCount|bigint||
# MAGIC ||Iso3166Code|varchar|Two-letter country codes, see https://en.wikipedia.org/wiki/ISO_3166-2|
# MAGIC ||Latitude|real||
# MAGIC ||Longitude|real||
# MAGIC ||CreatedDate|varchar||
# MAGIC |🔥|UpdatedDate|timestamp|NEW; set values updated from new ror data|

# COMMAND ----------

openalex_import_tables['Affiliations']={
  'schema':StructType([
    StructField("AffiliationId", LongType(), True),
    StructField("Rank", LongType(), True),
    StructField("NormalizedName", StringType(), True),
    StructField("DisplayName", StringType(), True),
    StructField("GridId", StringType(), True),
    StructField("RorId", StringType(), True),
    StructField("OfficialPage", StringType(), True),
    StructField("WikiPage", StringType(), True),
    StructField("PaperCount", LongType(), True),
    StructField("PaperFamilyCount", LongType(), True),
    StructField("CitationCount", LongType(), True),
    StructField("Iso3166Code", StringType(), True),
    StructField("Latitude", FloatType(), True),
    StructField("Longitude", FloatType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", TimestampType(), True),
  ]),
  'filename':'Affiliations.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ## Authors
# MAGIC Base table for authors (mag/Authors.txt)
# MAGIC 
# MAGIC | |Field Name|Data Type|Description|
# MAGIC |-|----------|---------|-----------|
# MAGIC ||AuthorId|bigint|PRIMARY KEY|
# MAGIC |📦️|Rank|integer|ARCHIVAL; no new ranks will be added after Jan 3|
# MAGIC ||NormalizedName|varchar|UPDATED; slightly different normalization algorithm|
# MAGIC ||DisplayName|varchar||
# MAGIC |🔥⚠️|Orcid|varchar|NEW; ORCID identifier for this author (see https://orcid.org). ⚠️ KNOWN ERROR: some ORCIDs are wrong, due to an error in the Crossref API. Should be fixed in next data dump.|
# MAGIC ||LastKnownAffiliationId|integer|FOREIGN KEY REFERENCES Affiliations.AffiliationId|
# MAGIC ||PaperCount|bigint||
# MAGIC |📦️|PaperFamilyCount|bigint|ARCHIVAL; same value as PaperCount after Jan 3|
# MAGIC ||CitationCount|bigint||
# MAGIC ||CreatedDate|varchar||
# MAGIC |🔥|UpdatedDate|timestamp|NEW; set when changes are made going forward|

# COMMAND ----------

openalex_import_tables['Authors']={
  'schema':StructType([
    StructField("AuthorId", LongType(), True),
    StructField("Rank", LongType(), True),
    StructField("NormalizedName", StringType(), True),
    StructField("DisplayName", StringType(), True),
    StructField("Orcid", StringType(), True),
    StructField("LastKnownAffiliationId", LongType(), True),
    StructField("PaperCount", LongType(), True),
    StructField("PaperFamilyCount", LongType(), True),
    StructField("CitationCount", LongType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", TimestampType(), True),
  ]),
  'filename':'Authors.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ## AuthorExtendedAttributes
# MAGIC Additional author name representations (mag/AuthorExtendedAttributes.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |----------|---------|-----------|
# MAGIC |AuthorId|bigint|FOREIGN KEY REFERENCES Authors.AuthorId|
# MAGIC |AttributeType|integer|Possible values: 1=Alternative name|
# MAGIC |AttributeValue|varchar||

# COMMAND ----------

openalex_import_tables['AuthorExtendedAttributes']={
  'schema':StructType([
    StructField("AuthorId", LongType(), True),
    StructField("AttributeType", LongType(), True),
    StructField("AttributeValue", StringType(), True),
  ]),
  'filename':'AuthorExtendedAttributes.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ## ConferenceInstances
# MAGIC 📦️ ARCHIVAL; Base table for Conference Instances (mag/ConferenceInstances.txt)
# MAGIC 
# MAGIC | |Field Name|Data Type|Description|
# MAGIC |-|----------|---------|-----------|
# MAGIC ||ConferenceInstanceId|bigint|PRIMARY KEY|
# MAGIC ||NormalizedName|varchar|UPDATED; slightly different normalization algorithm|
# MAGIC ||DisplayName|varchar||
# MAGIC ||ConferenceSeriesId|bigint|FOREIGN KEY REFERENCES ConferenceSeries.ConferenceSeriesId|
# MAGIC ||Location|varchar||
# MAGIC ||OfficialUrl|varchar||
# MAGIC ||StartDate|varchar||
# MAGIC ||EndDate|varchar||
# MAGIC ||AbstractRegistrationDate|varchar||
# MAGIC ||SubmissionDeadlineDate|varchar||
# MAGIC ||NotificationDueDate|varchar||
# MAGIC ||FinalVersionDueDate|varchar||
# MAGIC ||PaperCount|bigint||
# MAGIC ||PaperFamilyCount|bigint|ARCHIVAL; same value as PaperCount after Jan 3|
# MAGIC ||CitationCount|bigint||
# MAGIC ||Latitude|real||
# MAGIC ||Longitude|real||
# MAGIC ||CreatedDate|varchar||

# COMMAND ----------

openalex_import_tables['ConferenceInstances']={
  'schema':StructType([
    StructField("ConferenceInstanceId", LongType(), True),
    StructField("NormalizedName", StringType(), True),
    StructField("DisplayName", StringType(), True),
    StructField("ConferenceSeriesId", LongType(), True),
    StructField("Location", StringType(), True),
    StructField("OfficialUrl", StringType(), True),
    StructField("StartDate", StringType(), True),
    StructField("EndDate", StringType(), True),
    StructField("AbstractRegistrationDate", StringType(), True),
    StructField("SubmissionDeadlineDate", StringType(), True),
    StructField("NotificationDueDate", StringType(), True),
    StructField("FinalVersionDueDate", StringType(), True),
    StructField("PaperCount", LongType(), True),
    StructField("PaperFamilyCount", LongType(), True),
    StructField("CitationCount", LongType(), True),
    StructField("Latitude", FloatType(), True),
    StructField("Longitude", FloatType(), True),
    StructField("CreatedDate", StringType(), True),
  ]),
  'filename':'ConferenceInstances.txt',
  'subpath':'mag'
}


# COMMAND ----------

# MAGIC %md ## ConferenceSeries
# MAGIC 📦️ ARCHIVAL; Base table for Conference Series (mag/ConferenceSeries.txt)|
# MAGIC 
# MAGIC | |Field Name|Data Type|Description|
# MAGIC |-|----------|---------|-----------|
# MAGIC ||ConferenceSeriesId|bigint|PRIMARY KEY|
# MAGIC ||Rank|integer|ARCHIVAL; no new ranks will be added after Jan 3|
# MAGIC ||NormalizedName|varchar|UPDATED; slightly different normalization algorithm|
# MAGIC ||DisplayName|varchar||
# MAGIC ||PaperCount|bigint||
# MAGIC |️|PaperFamilyCount|bigint|ARCHIVAL; same value as PaperCount after Jan 3|
# MAGIC ||CitationCount|bigint||
# MAGIC ||CreatedDate|varchar||

# COMMAND ----------

openalex_import_tables['ConferenceSeries']={
  'schema':StructType([
    StructField("ConferenceSeriesId", LongType(), True),
    StructField("Rank", LongType(), True),
    StructField("NormalizedName", StringType(), True),
    StructField("DisplayName", StringType(), True),
    StructField("PaperCount", LongType(), True),
    StructField("PaperFamilyCount", LongType(), True),
    StructField("CitationCount", LongType(), True),
    StructField("CreatedDate", StringType(), True),
  ]),
  'filename':'ConferenceSeries.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ##EntityRelatedEntities
# MAGIC Relationship between papers, authors, fields of study. (advanced/EntityRelatedEntities.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |EntityId|bigint||
# MAGIC |EntityType|varchar|Possible values: af (Affiliation), j (Journal), c (Conference)|
# MAGIC |RelatedEntityId|bigint||
# MAGIC |RelatedEntityType|varchar|Possible values: af (Affiliation), j (Journal), c (Conference)|
# MAGIC |RelatedType|integer|Possible values: 0 (same paper), 1 (common coauthors), 2 (co-cited), 3 (common field of study), 4 (same venue), 5 (A cites B), 6 (B cites A)|
# MAGIC |Score|real|Confidence range between 0 and 1. Larger number representing higher confidence.|

# COMMAND ----------

openalex_import_tables['EntityRelatedEntities']={
  'schema':StructType([
    StructField("EntityId", LongType(), True),
    StructField("EntityType", StringType(), True),
    StructField("RelatedEntityId", LongType(), True),
    StructField("RelatedEntityType", StringType(), True),
    StructField("RelatedType", LongType(), True),
    StructField("Score", FloatType(), True),
  ]),
  'filename':'EntityRelatedEntities.txt',
  'subpath':'advanced'
}

# COMMAND ----------

# MAGIC %md ##FieldOfStudyChildren
# MAGIC Relationship between Fields of Study (advanced/FieldOfStudyChildren.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |FieldOfStudyId|bigint|FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId|
# MAGIC |ChildFieldOfStudyId|bigint|FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId|

# COMMAND ----------

openalex_import_tables['FieldOfStudyChildren']={
  'schema':StructType([
    StructField("FieldOfStudyId", LongType(), True),
    StructField("ChildFieldOfStudyId", LongType(), True),
  ]),
  'filename':'FieldOfStudyChildren.txt',
  'subpath':'advanced'
}

# COMMAND ----------

# MAGIC %md ##FieldOfStudyExtendedAttributes
# MAGIC Other identifiers for Fields of Study (advanced/FieldOfStudyExtendedAttributes.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |FieldOfStudyId|bigint|FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId|
# MAGIC |AttributeType|bigint|Possible values: 1 (AUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsarchives04.html#2018AA), 2 (source url), 3 (CUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html)|
# MAGIC |AttributeValue|varchar|-|

# COMMAND ----------

openalex_import_tables['FieldOfStudyExtendedAttributes']={
  'schema':StructType([
    StructField("FieldOfStudyId", LongType(), True),
    StructField("AttributeType", LongType(), True),
    StructField("AttributeValue", StringType(), True),
  ]),
  'filename':'FieldOfStudyExtendedAttributes.txt',
  'subpath':'advanced'
}


# COMMAND ----------

# MAGIC %md ##FieldsOfStudy
# MAGIC Base table for Fields of Study (advanced/FieldsOfStudy.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-||
# MAGIC ||FieldOfStudyId|bigint|PRIMARY KEY|
# MAGIC |📦️|Rank|varchar|ARCHIVAL; no new ranks will be added after Jan 3.|
# MAGIC ||NormalizedName|varchar|UPDATED; slightly different normalization algorithm|
# MAGIC ||DisplayName|varchar||
# MAGIC ||MainType|varchar||
# MAGIC ||Level|integer|Possible values: 0-5|
# MAGIC ||PaperCount|bigint||
# MAGIC |📦️|PaperFamilyCount|bigint|ARCHIVAL; same value as PaperCount after Jan 3|
# MAGIC ||CitationCount|bigint||
# MAGIC ||CreatedDate|varchar||

# COMMAND ----------

openalex_import_tables['FieldsOfStudy']={
  'schema':StructType([
    StructField("FieldOfStudyId", LongType(), True),
    StructField("Rank", StringType(), True),
    StructField("NormalizedName", StringType(), True),
    StructField("DisplayName", StringType(), True),
    StructField("MainType", StringType(), True),
    StructField("Level", LongType(), True),
    StructField("PaperCount", LongType(), True),
    StructField("PaperFamilyCount", LongType(), True),
    StructField("CitationCount", LongType(), True),
    StructField("CreatedDate", StringType(), True),
  ]),
  'filename':'FieldsOfStudy.txt',
  'subpath':'advanced'
}

# COMMAND ----------

# MAGIC %md ##Journals
# MAGIC Base table for Journals (mag/Journals.txt)
# MAGIC 
# MAGIC ||Field Name|Data Type|Description|
# MAGIC |-|-|-||
# MAGIC ||JournalId|bigint|PRIMARY KEY|
# MAGIC |📦️|Rank|integer|ARCHIVAL; no new ranks will be added after Jan 3|
# MAGIC ||NormalizedName|varchar|UPDATED; slightly different normalization algorithm|
# MAGIC ||DisplayName|varchar||
# MAGIC ||Issn|varchar|UPDATED; the ISSN-L for the journal (see https://en.wikipedia.org/wiki/International_Standard_Serial_Number#Linking_ISSN)|
# MAGIC |🔥|Issns|varchar|NEW; JSON list of all ISSNs for this journal (example: '["1469-5073","0016-6723"]' )|
# MAGIC |🔥|IsOa|boolean|NEW; TRUE when the journal is 100% OA|
# MAGIC |🔥|IsInDoaj|boolean|NEW; TRUE when the journal is in DOAJ (see https://doaj.org/)|
# MAGIC ||Publisher|varchar||
# MAGIC ||Webpage|varchar||
# MAGIC ||PaperCount|bigint||
# MAGIC |📦️|PaperFamilyCount|bigint|ARCHIVAL; same value as PaperCount after Jan 3|
# MAGIC ||CitationCount|bigint||
# MAGIC ||CreatedDate|varchar||
# MAGIC |🔥|UpdatedDate|timestamp|NEW; set when changes are made going forward|

# COMMAND ----------

openalex_import_tables['Journals']={
  'schema':StructType([
    StructField("JournalId", LongType(), True),
    StructField("Rank", StringType(), True),
    StructField("NormalizedName", StringType(), True),
    StructField("DisplayName", StringType(), True),
    StructField("Issn", StringType(), True),
    StructField("Issns", StringType(), True),
    StructField("IsOa", BooleanType(), True),
    StructField("IsInDoaj", BooleanType(), True),
    StructField("Publisher", StringType(), True),
    StructField("Webpage", StringType(), True),
    StructField("PaperCount", LongType(), True),
    StructField("PaperFamilyCount", LongType(), True),
    StructField("CitationCount", LongType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", TimestampType(), True),
  ]),
  'filename':'Journals.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ##PaperAbstractsInvertedIndex
# MAGIC Inverted index of abstracts (nlp/PaperAbstractsInvertedIndex.txt{*} split across multiple files)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |IndexedAbstract|varchar|Inverted index, see https://en.wikipedia.org/wiki/Inverted_index|

# COMMAND ----------

openalex_import_tables['PaperAbstractsInvertedIndex']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("IndexedAbstract", StringType(), True),
  ]),
  'filename':'PaperAbstractsInvertedIndex.txt*',
  'subpath':'nlp'
}

# COMMAND ----------

# MAGIC %md ##PaperAuthorAffiliations
# MAGIC Links between papers, authors, and affiliations/institutions. NOTE: It is possible to have multiple rows with same (PaperId, AuthorId, AffiliationId) when an author is associated with multiple affiliations. (mag/PaperAuthorAffiliations.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |AuthorId|bigint|FOREIGN KEY REFERENCES Authors.AuthorId|
# MAGIC |AffiliationId|bigint|FOREIGN KEY REFERENCES Affiliations.AffiliationId|
# MAGIC |AuthorSequenceNumber|integer|1-based author sequence number. 1: the 1st author listed on paper, 2: the 2nd author listed on paper, etc.|
# MAGIC |OriginalAuthor|varchar||
# MAGIC |OriginalAffiliation|varchar||

# COMMAND ----------

openalex_import_tables['PaperAuthorAffiliations']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("AuthorId", LongType(), True),
    StructField("AffiliationId", LongType(), True),
    StructField("AuthorSequenceNumber", LongType(), True),
    StructField("OriginalAuthor", StringType(), True),
    StructField("OriginalAffiliation", StringType(), True),
  ]),
  'filename':'PaperAuthorAffiliations.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ##PaperCitationContexts
# MAGIC 📦️ ARCHIVAL; citation contexts (nlp/PaperCitationContexts.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |PaperReferenceId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |CitationContext|varchar||

# COMMAND ----------

openalex_import_tables['PaperCitationContexts']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("PaperReferenceId", LongType(), True),
    StructField("CitationContext", StringType(), True),
  ]),
  'filename':'PaperCitationContexts.txt',
  'subpath':'nlp'
}

# COMMAND ----------

# MAGIC %md ##PaperExtendedAttributes
# MAGIC Extra paper identifiers (mag/PaperExtendedAttributes.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |AttributeType|integer|Possible values: 1=PatentId, 2=PubMedId, 3=PmcId, 4=Alternative Title|
# MAGIC |AttributeValue|varchar||

# COMMAND ----------

openalex_import_tables['PaperExtendedAttributes']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("AttributeType", LongType(), True),
    StructField("AttributeValue", StringType(), True),
  ]),
  'filename':'PaperExtendedAttributes.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ##PaperFieldsOfStudy
# MAGIC Linking table from papers to fields, with score (advanced/PaperFieldsOfStudy.txt)
# MAGIC 
# MAGIC ||Field Name|Data Type|Description|
# MAGIC |-|-|-|-|
# MAGIC ||PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC ||FieldOfStudyId|bigint|FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId|
# MAGIC ||Score|real|Confidence range between 0 and 1. Bigger number representing higher confidence.|
# MAGIC |🔥|AlgorithmVersion|integer|NEW; version of algorithm to assign fields. Possible values: 1=old MAG (ARCHIVAL), 2=OpenAlex

# COMMAND ----------

openalex_import_tables['PaperFieldsOfStudy']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("FieldOfStudyId", LongType(), True),
    StructField("Score", FloatType(), True),
    StructField("AlgorithmVersion", LongType(), True),
  ]),
  'filename':'PaperFieldsOfStudy.txt',
  'subpath':'advanced'
}

# COMMAND ----------

# MAGIC %md ##PaperMeSH
# MAGIC MeSH headings assigned to the paper by PubMed (advanced/PaperMeSH.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |DescriptorUI|varchar|see https://en.wikipedia.org/wiki/Medical_Subject_Headings|
# MAGIC |DescriptorName|varchar|see https://en.wikipedia.org/wiki/Medical_Subject_Headings|
# MAGIC |QualifierUI|varchar|see https://en.wikipedia.org/wiki/Medical_Subject_Headings|
# MAGIC |QualifierName|varchar|see https://en.wikipedia.org/wiki/Medical_Subject_Headings|
# MAGIC |IsMajorTopic|boolean|see https://en.wikipedia.org/wiki/Medical_Subject_Headings

# COMMAND ----------

openalex_import_tables['PaperMeSH']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("DescriptorUI", StringType(), True),
    StructField("DescriptorName", StringType(), True),
    StructField("QualifierUI", StringType(), True),
    StructField("QualifierName", StringType(), True),
    StructField("IsMajorTopic", BooleanType(), True),
  ]),
  'filename':'PaperMeSH.txt',
  'subpath':'advanced'
}


# COMMAND ----------

# MAGIC %md ##PaperRecommendations
# MAGIC Paper recommendations with score (advanced/PaperRecommendations.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |RecommendedPaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |Score|real|Confidence range between 0 and 1. Bigger number representing higher confidence.|

# COMMAND ----------

openalex_import_tables['PaperRecommendations']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("RecommendedPaperId", LongType(), True),
    StructField("Score", FloatType(), True),
  ]),
  'filename':'PaperRecommendations.txt',
  'subpath':'advanced'
}

# COMMAND ----------

# MAGIC %md ##PaperReferences
# MAGIC Paper references and, in reverse, citations (mag/PaperReferences.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |PaperReferenceId|bigint|FOREIGN KEY REFERENCES Papers.PaperId

# COMMAND ----------

openalex_import_tables['PaperReferences']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("PaperReferenceId", LongType(), True),
  ]),
  'filename':'PaperReferences.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ##PaperResources
# MAGIC 📦️ ARCHIVAL; not updated after Jan 3. Data and code urls associated with papers (mag/PaperResources.txt)
# MAGIC 
# MAGIC |Field Name|Data Type|Description|
# MAGIC |-|-|-|
# MAGIC |PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC |ResourceType|integer|Bit flags: 1=Project, 2=Data, 4=Code|
# MAGIC |ResourceUrl|varchar|Url of resource|
# MAGIC |SourceUrl|varchar|List of urls associated with the project, used to derive resource_url|
# MAGIC |RelationshipType|integer|Bit flags: 1=Own, 2=Cite|

# COMMAND ----------

openalex_import_tables['PaperResources']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("ResourceType", LongType(), True),
    StructField("ResourceUrl", StringType(), True),
    StructField("SourceUrl", StringType(), True),
    StructField("RelationshipType", LongType(), True),
  ]),
  'filename':'PaperResources.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ##PaperUrls
# MAGIC Urls for the paper (mag/PaperUrls.txt)
# MAGIC 
# MAGIC ||Field Name|Data Type|Description|
# MAGIC |-|-|-|-|
# MAGIC ||PaperId|bigint|FOREIGN KEY REFERENCES Papers.PaperId|
# MAGIC ||SourceType|integer|Possible values: 1=Html, 2=Text, 3=Pdf, 4=Doc, 5=Ppt, 6=Xls, 8=Rtf, 12=Xml, 13=Rss, 20=Swf, 27=Ics, 31=Pub, 33=Ods, 34=Odp, 35=Odt, 36=Zip, 40=Mp3, 0/999/NULL=unknown|
# MAGIC ||SourceUrl|varchar||
# MAGIC ||LanguageCode|varchar||
# MAGIC |🔥|UrlForLandingPage|varchar|NEW; URL for the landing page, when article is free to read|
# MAGIC |🔥|UrlForPdf|varchar|NEW; URL for the PDF, when article is free to read|
# MAGIC |🔥|HostType|varchar|NEW; host type of the free-to-read URL, Possible values: publisher, repository|
# MAGIC |🔥|Version|varchar|NEW; version of the free-to-read URL Possible values: submittedVersion, acceptedVersion, publishedVersion (see https://support.unpaywall.org/support/solutions/articles/44000708792)|
# MAGIC |🔥|License|varchar|NEW; license of the free-to-read URL (example: cc0, cc-by, publisher-specific)|
# MAGIC |🔥|RepositoryInstitution|varchar|NEW; name of repository host of URL|
# MAGIC |🔥|OaiPmhId|varchar|NEW; OAH-PMH id of the repository record|

# COMMAND ----------

openalex_import_tables['PaperUrls']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("SourceType", LongType(), True),
    StructField("SourceUrl", StringType(), True),
    StructField("LanguageCode", StringType(), True),
    StructField("UrlForLandingPage", StringType(), True),
    StructField("UrlForPdf", StringType(), True),
    StructField("HostType", StringType(), True),
    StructField("Version", StringType(), True),
    StructField("License", StringType(), True),
    StructField("RepositoryInstitution", StringType(), True),
    StructField("OaiPmhId", StringType(), True),
  ]),
  'filename':'PaperUrls.txt',
  'subpath':'mag'
}

# COMMAND ----------

# MAGIC %md ##Papers
# MAGIC Main data for papers (mag/Papers.txt)
# MAGIC 
# MAGIC ||Field Name|Data Type|Description|
# MAGIC |-|-|-|-|
# MAGIC ||PaperId|bigint|PRIMARY KEY|
# MAGIC |📦️|Rank|integer|ARCHIVAL; no new ranks will be added after Jan 3|
# MAGIC ||Doi|varchar|Doi values are upper-cased per DOI standard at https://www.doi.org/doi_handbook/2_Numbering.html#2.4|
# MAGIC |📦️|DocType|varchar|Possible values: Book, BookChapter, Conference, Dataset, Journal, Patent, Repository, Thesis, NULL : unknown. Patent is REMOVED; no patents are included.|
# MAGIC |🔥|Genre|varchar|NEW; Crossref ontology for work type such as journal-article, posted-content, dataset, or book-chapter|
# MAGIC |🔥|IsParatext|boolean|NEW; indicates front-matter. See https://support.unpaywall.org/support/solutions/articles/44001894783|
# MAGIC ||PaperTitle|varchar|UPDATED; slightly different normalization algorithm|
# MAGIC ||OriginalTitle|varchar||
# MAGIC ||BookTitle|varchar||
# MAGIC ||Year|integer||
# MAGIC ||Date|varchar||
# MAGIC ||OnlineDate|varchar||
# MAGIC ||Publisher|varchar||
# MAGIC ||JournalId|bigint|FOREIGN KEY references Journals.JournalId|
# MAGIC |📦️|ConferenceSeriesId|bigint|ARCHIVAL; not updated after Jan 3, no new Conference Series will be added after Jan 3. FOREIGN KEY references ConferenceSeries.ConferenceSeriesId;|
# MAGIC |📦️|ConferenceInstanceId|bigint|ARCHIVAL; not updated after Jan 3, no new Conference Instances will be added after Jan 3. FOREIGN KEY references ConferenceInstance.ConferenceInstanceId;|
# MAGIC ||Volume|varchar||
# MAGIC ||Issue|varchar||
# MAGIC ||FirstPage|varchar||
# MAGIC ||LastPage|varchar||
# MAGIC ||ReferenceCount|bigint||
# MAGIC ||CitationCount|bigint||
# MAGIC ||EstimatedCitation|bigint|UPDATED; new algorithm|
# MAGIC ||OriginalVenue|varchar||
# MAGIC |📦️|FamilyId|bigint|ARCHIVAL; not updated after Jan 3.|
# MAGIC |📦️|FamilyRank|bigint|ARCHIVAL; not updated after Jan 3.|
# MAGIC ||DocSubTypes|varchar|Possible values: Retracted Publication, Retraction Notice|
# MAGIC |🔥|OaStatus|varchar|NEW; Possible values: closed, green, gold, hybrid, bronze (see https://en.wikipedia.org/wiki/Open_access#Colour_naming_system)|
# MAGIC |🔥|BestUrl|varchar|NEW; An url for the paper (see PaperUrls table for more)|
# MAGIC |🔥|BestFreeUrl|varchar|NEW; Url of best legal free-to-read copy when it exists (see https://support.unpaywall.org/support/solutions/articles/44001943223)|
# MAGIC |🔥|BestFreeVersion|varchar|NEW; Possible values: submittedVersion, acceptedVersion, publishedVersion (see https://support.unpaywall.org/support/solutions/articles/44000708792)|
# MAGIC |🔥|DoiLower|varchar|NEW; lowercase doi for convenience linking to Unpaywall|
# MAGIC ||CreatedDate|varchar||
# MAGIC |🔥|UpdatedDate|timestamp|NEW; set when changes are made going forward|

# COMMAND ----------

openalex_import_tables['Papers']={
  'schema':StructType([
    StructField("PaperId", LongType(), True),
    StructField("Rank", LongType(), True),
    StructField("Doi", StringType(), True),
    StructField("DocType", StringType(), True),
    StructField("Genre", StringType(), True),
    StructField("IsParatext", BooleanType(), True),
    StructField("PaperTitle", StringType(), True),
    StructField("OriginalTitle", StringType(), True),
    StructField("BookTitle", StringType(), True),
    StructField("Year", LongType(), True),
    StructField("Date", StringType(), True),
    StructField("OnlineDate", StringType(), True),
    StructField("Publisher", StringType(), True),
    StructField("JournalId", LongType(), True),
    StructField("ConferenceSeriesId", LongType(), True),
    StructField("ConferenceInstanceId", LongType(), True),
    StructField("Volume", StringType(), True),
    StructField("Issue", StringType(), True),
    StructField("FirstPage", StringType(), True),
    StructField("LastPage", StringType(), True),
    StructField("ReferenceCount", LongType(), True),
    StructField("CitationCount", LongType(), True),
    StructField("EstimatedCitation", LongType(), True),
    StructField("OriginalVenue", StringType(), True),
    StructField("FamilyId", LongType(), True),
    StructField("FamilyRank", LongType(), True),
    StructField("DocSubTypes", StringType(), True),
    StructField("OaStatus", StringType(), True),
    StructField("BestUrl", StringType(), True),
    StructField("BestFreeUrl", StringType(), True),
    StructField("BestFreeVersion", StringType(), True),
    StructField("DoiLower", StringType(), True), # want to cry that this exits.
    StructField("CreatedDate", StringType(), True),
    StructField("UpdatedDate", TimestampType(), True),
  ]),
  'filename':'Papers.txt',
  'subpath':'mag'
}
